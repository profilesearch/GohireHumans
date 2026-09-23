"""Default-off, at-most-once AgentMail outbox transport (no delivery tracking).

The ledger is deliberately not a retry queue. Only newly enqueued, approved
notifications can enter it. A committed prepared intent is NEVER posted again.
Do not delete/reset ledger rows, including after a provider change or restore.
AGENTMAIL_TOTAL_SEND_CAP and AGENTMAIL_DAILY_SEND_CAP default to 1; explicit
overrides must be decimal integers 1..100. Every prepared intent consumes both
budgets, including ambiguous/crashed attempts. Raising a cap requires approval.
"""
import hashlib
import json
import os
import re
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
try:
    import password_reset_crypto
except ModuleNotFoundError as exc:
    if exc.name != 'password_reset_crypto':
        raise
    import importlib.util
    _crypto_spec = importlib.util.spec_from_file_location(
        'password_reset_crypto', os.path.join(os.path.dirname(__file__), 'password_reset_crypto.py'))
    if _crypto_spec is None or _crypto_spec.loader is None:
        raise ImportError('Password reset crypto module is unavailable')
    password_reset_crypto = importlib.util.module_from_spec(_crypto_spec)
    _crypto_spec.loader.exec_module(password_reset_crypto)
from datetime import datetime, timedelta, timezone

SENDER = 'gohirehumans.operations@agentmail.to'
SUPPORTED_TYPES = frozenset({'new_application', 'password_reset'})
SUBJECT = 'GoHireHumans activity update'
TEXT = ('There is an update related to your GoHireHumans account. '
        'Sign in to review it on GoHireHumans: https://www.gohirehumans.com\n\n'
        'You received this email because of activity related to your GoHireHumans account.')
MAX_AGE_SECONDS = 900
TABLE_SQL = """CREATE TABLE agentmail_send_ledger (
    key_digest TEXT PRIMARY KEY NOT NULL CHECK(length(key_digest)=64),
    outbox_id INTEGER NOT NULL UNIQUE,
    fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),
    state TEXT NOT NULL CHECK(state IN ('approved','prepared','accepted','unknown')),
    prepared_at TEXT,
    provider_id TEXT,
    message_id TEXT,
    thread_id TEXT,
    CHECK((state='approved' AND prepared_at IS NULL AND provider_id IS NULL)
       OR (state IN ('prepared','unknown') AND prepared_at IS NOT NULL AND provider_id IS NULL)
       OR (state='accepted' AND prepared_at IS NOT NULL AND provider_id IS NOT NULL)),
    CHECK((state!='accepted' AND message_id IS NULL AND thread_id IS NULL)
       OR (state='accepted' AND message_id IS NOT NULL AND thread_id IS NOT NULL))
)"""


def digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def init_schema(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='agentmail_send_ledger'").fetchone() is None:
        db.execute(TABLE_SQL)
    validate_schema(db)


def validate_schema(db):
    # This new table has no legacy variants: accept only our exact DDL. No
    # comment/constraint/trigger lookalikes or silently weakened same-name table.
    row = db.execute("SELECT type,sql FROM sqlite_master WHERE name='agentmail_send_ledger'").fetchone()
    if row is None or tuple(row) != ('table', TABLE_SQL):
        raise RuntimeError('agentmail_schema_invalid')
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name='agentmail_send_ledger'").fetchone():
        raise RuntimeError('agentmail_schema_invalid')
    indexes = db.execute('PRAGMA index_list(agentmail_send_ledger)').fetchall()
    if len(indexes) != 2 or any(r[2] != 1 or r[3] not in ('pk', 'u') or r[4] != 0 for r in indexes):
        raise RuntimeError('agentmail_schema_invalid')


def _expiry_time():
    value = os.environ.get('AGENTMAIL_EXPIRES_AT', '')
    try:
        end = datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        return end if end.strftime('%Y-%m-%dT%H:%M:%SZ') == value else None
    except (TypeError, ValueError, OverflowError):
        return None


def config() -> tuple[dict | None, str]:
    env = os.environ
    if env.get('EMAIL_PROVIDER', 'resend') != 'agentmail':
        return None, 'provider_not_selected'
    if env.get('AGENTMAIL_SEND_ENABLED') != 'true':
        return None, 'send_disabled'
    key = env.get('AGENTMAIL_API_KEY', '')
    if not key or not key.isascii() or any(c.isspace() or ord(c) < 33 or ord(c) > 126 for c in key):
        return None, 'key_missing_or_invalid'
    if env.get('AGENTMAIL_INBOX_ID') != SENDER:
        return None, 'sender_invalid'
    recipients = env.get('AGENTMAIL_RECIPIENT_ALLOWLIST', '').split(',')
    if not recipients or any(not re.fullmatch(r'[A-Za-z0-9.!#$%&\'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+', r) for r in recipients):
        return None, 'recipient_allowlist_invalid'
    types = set(env.get('AGENTMAIL_NOTIFICATION_TYPES', '').split(','))
    if not types or not types <= SUPPORTED_TYPES:
        return None, 'notification_types_invalid'
    values = []
    for name in ('AGENTMAIL_OUTBOX_HIGHWATER', 'AGENTMAIL_NOTIFICATION_HIGHWATER'):
        value = env.get(name, '')
        if not re.fullmatch(r'0|[1-9][0-9]{0,17}', value):
            return None, 'highwater_invalid'
        values.append(int(value))
    activated = env.get('AGENTMAIL_ACTIVATED_AT', '')
    try:
        start = datetime.strptime(activated, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        if start.strftime('%Y-%m-%dT%H:%M:%SZ') != activated:
            raise ValueError()
    except (TypeError, ValueError, OverflowError):
        return None, 'activation_time_invalid'
    end = _expiry_time()
    if end is None:
        return None, 'expiry_time_invalid'
    if not timedelta(0) < end - start <= timedelta(days=7):
        return None, 'expiry_window_invalid'
    now = datetime.now(timezone.utc)
    if now < start:
        return None, 'activation_not_started'
    if now >= end:
        return None, 'expired'
    cap = env.get('AGENTMAIL_DAILY_SEND_CAP', '1')
    if not re.fullmatch(r'[1-9][0-9]{0,2}', cap) or not 1 <= int(cap) <= 100:
        return None, 'daily_cap_invalid'
    total_cap = env.get('AGENTMAIL_TOTAL_SEND_CAP', '1')
    if not re.fullmatch(r'[1-9][0-9]{0,2}', total_cap) or not 1 <= int(total_cap) <= 100:
        return None, 'total_cap_invalid'
    return dict(key=key, recipients=set(recipients), types=types, highwater=values[0],
                notification_highwater=values[1], start=start, end=end, cap=int(cap),
                total_cap=int(total_cap)), 'ready'


def reset_config() -> tuple[dict | None, str]:
    """Independent, default-off password-reset transport; never inherit the canary."""
    env = os.environ
    if env.get('PASSWORD_RESET_EMAIL_ENABLED') != 'true':
        return None, 'reset_disabled'
    key = env.get('AGENTMAIL_API_KEY', '')
    if not key or not key.isascii() or any(c.isspace() or ord(c) < 33 or ord(c) > 126 for c in key):
        return None, 'key_missing_or_invalid'
    if env.get('AGENTMAIL_INBOX_ID') != SENDER:
        return None, 'sender_invalid'
    if not password_reset_crypto.configured():
        return None, 'encryption_key_invalid'
    cap = env.get('PASSWORD_RESET_DAILY_SEND_CAP', '10')
    if not re.fullmatch(r'[1-9][0-9]?', cap) or not 1 <= int(cap) <= 20:
        return None, 'daily_cap_invalid'
    app_base = env.get('APP_BASE_URL', 'https://www.gohirehumans.com')
    if not re.fullmatch(r'https://(?:www\.)?gohirehumans\.com', app_base):
        return None, 'app_base_invalid'
    return dict(key=key, cap=int(cap), app_base=app_base), 'ready'


def _config_for(notification_type):
    return reset_config() if notification_type == 'password_reset' else config()


def _binding(db, row, cfg):
    user = db.execute('SELECT email FROM users WHERE id=? AND is_active=1 AND is_banned=0 AND is_suspended=0', [row['user_id']]).fetchone()
    source = db.execute('SELECT user_id,type,created_at FROM notifications WHERE id=?', [row['notification_id']]).fetchone()
    if not user or not source or source['user_id'] != row['user_id'] or source['type'] != row['notification_type']:
        return None
    try:
        now = datetime.now(timezone.utc)
        created = datetime.strptime(row['created_at'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        source_created = datetime.strptime(source['created_at'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        expires = datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        if row['notification_type'] == 'password_reset':
            fresh = source_created <= created <= now < expires
        else:
            fresh = (cfg['start'] <= source_created <= created <= now < expires
                     and (now - source_created).total_seconds() <= MAX_AGE_SECONDS)
    except (TypeError, ValueError, OverflowError):
        return None
    if not fresh:
        return None
    if row['notification_type'] != 'password_reset' and (
            row['id'] <= cfg['highwater'] or row['notification_id'] <= cfg['notification_highwater']
            or user['email'] not in cfg['recipients'] or row['notification_type'] not in cfg['types']):
        return None
    if row['notification_type'] == 'password_reset':
        token = password_reset_crypto.active_token(db, row['link'], row['user_id'])
        if token is None:
            return None
        link = cfg['app_base'] + '/#/reset-password?token=' + urllib.parse.quote(token, safe='')
        payload = dict(to=[user['email']], reply_to=[SENDER],
                       subject='Reset your GoHireHumans password',
                       text='Use this link within 30 minutes to reset your password: ' + link
                            + '\n\nIf you did not request this, ignore this email.\n\nGoHireHumans',
                       track_opens=False)
        # Recipient is deliberately excluded from the reset fingerprint: a user
        # changing their address before send must receive it at the current one.
        fingerprint = digest(json.dumps([row['id'], row['notification_id'], row['user_id'],
            row['notification_type'], row['created_at'], source['created_at'],
            digest(row['link']), cfg['app_base'], SENDER], sort_keys=True, separators=(',', ':')))
        return fingerprint, payload
    else:
        payload = dict(to=[user['email']], reply_to=[SENDER], subject=SUBJECT,
                       text=TEXT, track_opens=False)
    fingerprint = digest(json.dumps([row['id'], row['notification_id'], row['user_id'],
        row['notification_type'], row['created_at'], source['created_at'],
        cfg['start'].isoformat(), cfg['highwater'], cfg['notification_highwater'], SENDER, payload],
        sort_keys=True, separators=(',', ':')))
    return fingerprint, payload


def enroll(db, outbox_id):
    """Called ONLY during creation, in the notification/outbox transaction."""
    row = db.execute('SELECT * FROM transactional_email_outbox WHERE id=?', [outbox_id]).fetchone()
    if row is None:
        return
    cfg, _ = _config_for(row['notification_type'])
    if cfg is None:
        return
    validate_schema(db)
    binding = _binding(db, row, cfg)
    if binding:
        if row['notification_type'] != 'password_reset' and not cfg['start'] <= datetime.now(timezone.utc) < cfg['end']:
            return
        db.execute("INSERT OR IGNORE INTO agentmail_send_ledger(key_digest,outbox_id,fingerprint,state) VALUES(?,?,?,'approved')",
                   [digest(row['dedupe_key']), row['id'], binding[0]])


def owns_key(db, key):
    validate_schema(db)
    return bool(key and db.execute('SELECT 1 FROM agentmail_send_ledger WHERE key_digest=?', [digest(key)]).fetchone())


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def send(db, outbox_id, claim_token, key, user_id, notification_type):
    """Worker-only path. Claim and enrollment are authoritative, not arguments."""
    if db.in_transaction:
        return 'manual_review', None
    try:
        db.execute('BEGIN IMMEDIATE')
        validate_schema(db)
        row = db.execute("SELECT * FROM transactional_email_outbox WHERE id=? AND state='sending' AND claim_token=?",
                         [outbox_id, claim_token]).fetchone()
        if row is None or row['dedupe_key'] != key or row['user_id'] != user_id or row['notification_type'] != notification_type:
            db.rollback()
            return 'suppressed', None
        cfg, _ = _config_for(row['notification_type'])
        intent = db.execute('SELECT * FROM agentmail_send_ledger WHERE key_digest=?', [digest(key)]).fetchone()
        if cfg is None or intent is None:
            db.rollback()
            return 'manual_review' if intent else 'suppressed', None
        binding = _binding(db, row, cfg)
        if binding is None or binding[0] != intent['fingerprint'] or intent['outbox_id'] != outbox_id:
            db.rollback()
            return 'manual_review', None
        if intent['state'] == 'accepted':
            db.rollback()
            return 'accepted', intent['provider_id']
        if intent['state'] != 'approved':
            db.rollback()
            return 'manual_review', None
        now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        if row['notification_type'] == 'password_reset':
            # Count every prepared intent, including ambiguous provider outcomes.
            # BEGIN IMMEDIATE serializes competing workers before preparation;
            # excess requests are suppressed rather than deferred past token expiry.
            since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
            used = db.execute("""SELECT COUNT(*) FROM agentmail_send_ledger l
                LEFT JOIN transactional_email_outbox o ON o.id=l.outbox_id
                WHERE (o.notification_type='password_reset' OR o.id IS NULL)
                  AND l.prepared_at>=?""", [since]).fetchone()[0]
            if used >= cfg['cap']:
                db.rollback()
                return 'suppressed', None
        else:
            # Reset mail has its own budget; it never consumes the canary's caps.
            used = db.execute("""SELECT COUNT(*) FROM agentmail_send_ledger l
                LEFT JOIN transactional_email_outbox o ON o.id=l.outbox_id
                WHERE l.prepared_at>=? AND COALESCE(o.notification_type,'')!='password_reset'""",
                [now[:10]]).fetchone()[0]
            total = db.execute("""SELECT COUNT(*) FROM agentmail_send_ledger l
                LEFT JOIN transactional_email_outbox o ON o.id=l.outbox_id
                WHERE l.prepared_at IS NOT NULL AND COALESCE(o.notification_type,'')!='password_reset'""").fetchone()[0]
            if used >= cfg['cap'] or total >= cfg['total_cap']:
                db.rollback()
                return 'suppressed', None
        db.execute("UPDATE agentmail_send_ledger SET state='prepared',prepared_at=? WHERE key_digest=? AND state='approved'",
                   [now, digest(key)])
        db.commit()  # irreversible send intent; never retain a writer over I/O
        request = urllib.request.Request(
            'https://api.agentmail.to/v0/inboxes/' + urllib.parse.quote(SENDER, safe='') + '/messages/send',
            data=json.dumps(binding[1], separators=(',', ':')).encode('utf-8'),
            headers={'Authorization': 'Bearer ' + cfg['key'], 'Content-Type': 'application/json',
                     'Idempotency-Key': 'ghh-agentmail-' + digest(key)}, method='POST')
        opener = urllib.request.build_opener(_NoRedirect())
        # Preparation/opener construction may cross the rollout boundary.
        # Keep the committed intent (and consumed budget); never retry it.
        if row['notification_type'] == 'password_reset':
            current_cfg, _ = reset_config()
            if current_cfg != cfg:
                return 'manual_review', None
        elif not cfg['start'] <= datetime.now(timezone.utc) < cfg['end']:
            return 'manual_review', None
        response = opener.open(request, timeout=10)
        try:
            body = response.read(16385)
            status = response.status
        finally:
            response.close()
        parsed = json.loads(body) if len(body) <= 16384 and status == 200 else None
        message_id = parsed.get('message_id') if isinstance(parsed, dict) else None
        thread_id = parsed.get('thread_id') if isinstance(parsed, dict) else None
        if any(not isinstance(value, str) or not re.fullmatch(r'[!-~]{1,998}', value)
               for value in (message_id, thread_id)):
            raise ValueError('invalid_response')
        # Restricted ledger fields preserve exact opaque handles for operator
        # GET readback (percent-encode each path segment, never normalize IDs).
        # Only the provider-qualified hash can enter public delivery metadata.
        provider_id = 'agentmail:' + digest(message_id)
        accepted = db.execute("UPDATE agentmail_send_ledger SET state='accepted',provider_id=?,message_id=?,thread_id=? WHERE key_digest=? AND state='prepared' AND fingerprint=?",
                              [provider_id, message_id, thread_id, digest(key), binding[0]])
        if accepted.rowcount != 1:
            db.rollback()
            return 'manual_review', None
        db.commit()
        return 'accepted', provider_id
    except Exception:
        db.rollback()
        # Never retain response text, headers, recipients or exception strings.
        # Even a 4xx/5xx/redirect may be ambiguous; prepared is enough to forbid
        # a retry if this evidence write itself fails or the process crashes.
        return 'manual_review', None


def health(db):
    cfg, reason = config()
    end = _expiry_time()
    counts = dict(approved=0, prepared=0, accepted=0, unknown=0)
    try:
        validate_schema(db)
        for row in db.execute('SELECT state,COUNT(*) FROM agentmail_send_ledger GROUP BY state'):
            counts[row[0]] = row[1]
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        used = db.execute('SELECT COUNT(*) FROM agentmail_send_ledger WHERE prepared_at >= ?', [today]).fetchone()[0]
        total = db.execute('SELECT COUNT(*) FROM agentmail_send_ledger WHERE prepared_at IS NOT NULL').fetchone()[0]
    except sqlite3.Error:
        reason, used, total = 'schema_unavailable', 0, 0
    except RuntimeError:
        reason, used, total = 'schema_invalid', 0, 0
    if reason == 'ready' and total >= cfg['total_cap']:
        reason = 'total_cap_reached'
    if reason == 'ready' and used >= cfg['cap']:
        reason = 'daily_cap_reached'
    return dict(counts, ready=reason == 'ready', blocked_reason=reason,
                expires_at=end.strftime('%Y-%m-%dT%H:%M:%SZ') if end else None,
                attempts_today=used, daily_cap=cfg['cap'] if cfg else None,
                attempts_total=total, total_cap=cfg['total_cap'] if cfg else None,
                delivery_tracking_enabled=False,
                max_outbox_id=db.execute('SELECT COALESCE(MAX(id),0) FROM transactional_email_outbox').fetchone()[0],
                max_notification_id=db.execute('SELECT COALESCE(MAX(id),0) FROM notifications').fetchone()[0])
