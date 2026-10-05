"""Default-off daily "your applicants" email for job owners, sent through AgentMail.

At most one email per job owner per UTC day. It lists, per open job, how many
new applications arrived since the owner last looked (or since the last digest)
and how many of those applicants can be hired right now (payout set up). For
hireable fixed-price jobs it also names up to three suggested, payout-ready
applicants (sanitized display name plus fixed-copy reasons, ranked over the
same pool as GET /jobs/{id}/applications). It never includes applicant emails,
cover messages, links, or anyone who is not suggested.

Safety contract (mirrors agentmail_transport):
- Off unless APPLICANT_DIGEST_ENABLED=true AND every other gate validates.
- A send intent row is committed BEFORE any HTTP and is never posted again.
  UNIQUE(employer_id, digest_date) makes a second email that day impossible.
- No SQLite writer is held during provider I/O.
- Any ambiguous provider outcome is recorded as 'unknown' and halts all
  digest sending for 24 hours instead of retrying.
- Known-not-sent outcomes are 'withheld' and never halt: a recipient on the
  AgentMail send block list (hard bounce, complaint, unsubscribe) is checked
  before sending and skipped, and AgentMail's documented 403
  code=message_rejected ("the message was not sent") is not ambiguous.
- Owners whose last digest was unclear go last, so one bad address cannot
  starve every owner behind it. (Under a saturated daily cap they can wait;
  at current volume the cap is far above the number of owners.)
- Owners can stop these emails with a signed one-click link.
"""
import hashlib
import hmac
import json
import os
import re
import sqlite3
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

try:
    import suggested_applicants
except ModuleNotFoundError as exc:
    if exc.name != 'suggested_applicants':
        raise
    # api_core may load this file by path; resolve the sibling the same way.
    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        'suggested_applicants', os.path.join(os.path.dirname(__file__), 'suggested_applicants.py'))
    if _spec is None or _spec.loader is None:
        raise ImportError('Suggested applicants module is unavailable')
    suggested_applicants = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(suggested_applicants)

SENDER = 'gohirehumans.operations@agentmail.to'
APP_BASE = 'https://www.gohirehumans.com'
API_BASE = 'https://gohirehumans-production.up.railway.app'
SEND_URL = 'https://api.agentmail.to/v0/inboxes/' + urllib.parse.quote(SENDER, safe='') + '/messages/send'
BLOCK_LIST_URL = 'https://api.agentmail.to/v0/lists/send/block/'
MAX_JOBS_PER_EMAIL = 5
MAX_PER_TICK = 5
MAX_TITLE_CHARS = 80
MAX_NAME_CHARS = 40
SUGGESTION_EXPLANATION = ('Suggestions are based on payout setup, how closely the message matches your task, '
                          'portfolio links and past work on GoHireHumans. You choose who to hire.')
HALT_WINDOW = timedelta(hours=24)
EMAIL_RE = r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+"

SENDS_SQL = """CREATE TABLE applicant_digest_sends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employer_id INTEGER NOT NULL REFERENCES users(id),
    digest_date TEXT NOT NULL CHECK(length(digest_date)=10),
    state TEXT NOT NULL CHECK(state IN ('prepared','accepted','unknown','withheld')),
    fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),
    jobs_count INTEGER NOT NULL CHECK(jobs_count>=1),
    applications_count INTEGER NOT NULL CHECK(applications_count>=1),
    ready_count INTEGER NOT NULL CHECK(ready_count>=0),
    prepared_at TEXT NOT NULL,
    resolved_at TEXT,
    message_id TEXT,
    thread_id TEXT,
    UNIQUE(employer_id, digest_date),
    CHECK((state='accepted' AND message_id IS NOT NULL AND thread_id IS NOT NULL)
       OR (state!='accepted' AND message_id IS NULL AND thread_id IS NULL))
)"""
ITEMS_SQL = """CREATE TABLE applicant_digest_items (
    send_id INTEGER NOT NULL REFERENCES applicant_digest_sends(id),
    job_id INTEGER NOT NULL,
    max_application_id INTEGER NOT NULL CHECK(max_application_id>=1),
    applications_count INTEGER NOT NULL CHECK(applications_count>=1),
    ready_count INTEGER NOT NULL CHECK(ready_count>=0),
    PRIMARY KEY(send_id, job_id)
)"""
PREFS_SQL = """CREATE TABLE email_preferences (
    user_id INTEGER PRIMARY KEY NOT NULL REFERENCES users(id),
    applicant_digest_opt_out INTEGER NOT NULL DEFAULT 0 CHECK(applicant_digest_opt_out IN (0,1)),
    updated_at TEXT NOT NULL
)"""
# name -> (exact DDL, expected autoindex origins). INTEGER PRIMARY KEY is the rowid (no index).
TABLES = {
    'applicant_digest_sends': (SENDS_SQL, ('u',)),
    'applicant_digest_items': (ITEMS_SQL, ('pk',)),
    'email_preferences': (PREFS_SQL, ()),
}


# ── schema ──────────────────────────────────────────────────────────────────
def init_schema(db):
    for name, (sql, _) in TABLES.items():
        if db.execute("SELECT 1 FROM sqlite_master WHERE name=?", [name]).fetchone() is None:
            db.execute(sql)
    validate_schema(db)


def validate_schema(db):
    """Accept only our exact DDL: a same-name weakened table must not pass."""
    for name, (sql, origins) in TABLES.items():
        row = db.execute("SELECT type,sql FROM sqlite_master WHERE name=?", [name]).fetchone()
        if row is None or tuple(row) != ('table', sql):
            raise RuntimeError('applicant_digest_schema_invalid')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND tbl_name=?", [name]).fetchone():
            raise RuntimeError('applicant_digest_schema_invalid')
        indexes = db.execute(f'PRAGMA index_list({name})').fetchall()
        if sorted(r[3] for r in indexes) != sorted(origins) or any(r[2] != 1 or r[4] != 0 for r in indexes):
            raise RuntimeError('applicant_digest_schema_invalid')


# ── configuration ───────────────────────────────────────────────────────────
def _secret():
    value = os.environ.get('APPLICANT_DIGEST_SECRET', '')
    return value.encode('utf-8') if len(value) >= 32 and value.isascii() and not any(c.isspace() for c in value) else None


def config(now=None):
    """Return (cfg, 'ready') or (None, reason). Unset/unknown values mean OFF."""
    env = os.environ
    if env.get('APPLICANT_DIGEST_ENABLED') != 'true':
        return None, 'disabled'
    key = env.get('AGENTMAIL_API_KEY', '')
    if not key or not key.isascii() or any(c.isspace() or ord(c) < 33 or ord(c) > 126 for c in key):
        return None, 'key_missing_or_invalid'
    if env.get('AGENTMAIL_INBOX_ID') != SENDER:
        return None, 'sender_invalid'
    secret = _secret()
    if secret is None:
        return None, 'secret_invalid'
    recipients = env.get('APPLICANT_DIGEST_RECIPIENTS', '')
    if recipients == 'all':
        allow = None
    else:
        parts = [p.strip().lower() for p in recipients.split(',')]
        if not recipients or any(not re.fullmatch(EMAIL_RE, p) for p in parts):
            return None, 'recipients_invalid'
        allow = frozenset(parts)
    cap = env.get('APPLICANT_DIGEST_DAILY_CAP', '10')
    if not re.fullmatch(r'[1-9][0-9]?', cap) or not 1 <= int(cap) <= 50:
        return None, 'daily_cap_invalid'
    hour = env.get('APPLICANT_DIGEST_SEND_HOUR_UTC', '14')
    if not re.fullmatch(r'[0-9]{1,2}', hour) or not 0 <= int(hour) <= 23:
        return None, 'send_hour_invalid'
    lookback = env.get('APPLICANT_DIGEST_LOOKBACK_DAYS', '14')
    if not re.fullmatch(r'[1-9][0-9]?', lookback) or not 1 <= int(lookback) <= 30:
        return None, 'lookback_invalid'
    end = None
    expires = env.get('APPLICANT_DIGEST_EXPIRES_AT', '')
    if expires:
        try:
            end = datetime.strptime(expires, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        except (TypeError, ValueError, OverflowError):
            return None, 'expiry_invalid'
        if (now or datetime.now(timezone.utc)) >= end:
            return None, 'expired'
    return dict(key=key, secret=secret, allow=allow, cap=int(cap), hour=int(hour),
                lookback=int(lookback), end=end), 'ready'


# ── unsubscribe tokens ──────────────────────────────────────────────────────
def unsubscribe_token(user_id, secret=None):
    secret = secret or _secret()
    if secret is None or not isinstance(user_id, int) or user_id <= 0:
        return None
    mac = hmac.new(secret, f'applicant_digest_unsubscribe:v1:{user_id}'.encode(), hashlib.sha256).hexdigest()
    return f'{user_id}.{mac[:32]}'


def verify_unsubscribe_token(token):
    """Return the user id for a valid token, else None. Constant-time compare."""
    if not isinstance(token, str) or len(token) > 64:
        return None
    match = re.fullmatch(r'([1-9][0-9]{0,17})\.([0-9a-f]{32})', token)
    if not match:
        return None
    user_id = int(match.group(1))
    expected = unsubscribe_token(user_id)
    if expected is None or not hmac.compare_digest(expected, token):
        return None
    return user_id


def opt_out(db, user_id):
    """Idempotent opt-out; returns True when the user exists."""
    if db.execute('SELECT 1 FROM users WHERE id=?', [user_id]).fetchone() is None:
        return False
    db.execute("""INSERT INTO email_preferences(user_id,applicant_digest_opt_out,updated_at)
                  VALUES(?,1,datetime('now'))
                  ON CONFLICT(user_id) DO UPDATE SET applicant_digest_opt_out=1, updated_at=datetime('now')""",
               [user_id])
    return True


# ── selection (pure reads) ──────────────────────────────────────────────────
def _clean_title(title):
    # Cc (C0/C1 controls), Cf (bidi overrides, zero-width), Zl/Zp (line/para separators)
    text = ''.join(' ' if unicodedata.category(ch) in ('Cc', 'Cf', 'Zl', 'Zp', 'Cs', 'Co', 'Cn') else ch
                   for ch in str(title or ''))
    text = re.sub(r'\s+', ' ', text).strip()
    return (text[:MAX_TITLE_CHARS - 1].rstrip() + '…') if len(text) > MAX_TITLE_CHARS else (text or 'Your job')


def _clean_name(name):
    """Keep letters, combining marks, spaces, period, apostrophe and hyphen; never contact tokens."""
    # NFKC first: ligatures/fullwidth forms (ﬁ, ｅ, ．) become the ASCII a mail
    # client or hostname normalizer would see, so the checks below see it too.
    text = unicodedata.normalize('NFKC', str(name or ''))
    text = ''.join(' ' if unicodedata.category(ch) in ('Cc', 'Cf', 'Zl', 'Zp', 'Cs', 'Co', 'Cn') else ch
                   for ch in text)
    words = []
    for token in text.split():
        # Strip complete contact/link tokens before filtering punctuation, rather
        # than turning http://x.y into httpx.y.
        if '@' in token or '://' in token or token.lower().startswith('www.'):
            continue
        kept = ''.join(ch for ch in token
                       if ch.isalpha() or unicodedata.category(ch).startswith('M') or ch in "'-.")
        # Filtering can itself create a domain (evil.(com) -> evil.com), and a
        # combining mark after the dot must not hide one. Check the final token
        # without marks; only single ASCII-letter initials (J.P.) are exempt.
        skeleton = ''.join(ch for ch in kept if not unicodedata.category(ch).startswith('M'))
        initials = bool(re.fullmatch(r'(?:[A-Za-z]\.)+', skeleton))
        if re.search(r'\.\w', skeleton) and not initials:
            continue
        if any(ch.isalpha() for ch in kept):
            words.append(kept)
    return ' '.join(words)[:MAX_NAME_CHARS].rstrip() or 'Applicant'


def _suggestions(db, job_row, hiring_enabled):
    """Ranked [{rank, display_name, reasons}] over the job's full applicant pool (read-only)."""
    ranked = suggested_applicants.suggest(db, job_row, hiring_enabled)
    if not ranked:
        return []
    marks = ','.join('?' * len(ranked))
    names = {r['id']: r['name'] for r in db.execute(
        f"""SELECT a.id, u.name FROM applications a JOIN users u ON u.id=a.worker_id
            WHERE a.id IN ({marks})""", list(ranked)).fetchall()}
    return [dict(rank=v['rank'], display_name=names.get(aid, ''), reasons=list(v['reasons']))
            for aid, v in sorted(ranked.items(), key=lambda item: item[1]['rank'])]


def _excluded_owner_sql(agent_sql, sample_emails):
    marks = ','.join('?' * len(sample_emails))
    sample = f"LOWER(TRIM(u.email)) IN ({marks})" if sample_emails else '0'
    return f"({agent_sql} OR {sample} OR COALESCE(u.is_admin,0)=1)", list(sample_emails)


def candidate_jobs(db, employer_id, now, lookback_days, hiring_enabled):
    """New, unseen, not-yet-digested applications per open job for one owner."""
    since = (now - timedelta(days=lookback_days)).strftime('%Y-%m-%d %H:%M:%S')
    rows = db.execute(
        """SELECT j.id, j.title, j.description, j.status, j.budget_type,
                  COUNT(a.id) AS n, MAX(a.id) AS max_id,
                  SUM(CASE WHEN COALESCE(wp.payout_method,'')='stripe_connect_active' THEN 1 ELSE 0 END) AS ready
           FROM jobs j
           JOIN applications a ON a.job_id=j.id AND a.status IN ('pending','shortlisted')
           JOIN users w ON w.id=a.worker_id AND w.is_active=1 AND w.is_banned=0 AND w.is_suspended=0
           LEFT JOIN worker_profiles wp ON wp.user_id=a.worker_id
           LEFT JOIN job_application_views v ON v.job_id=j.id AND v.employer_id=j.employer_id
           WHERE j.employer_id=? AND j.status IN ('open','reviewing')
             AND a.created_at >= ?
             AND a.id > COALESCE(v.last_seen_application_id, 0)
             AND a.id > COALESCE((SELECT MAX(i.max_application_id)
                                  FROM applicant_digest_items i
                                  JOIN applicant_digest_sends s ON s.id=i.send_id
                                  WHERE i.job_id=j.id AND s.employer_id=j.employer_id
                                    AND s.state IN ('prepared','accepted','unknown')), 0)
           GROUP BY j.id
           ORDER BY ready DESC, n DESC, j.id DESC""",
        [employer_id, since]).fetchall()
    jobs = []
    for r in rows[:MAX_JOBS_PER_EMAIL]:
        hireable = hiring_enabled and r['budget_type'] == 'fixed'
        jobs.append(dict(job_id=r['id'], title=_clean_title(r['title']), count=int(r['n']),
                         max_application_id=int(r['max_id']),
                         ready=int(r['ready'] or 0) if hireable else 0, hireable=hireable,
                         suggestions=_suggestions(db, r, hiring_enabled) if hireable else []))
    return jobs


def eligible_owners(db, now, cfg, agent_sql, sample_emails, limit, hiring_enabled=False):
    """Owners with something new to report, not yet emailed today.

    Every returned owner has non-empty candidate_jobs, so owners whose recent
    applications were already seen or digested can never fill the per-tick
    limit and starve owners further down the list.
    """
    today = now.strftime('%Y-%m-%d')
    since = (now - timedelta(days=cfg['lookback'])).strftime('%Y-%m-%d %H:%M:%S')
    excluded, args = _excluded_owner_sql(agent_sql, sample_emails)
    rows = db.execute(
        f"""SELECT DISTINCT u.id, LOWER(TRIM(u.email)) AS email,
                   COALESCE((SELECT s.state FROM applicant_digest_sends s WHERE s.employer_id=u.id
                             ORDER BY s.digest_date DESC, s.id DESC LIMIT 1)='unknown', 0) AS last_unclear
            FROM users u
            JOIN jobs j ON j.employer_id=u.id AND j.status IN ('open','reviewing')
            JOIN applications a ON a.job_id=j.id AND a.status IN ('pending','shortlisted') AND a.created_at >= ?
            LEFT JOIN email_preferences p ON p.user_id=u.id
            WHERE u.is_active=1 AND u.is_banned=0 AND u.is_suspended=0
              AND u.email IS NOT NULL AND TRIM(u.email) <> ''
              AND COALESCE(p.applicant_digest_opt_out,0)=0
              AND NOT {excluded}
              AND NOT EXISTS (SELECT 1 FROM applicant_digest_sends s WHERE s.employer_id=u.id AND s.digest_date=?)
            ORDER BY last_unclear, u.id""",
        [since, *args, today]).fetchall()
    out = []
    for r in rows:
        if cfg['allow'] is not None and r['email'] not in cfg['allow']:
            continue
        if not candidate_jobs(db, r['id'], now, cfg['lookback'], hiring_enabled):
            continue
        out.append((r['id'], r['email']))
        if len(out) >= limit:
            break
    return out


# ── rendering ───────────────────────────────────────────────────────────────
def render(jobs, token):
    total = sum(j['count'] for j in jobs)
    ready = sum(j['ready'] for j in jobs)
    s = lambda n: '' if n == 1 else 's'
    if ready:
        subject = f'{ready} applicant{s(ready)} ready to hire on GoHireHumans'
    else:
        subject = f'{total} new applicant{s(total)} on your GoHireHumans job{s(len(jobs))}'
    lines = ['Hello,', '', 'Here is your daily summary of new applicants on GoHireHumans.', '']
    any_hireable = any_suggested = False
    for j in jobs:
        line = f'- "{j["title"]}": {j["count"]} new applicant{s(j["count"])}'
        if j['hireable']:
            any_hireable = True
            line += (f', {j["ready"]} ready to hire now' if j['ready']
                     else ', none ready to hire yet')
        lines.append(line)
        suggested = (j.get('suggestions') or [])[:3] if j['hireable'] else []
        if suggested:
            any_suggested = True
            lines.append('  Suggested applicants (ready to hire):')
            lines += [f'  {n}. {_clean_name(item.get("display_name"))}: '
                      + '; '.join(str(reason) for reason in (item.get('reasons') or ['Ready to hire'])[:3])
                      for n, item in enumerate(suggested, 1)]
        lines += [f'  Review them: {APP_BASE}/#/jobs/{j["job_id"]}/applicants', '']
    if any_suggested:
        lines += [SUGGESTION_EXPLANATION, '']
    if any_hireable:
        lines += ['"Ready to hire" means the applicant has finished payout setup, so you can hire them '
                  'right away. Other applicants can be hired once they finish setup.', '']
    unsubscribe = f'{APP_BASE}/email-preferences/?t={token}'
    lines += ['GoHireHumans', '',
              'You get this email at most once a day because you posted a job on GoHireHumans.',
              f'Stop these emails: {unsubscribe}']
    payload = dict(
        to=None, reply_to=[SENDER], subject=subject, text='\n'.join(lines),
        headers={
            'List-Unsubscribe': f'<{API_BASE}/email-preferences/one-click?t={token}>',
            'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
        })
    return payload, total, ready


def _digest(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


# ── sending ─────────────────────────────────────────────────────────────────
def _opener():
    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    return urllib.request.build_opener(_NoRedirect())


def _recipient_email(db, employer_id, agent_sql, sample_emails):
    """Current address if the owner may still be emailed, else None.

    Same exclusions as eligible_owners (agent, sample, admin, inactive, banned,
    suspended, opted out), so a change after selection is caught before I/O.
    """
    excluded, args = _excluded_owner_sql(agent_sql, sample_emails)
    row = db.execute(
        f"""SELECT LOWER(TRIM(u.email)) AS email FROM users u LEFT JOIN email_preferences p ON p.user_id=u.id
            WHERE u.id=? AND u.is_active=1 AND u.is_banned=0 AND u.is_suspended=0
              AND COALESCE(p.applicant_digest_opt_out,0)=0
              AND NOT {excluded}""", [employer_id, *args]).fetchone()
    return row[0] if row else None


def _is_owner_day_collision(exc):
    """True only for the UNIQUE(employer_id, digest_date) violation."""
    name = getattr(exc, 'sqlite_errorname', None)
    if name is not None and name != 'SQLITE_CONSTRAINT_UNIQUE':
        return False
    return 'applicant_digest_sends.employer_id, applicant_digest_sends.digest_date' in str(exc)


def _halted(db, now):
    since = (now - HALT_WINDOW).strftime('%Y-%m-%d %H:%M:%S')
    return db.execute("""SELECT 1 FROM applicant_digest_sends
                         WHERE state IN ('prepared','unknown') AND prepared_at >= ? LIMIT 1""",
                      [since]).fetchone() is not None


def _sends_used(db, today):
    """Every intent today uses a cap slot, withheld ones included: a provider that
    rejects every send (e.g. a suspended account) must not walk the whole owner list."""
    return db.execute("SELECT COUNT(*) FROM applicant_digest_sends WHERE digest_date=?",
                      [today]).fetchone()[0]


def _send_blocked(opener, key, email):
    """True only when AgentMail confirms this exact address is on the org send block list.

    Read-only GET; any failure or unexpected answer means "not known to be
    blocked", so the normal send (and its unclear-outcome halt) still applies.
    """
    request = urllib.request.Request(
        BLOCK_LIST_URL + urllib.parse.quote(email, safe=''), method='GET',
        headers={'Authorization': 'Bearer ' + key})
    try:
        response = opener.open(request, timeout=5)
        try:
            body, status = response.read(16385), response.status
        finally:
            response.close()
        entry = json.loads(body) if status == 200 and len(body) <= 16384 else None
    except Exception:
        return False
    return (isinstance(entry, dict) and entry.get('list_type') == 'block' and entry.get('direction') == 'send'
            and isinstance(entry.get('entry'), str) and entry['entry'].strip().lower() == email)


def _rejected(exc):
    """True only for AgentMail's documented 403 code=message_rejected: the message was not sent."""
    if not isinstance(exc, urllib.error.HTTPError) or exc.code != 403:
        return False
    try:
        try:
            body = exc.read(16385)
        finally:
            exc.close()
        parsed = json.loads(body) if len(body) <= 16384 else None
    except Exception:
        return False
    return isinstance(parsed, dict) and parsed.get('code') == 'message_rejected'


def _resolve(db, send_id, state, message_id=None, thread_id=None):
    if db.in_transaction:
        db.commit()
    db.execute('BEGIN IMMEDIATE')
    try:
        changed = db.execute(
            """UPDATE applicant_digest_sends SET state=?, message_id=?, thread_id=?, resolved_at=datetime('now')
               WHERE id=? AND state='prepared'""",
            [state, message_id, thread_id, send_id]).rowcount
        db.commit()
        return changed == 1
    except Exception:
        db.rollback()
        raise


def run_once(db, *, now=None, renew_lease=None, agent_sql='0', sample_emails=(),
             hiring_enabled=False, audit=None, opener=None):
    """One bounded pass. Returns aggregate counts only (no recipient data)."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    sample_emails = tuple(sample_emails)
    summary = dict(status='idle', attempted=0, accepted=0, unknown=0, withheld=0, skipped=0)
    cfg, reason = config(now)
    if cfg is None:
        summary['status'] = reason
        return summary
    validate_schema(db)
    if now.hour < cfg['hour']:
        summary['status'] = 'before_send_hour'
        return summary
    if _halted(db, now):
        summary['status'] = 'halted_unresolved_attempt'
        return summary
    today = now.strftime('%Y-%m-%d')
    used = _sends_used(db, today)
    if used >= cfg['cap']:
        summary['status'] = 'daily_cap_reached'
        return summary
    owners = eligible_owners(db, now, cfg, agent_sql, tuple(sample_emails),
                             min(MAX_PER_TICK, cfg['cap'] - used), hiring_enabled)
    if db.in_transaction:
        db.commit()
    summary['status'] = 'ran'
    opener = opener or _opener()
    for employer_id, email in owners:
        if renew_lease is not None and not renew_lease():
            summary['status'] = 'lease_lost'
            break
        db.execute('BEGIN IMMEDIATE')
        try:
            jobs = candidate_jobs(db, employer_id, now, cfg['lookback'], hiring_enabled)
            used = _sends_used(db, today)
            if not jobs or used >= cfg['cap'] or _recipient_email(db, employer_id, agent_sql, sample_emails) != email:
                db.rollback()
                summary['skipped'] += 1
                continue
            payload, total, ready = render(jobs, unsubscribe_token(employer_id, cfg['secret']))
            payload['to'] = [email]
            fingerprint = _digest(json.dumps([employer_id, today, payload], sort_keys=True, separators=(',', ':')))
            try:
                send_id = db.execute(
                    """INSERT INTO applicant_digest_sends
                       (employer_id,digest_date,state,fingerprint,jobs_count,applications_count,ready_count,prepared_at)
                       VALUES(?,?,'prepared',?,?,?,?,?)""",
                    [employer_id, today, fingerprint, len(jobs), total, ready,
                     now.strftime('%Y-%m-%d %H:%M:%S')]).lastrowid
            except sqlite3.IntegrityError as exc:
                if not _is_owner_day_collision(exc):
                    raise  # any other constraint is a real bug: outer handler rolls back
                db.rollback()
                summary['skipped'] += 1  # a concurrent worker already owns today's email
                continue
            for j in jobs:
                db.execute("""INSERT INTO applicant_digest_items(send_id,job_id,max_application_id,applications_count,ready_count)
                              VALUES(?,?,?,?,?)""", [send_id, j['job_id'], j['max_application_id'], j['count'], j['ready']])
            if audit is not None:
                audit(db, None, 'applicant_digest_prepared', 'user', employer_id,
                      {'send_id': send_id, 'jobs': len(jobs), 'applications': total, 'ready': ready})
            db.commit()  # irreversible intent: this owner gets no second attempt today
        except Exception:
            db.rollback()
            raise
        summary['attempted'] += 1
        # A bounced/complained/unsubscribed address is on the send block list and
        # every send to it is refused: known not sent, so skip it without halting.
        if _send_blocked(opener, cfg['key'], email):
            _resolve(db, send_id, 'withheld')
            summary['withheld'] += 1
            continue
        # Fence right before I/O. Anything that changed since the intent commit
        # withholds this email (known not sent): gate, lease, or the recipient's
        # own eligibility (opt-out, ban, address change). An opt-out that lands
        # after this point races the provider call and cannot be recalled.
        current_cfg, _ = config(datetime.now(timezone.utc))
        if current_cfg is None or current_cfg['allow'] != cfg['allow'] or current_cfg['key'] != cfg['key']:
            _resolve(db, send_id, 'withheld')
            summary['withheld'] += 1
            summary['status'] = 'gate_changed'
            break
        if renew_lease is not None and not renew_lease():
            _resolve(db, send_id, 'withheld')
            summary['withheld'] += 1
            summary['status'] = 'lease_lost'
            break
        if db.in_transaction:
            db.commit()
        if _recipient_email(db, employer_id, agent_sql, sample_emails) != email:
            if db.in_transaction:
                db.commit()
            _resolve(db, send_id, 'withheld')
            summary['withheld'] += 1
            continue
        if db.in_transaction:
            db.commit()
        request = urllib.request.Request(
            SEND_URL, data=json.dumps(payload, separators=(',', ':')).encode('utf-8'), method='POST',
            headers={'Authorization': 'Bearer ' + cfg['key'], 'Content-Type': 'application/json',
                     'Idempotency-Key': 'ghh-applicant-digest-' + _digest(f'{employer_id}:{today}')})
        message_id = thread_id = None
        rejected = False
        try:
            response = opener.open(request, timeout=10)
            try:
                body, status = response.read(16385), response.status
            finally:
                response.close()
            parsed = json.loads(body) if status == 200 and len(body) <= 16384 else None
            if isinstance(parsed, dict):
                message_id, thread_id = parsed.get('message_id'), parsed.get('thread_id')
            if any(not isinstance(v, str) or not re.fullmatch(r'[!-~]{1,998}', v) for v in (message_id, thread_id)):
                message_id = thread_id = None
        except Exception as exc:
            # Never keep response text, headers or recipients. Ambiguous = unknown,
            # except the documented "message was not sent" rejection.
            message_id = thread_id = None
            rejected = _rejected(exc)
        if message_id and _resolve(db, send_id, 'accepted', message_id, thread_id):
            summary['accepted'] += 1
        elif rejected and _resolve(db, send_id, 'withheld'):
            summary['withheld'] += 1
        else:
            _resolve(db, send_id, 'unknown')
            summary['unknown'] += 1
            summary['status'] = 'halted_unresolved_attempt'
            break  # stop the whole pass; the 24h halt prevents a retry storm
    return summary


def health(db, now=None):
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cfg, reason = config(now)
    try:
        validate_schema(db)
    except RuntimeError:
        return dict(ready=False, blocked_reason='schema_invalid')
    today = now.strftime('%Y-%m-%d')
    states = {s: 0 for s in ('prepared', 'accepted', 'unknown', 'withheld')}
    for row in db.execute('SELECT state,COUNT(*) FROM applicant_digest_sends GROUP BY state'):
        states[row[0]] = row[1]
    if reason == 'ready' and _halted(db, now):
        reason = 'halted_unresolved_attempt'
    return dict(
        ready=reason == 'ready', blocked_reason=reason, states=states,
        sent_today=db.execute('SELECT COUNT(*) FROM applicant_digest_sends WHERE digest_date=?', [today]).fetchone()[0],
        daily_cap=cfg['cap'] if cfg else None, send_hour_utc=cfg['hour'] if cfg else None,
        recipients_scope=('all' if cfg and cfg['allow'] is None else (len(cfg['allow']) if cfg else None)),
        opted_out=db.execute('SELECT COUNT(*) FROM email_preferences WHERE applicant_digest_opt_out=1').fetchone()[0],
        last_accepted_at=db.execute("SELECT MAX(resolved_at) FROM applicant_digest_sends WHERE state='accepted'").fetchone()[0],
    )
