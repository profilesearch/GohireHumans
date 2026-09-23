"""Authenticated, at-rest wrapping of reset links in the durable email outbox.

Keep the 32-byte key stable across app/worker restarts. If absent or malformed,
requests return the generic response without issuing an undeliverable token.
"""
import base64
import hashlib
import os
import re
import secrets

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


def _cipher():
    value = os.environ.get('PASSWORD_RESET_ENCRYPTION_KEY', '')
    if not re.fullmatch(r'[0-9a-fA-F]{64}', value):
        return None
    return AESGCM(bytes.fromhex(value))


def configured():
    return _cipher() is not None


def seal(token, user_id):
    cipher = _cipher()
    if cipher is None:
        raise ValueError('reset key not configured')
    nonce = secrets.token_bytes(12)
    payload = cipher.encrypt(nonce, token.encode('ascii'), str(user_id).encode('ascii'))
    return 'reset:v1:' + base64.urlsafe_b64encode(nonce + payload).decode('ascii')


def open_token(sealed, user_id):
    cipher = _cipher()
    if cipher is None or not isinstance(sealed, str) or not sealed.startswith('reset:v1:'):
        return None
    try:
        raw = base64.b64decode(sealed[9:], altchars=b'-_', validate=True)
        token = cipher.decrypt(raw[:12], raw[12:], str(user_id).encode('ascii')).decode('ascii')
        if len(raw) < 29 or not re.fullmatch(r'[A-Za-z0-9_-]{43}', token):
            return None
        return token
    except (ValueError, UnicodeError, InvalidTag):
        return None


def active_token(db, sealed, user_id):
    token = open_token(sealed, user_id)
    if token is None:
        return None
    digest = hashlib.sha256(token.encode('ascii')).hexdigest()
    row = db.execute(
        """SELECT 1 FROM password_reset_tokens
           WHERE user_id=? AND token_hash=? AND used_at IS NULL
             AND expires_at > datetime('now')""", [user_id, digest],
    ).fetchone()
    return token if row else None
