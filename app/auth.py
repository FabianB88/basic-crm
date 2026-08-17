"""Authentication, sessions, CSRF and role checks.

Changes worth knowing about:

* Passwords were single-round SHA-256. Worse, the *change password* path built
  its salt as sha256(str(user_id))[:16] — fully predictable, so a changed
  password was effectively unsalted. Hashing is now scrypt (pbkdf2 fallback)
  with a random salt. Existing SHA-256 hashes still verify and are silently
  re-hashed on the next successful login, so nobody is locked out.

* Sessions were a module-level dict: a restart logged everyone out, abandoned
  sessions leaked forever and nothing ever expired. They now live in the
  database with an idle and an absolute timeout.

* CSRF tokens were keyed by user id and overwritten on every login, so signing
  in on a second device broke every form on the first. They are per session now.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from typing import Any, Dict, Optional, Tuple

from . import config
from .db import connect, execute, get_user_by_username_or_email, query_one

# ── Password hashing ──────────────────────────────────────────────────────
_SCRYPT_N = 2 ** 14
_SCRYPT_R = 8
_SCRYPT_P = 1
_DKLEN = 32
_PBKDF2_ROUNDS = 260_000


def _scrypt_available() -> bool:
    try:
        hashlib.scrypt(b'x', salt=b'y', n=2, r=8, p=1, dklen=16)
        return True
    except (ValueError, AttributeError):  # pragma: no cover - platform dependent
        return False


_USE_SCRYPT = _scrypt_available()


def hash_password(password: str) -> str:
    """Return a self-describing hash string."""
    salt = secrets.token_bytes(16)
    if _USE_SCRYPT:
        dk = hashlib.scrypt(password.encode('utf-8'), salt=salt, n=_SCRYPT_N,
                            r=_SCRYPT_R, p=_SCRYPT_P, dklen=_DKLEN,
                            maxmem=64 * 1024 * 1024)
        return f'scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${salt.hex()}${dk.hex()}'
    dk = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, _PBKDF2_ROUNDS, _DKLEN)
    return f'pbkdf2${_PBKDF2_ROUNDS}${salt.hex()}${dk.hex()}'


def verify_password(password: str, stored: str) -> Tuple[bool, bool]:
    """Check a password.

    Returns (ok, needs_rehash). needs_rehash is True for legacy SHA-256 hashes
    that verified correctly, so the caller can transparently upgrade them.
    """
    if not stored:
        return False, False
    try:
        if stored.startswith('scrypt$'):
            _, n, r, p, salt_hex, hash_hex = stored.split('$')
            dk = hashlib.scrypt(password.encode('utf-8'), salt=bytes.fromhex(salt_hex),
                                n=int(n), r=int(r), p=int(p), dklen=len(hash_hex) // 2,
                                maxmem=64 * 1024 * 1024)
            return hmac.compare_digest(dk.hex(), hash_hex), False
        if stored.startswith('pbkdf2$'):
            _, rounds, salt_hex, hash_hex = stored.split('$')
            dk = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'),
                                     bytes.fromhex(salt_hex), int(rounds), len(hash_hex) // 2)
            return hmac.compare_digest(dk.hex(), hash_hex), False
        # Legacy format: "<salt>$<sha256(salt + password)>"
        if '$' in stored:
            salt, digest = stored.split('$', 1)
            candidate = hashlib.sha256((salt + password).encode('utf-8')).hexdigest()
            return hmac.compare_digest(candidate, digest), True
    except (ValueError, TypeError):
        return False, False
    return False, False


def create_user(username: str, email: str, password: str,
                is_admin: bool = False) -> Tuple[bool, str]:
    """Create an account. Returns (success, message)."""
    username, email = username.strip(), email.strip()
    if not username or not email or not password:
        return False, 'Alle velden zijn verplicht.'
    if len(password) < config.MIN_PASSWORD_LENGTH:
        return False, f'Wachtwoord moet minimaal {config.MIN_PASSWORD_LENGTH} tekens zijn.'
    with connect() as conn:
        exists = conn.execute(
            'SELECT id FROM users WHERE username = ? OR email = ?', (username, email)
        ).fetchone()
        if exists:
            return False, 'Gebruikersnaam of e‑mail bestaat al.'
        conn.execute(
            'INSERT INTO users (username, email, password, is_admin) VALUES (?, ?, ?, ?)',
            (username, email, hash_password(password), 1 if is_admin else 0),
        )
    return True, 'Account aangemaakt. Je kunt nu inloggen.'


def verify_user(identifier: str, password: str) -> Optional[Dict[str, Any]]:
    """Validate credentials, upgrading a legacy hash on success."""
    user = get_user_by_username_or_email(identifier)
    if not user:
        # Spend comparable time so a missing user is not obviously faster.
        hash_password(password)
        return None
    ok, needs_rehash = verify_password(password, user['password'])
    if not ok:
        return None
    if needs_rehash:
        execute('UPDATE users SET password = ? WHERE id = ?',
                (hash_password(password), user['id']))
    return user


def set_password(user_id: int, new_password: str) -> None:
    execute('UPDATE users SET password = ? WHERE id = ?',
            (hash_password(new_password), user_id))


# ── Login throttling (per client IP, in memory) ───────────────────────────
_lockouts: Dict[str, Tuple[int, float]] = {}


def login_allowed(ip: str) -> Tuple[bool, int]:
    entry = _lockouts.get(ip)
    if not entry:
        return True, 0
    count, until = entry
    now = time.time()
    if until > now:
        return False, int(until - now) + 1
    if until:
        _lockouts.pop(ip, None)
    return True, 0


def record_login_failure(ip: str) -> None:
    now = time.time()
    entry = _lockouts.get(ip)
    count = entry[0] + 1 if entry and entry[1] <= now else 1
    until = now + config.LOGIN_LOCKOUT_SECONDS if count >= config.MAX_LOGIN_ATTEMPTS else 0.0
    _lockouts[ip] = (count, until)


def record_login_success(ip: str) -> None:
    _lockouts.pop(ip, None)


# ── Sessions ──────────────────────────────────────────────────────────────
_LAST_SEEN_REFRESH_SECONDS = 300


def create_session(user_id: int) -> Tuple[str, str]:
    """Start a session. Returns (session_id, csrf_token)."""
    session_id = secrets.token_urlsafe(32)
    csrf_token = secrets.token_urlsafe(32)
    execute(
        'INSERT INTO sessions (id, user_id, csrf_token) VALUES (?, ?, ?)',
        (session_id, user_id, csrf_token),
    )
    return session_id, csrf_token


def load_session(session_id: str) -> Optional[Dict[str, Any]]:
    """Return the session joined with its user, or None when absent/expired."""
    if not session_id:
        return None
    row = query_one('''
        SELECT s.id AS session_id, s.csrf_token, s.user_id,
               strftime('%s', s.created_at) AS created_ts,
               strftime('%s', s.last_seen)  AS seen_ts,
               u.username, u.email, u.is_admin, u.is_comm, u.is_governance
          FROM sessions s
          JOIN users u ON u.id = s.user_id
         WHERE s.id = ?
    ''', (session_id,))
    if not row:
        return None

    now = time.time()
    created = float(row['created_ts'] or now)
    seen = float(row['seen_ts'] or now)
    if now - seen > config.SESSION_IDLE_SECONDS or now - created > config.SESSION_MAX_SECONDS:
        destroy_session(session_id)
        return None

    # Only touch the row occasionally; a write on every request is expensive.
    if now - seen > _LAST_SEEN_REFRESH_SECONDS:
        execute("UPDATE sessions SET last_seen = CURRENT_TIMESTAMP WHERE id = ?", (session_id,))
    return dict(row)


def destroy_session(session_id: str) -> None:
    if session_id:
        execute('DELETE FROM sessions WHERE id = ?', (session_id,))


def destroy_user_sessions(user_id: int) -> None:
    """Drop every session for a user (used after a password change)."""
    execute('DELETE FROM sessions WHERE user_id = ?', (user_id,))


def purge_expired_sessions() -> None:
    execute(
        "DELETE FROM sessions "
        "WHERE strftime('%s','now') - strftime('%s', last_seen) > ? "
        "   OR strftime('%s','now') - strftime('%s', created_at) > ?",
        (config.SESSION_IDLE_SECONDS, config.SESSION_MAX_SECONDS),
    )


def csrf_ok(submitted: str, expected: str) -> bool:
    return bool(expected) and hmac.compare_digest(submitted or '', expected)


# ── Role checks ───────────────────────────────────────────────────────────
# Prefer the flags already loaded on the request context; these helpers exist
# for the few places that only have a user id.
def is_admin(user_id: Optional[int]) -> bool:
    if not user_id:
        return False
    if user_id == 1:
        return True
    row = query_one('SELECT is_admin FROM users WHERE id = ?', (user_id,))
    return bool(row and row['is_admin'])


def is_comm_member(user_id: Optional[int]) -> bool:
    if not user_id:
        return False
    row = query_one('SELECT is_admin, is_comm FROM users WHERE id = ?', (user_id,))
    return bool(row and (row['is_comm'] or row['is_admin'] or user_id == 1))


def is_gov_member(user_id: Optional[int]) -> bool:
    if not user_id:
        return False
    row = query_one('SELECT is_admin, is_governance FROM users WHERE id = ?', (user_id,))
    return bool(row and (row['is_governance'] or row['is_admin'] or user_id == 1))
