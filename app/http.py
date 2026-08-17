"""Per-request context.

Views are plain functions taking a Ctx. Everything they need — the current
user, parsed query/form values, and the response helpers — hangs off it, so no
view has to touch the BaseHTTPRequestHandler directly.

The user's roles are resolved once per request here. Previously html_header()
called is_admin(), is_comm_member() and is_gov_member(), each opening its own
SQLite connection, on top of parse_session() opening another — roughly eight to
ten connections to render one page.
"""

from __future__ import annotations

import html
import json
import urllib.parse
from typing import Any, Dict, List, Optional

from . import auth, config

MAX_BODY_BYTES = 8 * 1024 * 1024


class Ctx:
    """State and helpers for a single HTTP request."""

    def __init__(self, handler) -> None:
        self.handler = handler
        # HEAD is routed as GET and answered with headers only, so health
        # checks and link probes get a real status instead of 405.
        self.is_head: bool = handler.command == 'HEAD'
        self.method: str = 'GET' if self.is_head else handler.command
        parsed = urllib.parse.urlparse(handler.path)
        self.path: str = parsed.path
        self.query: Dict[str, List[str]] = urllib.parse.parse_qs(parsed.query)

        self._body: Optional[bytes] = None
        self._form: Optional[Dict[str, List[str]]] = None
        self.responded = False

        # ── Session ──
        self.session_id: Optional[str] = self._cookie('session_id')
        session = auth.load_session(self.session_id) if self.session_id else None
        if session:
            self.logged_in = True
            self.user_id: Optional[int] = session['user_id']
            self.username: Optional[str] = session['username']
            self.email: Optional[str] = session['email']
            self.csrf_token: str = session['csrf_token']
            self.is_admin = bool(session['is_admin']) or session['user_id'] == 1
            self.is_comm = bool(session['is_comm']) or self.is_admin
            self.is_gov = bool(session['is_governance']) or self.is_admin
        else:
            self.logged_in = False
            self.user_id = None
            self.username = None
            self.email = None
            self.csrf_token = ''
            self.is_admin = self.is_comm = self.is_gov = False

    # ── Request data ──────────────────────────────────────────────────────
    def _cookie(self, name: str) -> Optional[str]:
        raw = self.handler.headers.get('Cookie')
        if not raw:
            return None
        for part in raw.split(';'):
            if '=' in part:
                key, value = part.strip().split('=', 1)
                if key == name:
                    return value
        return None

    @property
    def client_ip(self) -> str:
        forwarded = self.handler.headers.get('X-Forwarded-For')
        if forwarded:
            return forwarded.split(',')[0].strip()
        return self.handler.client_address[0]

    def body(self) -> bytes:
        """Read the request body once, capped."""
        if self._body is None:
            try:
                length = int(self.handler.headers.get('Content-Length', '0') or 0)
            except ValueError:
                length = 0
            length = max(0, min(length, MAX_BODY_BYTES))
            self._body = self.handler.rfile.read(length) if length else b''
        return self._body

    def drain(self) -> None:
        """Consume any unread request body.

        Connections are keep-alive (HTTP/1.1). A handler that answers before
        reading the body — a role redirect, a rejected CSRF token, a 405 on a
        POST-only route — leaves those bytes in the socket, and the next
        request on that connection gets parsed starting from them. That showed
        up as `code 501, Unsupported method ('username=...&password=...GET')`
        and silently swallowed the following request.
        """
        try:
            self.body()
        except Exception:
            pass

    def form(self) -> Dict[str, List[str]]:
        """Parsed urlencoded POST body."""
        if self._form is None:
            ctype = self.handler.headers.get('Content-Type', '') or ''
            if 'multipart/form-data' in ctype:
                self._form = {}
            else:
                self._form = urllib.parse.parse_qs(self.body().decode('utf-8', 'replace'))
        return self._form

    # ── Value accessors ───────────────────────────────────────────────────
    def q(self, name: str, default: str = '') -> str:
        return (self.query.get(name, [default]) or [default])[0].strip()

    def qint(self, name: str, default: Optional[int] = None) -> Optional[int]:
        try:
            return int(self.q(name))
        except (TypeError, ValueError):
            return default

    def f(self, name: str, default: str = '') -> str:
        return (self.form().get(name, [default]) or [default])[0].strip()

    def fint(self, name: str, default: Optional[int] = None) -> Optional[int]:
        try:
            return int(self.f(name))
        except (TypeError, ValueError):
            return default

    def flist(self, name: str) -> List[str]:
        return self.form().get(name, [])

    def choice(self, value: str, allowed, fallback):
        """Clamp a value to a known vocabulary."""
        return value if value in allowed else fallback

    # ── CSRF ──────────────────────────────────────────────────────────────
    def csrf_input(self) -> str:
        return f'<input type="hidden" name="csrf_token" value="{html.escape(self.csrf_token)}">'

    def csrf_valid(self) -> bool:
        return auth.csrf_ok(self.f('csrf_token'), self.csrf_token)

    # ── Responses ─────────────────────────────────────────────────────────
    def _start(self, status: int, content_type: str, extra: Optional[List] = None,
               length: Optional[int] = None) -> None:
        self.responded = True
        self.handler.send_response(status)
        self.handler.send_header('Content-Type', content_type)
        if length is not None:
            self.handler.send_header('Content-Length', str(length))
        # Cheap hardening; costs nothing and blocks a class of silly mistakes.
        self.handler.send_header('X-Content-Type-Options', 'nosniff')
        self.handler.send_header('X-Frame-Options', 'DENY')
        self.handler.send_header('Referrer-Policy', 'same-origin')
        for key, value in (extra or []):
            self.handler.send_header(key, value)
        self.handler.end_headers()

    def _write(self, payload: bytes) -> None:
        if not self.is_head:          # HEAD gets the headers and nothing else
            self.handler.wfile.write(payload)

    def html(self, body: str, status: int = 200) -> None:
        payload = body.encode('utf-8')
        self._start(status, 'text/html; charset=utf-8', length=len(payload))
        self._write(payload)

    def json(self, data: Any, status: int = 200) -> None:
        payload = json.dumps(data).encode('utf-8')
        self._start(status, 'application/json; charset=utf-8', length=len(payload))
        self._write(payload)

    def csv(self, filename: str, payload: bytes) -> None:
        self._start(200, 'text/csv; charset=utf-8',
                    extra=[('Content-Disposition', f'attachment; filename="{filename}"')],
                    length=len(payload))
        self._write(payload)

    def no_content(self, status: int = 204) -> None:
        self.responded = True
        self.handler.send_response(status)
        self.handler.send_header('Content-Length', '0')
        self.handler.end_headers()

    def redirect(self, location: str) -> None:
        """Redirect, refusing anything that would leave the site."""
        if not location.startswith('/') or location.startswith('//'):
            location = '/dashboard'
        self.responded = True
        self.handler.send_response(302)
        self.handler.send_header('Location', location)
        self.handler.send_header('Content-Length', '0')
        self.handler.end_headers()

    def set_session_cookie(self, session_id: str, location: str) -> None:
        flags = 'Path=/; HttpOnly; SameSite=Lax'
        if config.COOKIE_SECURE:
            flags += '; Secure'
        self.responded = True
        self.handler.send_response(302)
        self.handler.send_header('Location', location)
        self.handler.send_header('Set-Cookie', f'session_id={session_id}; {flags}')
        self.handler.send_header('Content-Length', '0')
        self.handler.end_headers()

    def clear_session_cookie(self, location: str = '/login') -> None:
        flags = 'Path=/; Max-Age=0; HttpOnly; SameSite=Lax'
        if config.COOKIE_SECURE:
            flags += '; Secure'
        self.responded = True
        self.handler.send_response(302)
        self.handler.send_header('Location', location)
        self.handler.send_header('Set-Cookie', f'session_id=; {flags}')
        self.handler.send_header('Content-Length', '0')
        self.handler.end_headers()

    # ── Error pages ───────────────────────────────────────────────────────
    def _error_page(self, status: int, heading: str, message: str) -> None:
        from .ui import page_footer, page_header
        body = page_header(heading, self)
        body += (f'<h2 class="mt-4">{html.escape(heading)}</h2>'
                 f'<div class="card"><p>{html.escape(message)}</p>'
                 f'<p class="mt-3"><a href="/" class="btn btn-primary">Terug naar start</a></p>'
                 f'</div>')
        body += page_footer()
        self.html(body, status=status)

    def not_found(self) -> None:
        self._error_page(404, 'Niet gevonden', 'Deze pagina of dit item bestaat niet (meer).')

    def forbidden(self, message: str = 'Je hebt geen toegang tot dit onderdeel.') -> None:
        self._error_page(403, 'Geen toegang', message)

    def bad_request(self, message: str = 'Het verzoek klopt niet.') -> None:
        self._error_page(400, 'Ongeldig verzoek', message)

    def server_error(self) -> None:
        self._error_page(500, 'Er ging iets mis',
                         'De server kon dit verzoek niet verwerken. Probeer het opnieuw.')
