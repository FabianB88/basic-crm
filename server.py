"""Green Office CRM — entry point.

Start with:  python server.py

The application itself lives in the ``app`` package:

    app/config.py      constants and paths
    app/db.py          connections, schema, migrations, indexes
    app/auth.py        password hashing, sessions, CSRF, roles
    app/permissions.py who may change what
    app/ui.py          shared HTML chrome
    app/importer.py    multipart + CSV/XLSX import
    app/reminders.py   follow-up reminder tasks
    app/http.py        per-request context
    app/router.py      path -> handler table
    app/views/         the page handlers

This file used to hold all of that in 7,670 lines, with a single ~2,800-line
if/elif chain for routing.
"""

from __future__ import annotations

import socketserver
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app import config, reminders
from app.db import init_db
from app.http import Ctx
from app.router import dispatch


class CRMRequestHandler(BaseHTTPRequestHandler):
    """Builds a request context and hands it to the router."""

    server_version = 'GreenOfficeCRM'
    sys_version = ''
    protocol_version = 'HTTP/1.1'

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_HEAD(self) -> None:
        self._handle()

    def _handle(self) -> None:
        ctx = None
        try:
            ctx = Ctx(self)
            dispatch(ctx)
            # Keep-alive connections must be left at a request boundary.
            ctx.drain()
        except (BrokenPipeError, ConnectionResetError):
            # Browser navigated away mid-response; nothing useful to do.
            return
        except Exception:
            # An unhandled view error used to take down the connection with a
            # bare traceback and no response at all.
            traceback.print_exc()
            try:
                if ctx is not None and not ctx.responded:
                    ctx.server_error()
                elif ctx is None:
                    self.send_error(500)
            except Exception:
                pass

    def log_message(self, fmt: str, *args) -> None:
        # Default logging writes an unresolved reverse-DNS line per request.
        sys.stderr.write('%s - %s\n' % (self.address_string(), fmt % args))

    def address_string(self) -> str:
        return self.client_address[0]


class Server(ThreadingHTTPServer):
    # Without this a restart hits "Address already in use" for about a minute.
    allow_reuse_address = True
    daemon_threads = True


def run_server() -> None:
    init_db()
    reminders.start_background_thread()
    with Server((config.HOST, config.PORT), CRMRequestHandler) as httpd:
        print(f'Green Office CRM draait op http://{config.HOST}:{config.PORT}')
        print(f'Database: {config.DB_PATH}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nServer wordt afgesloten...')


if __name__ == '__main__':
    run_server()
