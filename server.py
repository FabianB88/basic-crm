"""A minimal CRM system implemented with Python's standard library.

This script implements a simple HTTP server that provides user
authentication, customer management and note tracking functionality. It
uses SQLite (via the built‑in sqlite3 module) for persistent storage
and manages sessions in memory. There are no external dependencies,
making it easy to run in constrained environments without internet
access. To start the server run this module with Python; the
application will listen on port 8000 by default.

The user interface is basic but functional, leveraging Bootstrap from
a CDN for styling. You can adjust the HTML templates within the code
to suit your needs.

Note: Because sessions are stored in memory, restarting the server
will log out all users. For a more robust solution consider
implementing session persistence (e.g. storing session IDs in the
database).
"""

from __future__ import annotations

import http.server
import socketserver
import urllib.parse
import sqlite3
import secrets
import html
import os
from typing import Tuple, Dict, Any, Optional, List


# Configuration constants
import os

HOST = '0.0.0.0'
# Use the PORT environment variable if provided (e.g. by hosting platforms like Render).
PORT = int(os.environ.get('PORT', '8000'))
DB_PATH = os.path.join(os.path.dirname(__file__), 'crm.db')

# In‑memory session store: maps session_id -> user_id
sessions: Dict[str, int] = {}


def init_db() -> None:
    """Initialize the SQLite database if it doesn't already exist."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        # Enable foreign key support
        cur.execute('PRAGMA foreign_keys = ON;')
        # Create users table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        # Create customers table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                phone TEXT,
                address TEXT,
                company TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        # Create notes table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                customer_id INTEGER NOT NULL,
                user_id INTEGER,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
            );
        ''')
        conn.commit()


def get_user_by_username_or_email(identifier: str) -> Optional[Dict[str, Any]]:
    """Retrieve a user record by username or email."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            'SELECT * FROM users WHERE username = ? OR email = ?',
            (identifier, identifier)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def create_user(username: str, email: str, password: str) -> Tuple[bool, str]:
    """Attempt to create a new user. Returns (success, message)."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        # Check uniqueness
        cur.execute('SELECT id FROM users WHERE username = ? OR email = ?', (username, email))
        if cur.fetchone():
            return False, 'Gebruikersnaam of e‑mail bestaat al.'
        cur.execute(
            'INSERT INTO users (username, email, password) VALUES (?, ?, ?)',
            (username, email, password)
        )
        conn.commit()
        return True, 'Account aangemaakt. Je kunt nu inloggen.'


def verify_user(identifier: str, password: str) -> Optional[Dict[str, Any]]:
    """Verify user credentials. Returns user dict if valid."""
    user = get_user_by_username_or_email(identifier)
    if user and user['password'] == password:
        return user
    return None


def html_header(title: str, logged_in: bool, username: str | None = None) -> str:
    """Return the HTML header and navigation bar."""
    return f'''<!doctype html>
<html lang="nl">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet"
          integrity="sha384-wnN4ghJ6U9JbYe6bcXfkwf4TDTI4ivEoKk0PHvJ/lAW/ML7yF6PKmzpF8fOhoqf9" crossorigin="anonymous">
    <style>body {{ padding-top: 60px; }}</style>
</head>
<body>
<nav class="navbar navbar-expand-lg navbar-dark bg-dark fixed-top">
  <div class="container-fluid">
    <a class="navbar-brand" href="/">CRM</a>
    <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav"
            aria-controls="navbarNav" aria-expanded="false" aria-label="Toggle navigation">
      <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse" id="navbarNav">
      <ul class="navbar-nav me-auto mb-2 mb-lg-0">
        {f'<li class="nav-item"><a class="nav-link" href="/dashboard">Dashboard</a></li><li class="nav-item"><a class="nav-link" href="/customers">Klanten</a></li>' if logged_in else ''}
      </ul>
      <ul class="navbar-nav ms-auto mb-2 mb-lg-0">
        {f'<li class="nav-item"><span class="navbar-text me-2">Ingelogd als {html.escape(username)}</span></li><li class="nav-item"><a class="nav-link" href="/logout">Uitloggen</a></li>' if logged_in else '<li class="nav-item"><a class="nav-link" href="/login">Inloggen</a></li><li class="nav-item"><a class="nav-link" href="/register">Registreren</a></li>'}
      </ul>
    </div>
  </div>
</nav>
<div class="container">
'''


def html_footer() -> str:
    """Return the HTML footer."""
    return '''</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"
        integrity="sha384-86xU3v3VN4Wv45/Vs8+1wC8BQpLHVfDymo5FgkwiaHZFf61LNR0OsoxlkCK2Crdb" crossorigin="anonymous"></script>
</body>
</html>'''


def redirect(location: str) -> bytes:
    """Return a redirect response body."""
    return f'<meta http-equiv="refresh" content="0; url={location}" />'.encode('utf-8')


class CRMRequestHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP request handler implementing the CRM logic."""

    def do_GET(self) -> None:
        self.handle_request()

    def do_POST(self) -> None:
        self.handle_request()

    def parse_session(self) -> Tuple[bool, Optional[int], Optional[str]]:
        """Parse the session cookie and return (logged_in, user_id, username)."""
        cookie = self.headers.get('Cookie')
        if cookie:
            for part in cookie.split(';'):
                if '=' in part:
                    name, value = part.strip().split('=', 1)
                    if name == 'session_id' and value in sessions:
                        user_id = sessions[value]
                        user = get_user_by_id(user_id)
                        if user:
                            return True, user['id'], user['username']
        return False, None, None

    def handle_request(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query_params = urllib.parse.parse_qs(parsed.query)
        logged_in, user_id, username = self.parse_session()
        method = self.command

        # Route dispatch
        if path == '/':
            if logged_in:
                self.respond_redirect('/dashboard')
            else:
                self.respond_redirect('/login')
        elif path == '/register':
            if logged_in:
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                username_f = params.get('username', [''])[0].strip()
                email_f = params.get('email', [''])[0].strip()
                password_f = params.get('password', [''])[0].strip()
                if not username_f or not email_f or not password_f:
                    self.render_register(error='Alle velden zijn verplicht.')
                    return
                success, msg = create_user(username_f, email_f, password_f)
                if success:
                    self.render_login(info=msg)
                else:
                    self.render_register(error=msg)
            else:
                self.render_register()
        elif path == '/login':
            if logged_in:
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                identifier = params.get('username', [''])[0].strip()
                password_f = params.get('password', [''])[0]
                user = verify_user(identifier, password_f)
                if user:
                    # Create session
                    session_id = secrets.token_hex(16)
                    sessions[session_id] = user['id']
                    # Set cookie
                    self.send_response(302)
                    self.send_header('Location', '/dashboard')
                    self.send_header('Set-Cookie', f'session_id={session_id}; Path=/')
                    self.end_headers()
                else:
                    self.render_login(error='Ongeldige inloggegevens.')
            else:
                self.render_login()
        elif path == '/logout':
            # Remove session cookie
            if logged_in:
                # find session id cookie
                cookie = self.headers.get('Cookie')
                if cookie:
                    for part in cookie.split(';'):
                        if '=' in part:
                            name, value = part.strip().split('=', 1)
                            if name == 'session_id' and value in sessions:
                                sessions.pop(value, None)
                                break
            self.send_response(302)
            self.send_header('Location', '/login')
            # Overwrite cookie to expire it
            self.send_header('Set-Cookie', 'session_id=; Path=/; Max-Age=0')
            self.end_headers()
        elif path == '/dashboard':
            if not logged_in:
                self.respond_redirect('/login')
                return
            self.render_dashboard(user_id, username)
        elif path == '/customers':
            if not logged_in:
                self.respond_redirect('/login')
                return
            search = query_params.get('q', [''])[0].strip()
            self.render_customers(search)
        elif path == '/customers/add':
            if not logged_in:
                self.respond_redirect('/login')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                name = params.get('name', [''])[0].strip()
                email_c = params.get('email', [''])[0].strip()
                phone = params.get('phone', [''])[0].strip()
                address = params.get('address', [''])[0].strip()
                company = params.get('company', [''])[0].strip()
                if not name or not email_c:
                    self.render_customer_form(None, error='Naam en e‑mail zijn verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT id FROM customers WHERE email = ?', (email_c,))
                    if cur.fetchone():
                        self.render_customer_form(None, error='Er bestaat al een klant met dit e‑mailadres.')
                        return
                    cur.execute('''INSERT INTO customers (name, email, phone, address, company) VALUES (?, ?, ?, ?, ?)''',
                                (name, email_c, phone or None, address or None, company or None))
                    conn.commit()
                self.send_response(302)
                self.send_header('Location', '/customers')
                self.end_headers()
            else:
                self.render_customer_form(None)
        elif path == '/customers/edit':
            if not logged_in:
                self.respond_redirect('/login')
                return
            cid = query_params.get('id', [None])[0]
            if cid is None:
                self.respond_not_found()
                return
            try:
                cid_int = int(cid)
            except ValueError:
                self.respond_not_found()
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                name = params.get('name', [''])[0].strip()
                email_c = params.get('email', [''])[0].strip()
                phone = params.get('phone', [''])[0].strip()
                address = params.get('address', [''])[0].strip()
                company = params.get('company', [''])[0].strip()
                if not name or not email_c:
                    customer = self.get_customer(cid_int)
                    self.render_customer_form(customer, error='Naam en e‑mail zijn verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    # ensure email uniqueness among others
                    cur.execute('SELECT id FROM customers WHERE email = ? AND id != ?', (email_c, cid_int))
                    if cur.fetchone():
                        customer = self.get_customer(cid_int)
                        self.render_customer_form(customer, error='Er bestaat al een andere klant met dit e‑mailadres.')
                        return
                    cur.execute('''UPDATE customers SET name=?, email=?, phone=?, address=?, company=?, updated_at=CURRENT_TIMESTAMP WHERE id = ?''',
                                (name, email_c, phone or None, address or None, company or None, cid_int))
                    conn.commit()
                self.send_response(302)
                self.send_header('Location', '/customers')
                self.end_headers()
            else:
                customer = self.get_customer(cid_int)
                if customer:
                    self.render_customer_form(customer)
                else:
                    self.respond_not_found()
        elif path == '/customers/delete':
            if not logged_in:
                self.respond_redirect('/login')
                return
            cid = query_params.get('id', [None])[0]
            if cid is None:
                self.respond_not_found()
                return
            try:
                cid_int = int(cid)
            except ValueError:
                self.respond_not_found()
                return
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                # Delete notes first (ON DELETE CASCADE would handle this too)
                cur.execute('DELETE FROM notes WHERE customer_id = ?', (cid_int,))
                cur.execute('DELETE FROM customers WHERE id = ?', (cid_int,))
                conn.commit()
            self.send_response(302)
            self.send_header('Location', '/customers')
            self.end_headers()
        elif path == '/customers/view':
            if not logged_in:
                self.respond_redirect('/login')
                return
            cid = query_params.get('id', [None])[0]
            if cid is None:
                self.respond_not_found()
                return
            try:
                cid_int = int(cid)
            except ValueError:
                self.respond_not_found()
                return
            if method == 'POST':
                # Add note
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                content = params.get('content', [''])[0].strip()
                if content:
                    with sqlite3.connect(DB_PATH) as conn:
                        cur = conn.cursor()
                        cur.execute('''INSERT INTO notes (content, customer_id, user_id) VALUES (?, ?, ?)''',
                                    (content, cid_int, user_id))
                        conn.commit()
                self.send_response(302)
                self.send_header('Location', f'/customers/view?id={cid_int}')
                self.end_headers()
            else:
                customer = self.get_customer(cid_int)
                if not customer:
                    self.respond_not_found()
                    return
                self.render_customer_detail(customer, user_id, username)
        elif path == '/notes/delete':
            if not logged_in:
                self.respond_redirect('/login')
                return
            nid = query_params.get('id', [None])[0]
            cid = query_params.get('customer_id', [None])[0]
            if not nid or not cid:
                self.respond_not_found()
                return
            try:
                nid_int = int(nid)
                cid_int = int(cid)
            except ValueError:
                self.respond_not_found()
                return
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute('DELETE FROM notes WHERE id = ?', (nid_int,))
                conn.commit()
            self.send_response(302)
            self.send_header('Location', f'/customers/view?id={cid_int}')
            self.end_headers()
        else:
            self.respond_not_found()

    # ----------------------------------------------------------------------
    # Helper methods for rendering pages
    # ----------------------------------------------------------------------
    def respond_redirect(self, location: str) -> None:
        self.send_response(302)
        self.send_header('Location', location)
        self.end_headers()

    def respond_not_found(self) -> None:
        self.send_response(404)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(b'404 Niet gevonden')

    def render_register(self, error: str | None = None) -> None:
        body = html_header('Registreren', False)
        body += '<h2 class="mt-4">Registreren</h2>'
        if error:
            body += f'<div class="alert alert-danger mt-2">{html.escape(error)}</div>'
        body += '''
        <form method="post" class="mt-3">
            <div class="mb-3">
                <label for="username" class="form-label">Gebruikersnaam</label>
                <input type="text" class="form-control" id="username" name="username" required>
            </div>
            <div class="mb-3">
                <label for="email" class="form-label">E‑mail</label>
                <input type="email" class="form-control" id="email" name="email" required>
            </div>
            <div class="mb-3">
                <label for="password" class="form-label">Wachtwoord</label>
                <input type="password" class="form-control" id="password" name="password" required>
            </div>
            <button type="submit" class="btn btn-primary">Registreren</button>
            <p class="mt-3">Al een account? <a href="/login">Inloggen</a></p>
        </form>
        '''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_login(self, error: str | None = None, info: str | None = None) -> None:
        body = html_header('Inloggen', False)
        body += '<h2 class="mt-4">Inloggen</h2>'
        if error:
            body += f'<div class="alert alert-danger mt-2">{html.escape(error)}</div>'
        if info:
            body += f'<div class="alert alert-success mt-2">{html.escape(info)}</div>'
        body += '''
        <form method="post" class="mt-3">
            <div class="mb-3">
                <label for="username" class="form-label">Gebruikersnaam of e‑mail</label>
                <input type="text" class="form-control" id="username" name="username" required>
            </div>
            <div class="mb-3">
                <label for="password" class="form-label">Wachtwoord</label>
                <input type="password" class="form-control" id="password" name="password" required>
            </div>
            <button type="submit" class="btn btn-primary">Inloggen</button>
            <p class="mt-3">Geen account? <a href="/register">Registreren</a></p>
        </form>
        '''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_dashboard(self, user_id: int, username: str) -> None:
        # Count customers and get recent notes
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM customers')
            total_customers = cur.fetchone()[0]
            cur.execute('''
                SELECT notes.id AS note_id, notes.content, notes.created_at, customers.name AS customer_name
                FROM notes
                JOIN customers ON notes.customer_id = customers.id
                ORDER BY notes.created_at DESC
                LIMIT 5
            ''')
            notes = cur.fetchall()
        body = html_header('Dashboard', True, username)
        body += '<h2 class="mt-4">Dashboard</h2>'
        body += f'''
        <div class="row mt-3">
            <div class="col-md-4">
                <div class="card text-white bg-primary mb-3">
                    <div class="card-body">
                        <h5 class="card-title">Totaal aantal klanten</h5>
                        <p class="card-text display-5">{total_customers}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-8">
                <div class="card mb-3">
                    <div class="card-header">Recente notities</div>
                    <ul class="list-group list-group-flush">'''
        if notes:
            for note in notes:
                snippet = (note['content'][:100] + '…') if len(note['content']) > 100 else note['content']
                created_at = note['created_at']
                body += f'''<li class="list-group-item"><strong>{html.escape(note['customer_name'])}</strong><br>{html.escape(snippet)}<small class="text-muted d-block">{created_at}</small></li>'''
        else:
            body += '<li class="list-group-item">Er zijn nog geen notities.</li>'
        body += '''</ul></div></div></div>'''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_customers(self, search: str) -> None:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            if search:
                like = f'%{search}%'
                cur.execute('''SELECT * FROM customers WHERE name LIKE ? OR email LIKE ? OR company LIKE ? ORDER BY name ASC''',
                            (like, like, like))
            else:
                cur.execute('SELECT * FROM customers ORDER BY name ASC')
            customers = cur.fetchall()
        logged_in, _, username = self.parse_session()
        body = html_header('Klanten', logged_in, username)
        body += '<h2 class="mt-4">Klanten</h2>'
        body += f'''
        <div class="row mt-3">
            <div class="col-md-6">
                <form method="get" class="d-flex" role="search">
                    <input class="form-control me-2" type="search" name="q" placeholder="Zoeken" value="{html.escape(search)}">
                    <button class="btn btn-outline-success" type="submit">Zoek</button>
                </form>
            </div>
            <div class="col-md-6 text-end">
                <a href="/customers/add" class="btn btn-primary">Klant toevoegen</a>
            </div>
        </div>
        <table class="table table-striped table-hover mt-3">
            <thead><tr><th>Naam</th><th>Bedrijf</th><th>E‑mail</th><th>Telefoon</th><th class="text-end">Acties</th></tr></thead>
            <tbody>
        '''
        if customers:
            for cust in customers:
                body += f'''<tr>
                    <td><a href="/customers/view?id={cust['id']}">{html.escape(cust['name'])}</a></td>
                    <td>{html.escape(cust['company'] or '-')}</td>
                    <td>{html.escape(cust['email'])}</td>
                    <td>{html.escape(cust['phone'] or '-')}</td>
                    <td class="text-end">
                        <a href="/customers/edit?id={cust['id']}" class="btn btn-sm btn-secondary">Bewerk</a>
                        <a href="/customers/delete?id={cust['id']}" class="btn btn-sm btn-danger" onclick="return confirm('Weet je zeker dat je deze klant wilt verwijderen?');">Verwijder</a>
                    </td>
                </tr>'''
        else:
            body += '<tr><td colspan="5" class="text-center">Geen klanten gevonden.</td></tr>'
        body += '</tbody></table>'
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def get_customer(self, customer_id: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM customers WHERE id = ?', (customer_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def render_customer_form(self, customer: Optional[Dict[str, Any]], error: str | None = None) -> None:
        logged_in, _, username = self.parse_session()
        page_title = 'Klant bewerken' if customer else 'Klant toevoegen'
        body = html_header(page_title, logged_in, username)
        body += f'<h2 class="mt-4">{page_title}</h2>'
        if error:
            body += f'<div class="alert alert-danger mt-2">{html.escape(error)}</div>'
        name = customer['name'] if customer else ''
        email = customer['email'] if customer else ''
        phone = customer['phone'] if customer else ''
        address = customer['address'] if customer else ''
        company = customer['company'] if customer else ''
        action = '/customers/edit?id={}'.format(customer['id']) if customer else '/customers/add'
        body += f'''
        <form method="post" class="mt-3">
            <div class="row">
                <div class="col-md-6">
                    <div class="mb-3">
                        <label for="name" class="form-label">Naam</label>
                        <input type="text" class="form-control" id="name" name="name" value="{html.escape(name)}" required>
                    </div>
                    <div class="mb-3">
                        <label for="email" class="form-label">E‑mail</label>
                        <input type="email" class="form-control" id="email" name="email" value="{html.escape(email)}" required>
                    </div>
                    <div class="mb-3">
                        <label for="phone" class="form-label">Telefoon</label>
                        <input type="text" class="form-control" id="phone" name="phone" value="{html.escape(phone or '')}">
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="mb-3">
                        <label for="address" class="form-label">Adres</label>
                        <input type="text" class="form-control" id="address" name="address" value="{html.escape(address or '')}">
                    </div>
                    <div class="mb-3">
                        <label for="company" class="form-label">Bedrijf</label>
                        <input type="text" class="form-control" id="company" name="company" value="{html.escape(company or '')}">
                    </div>
                </div>
            </div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/customers" class="btn btn-link">Annuleren</a>
        </form>
        '''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_customer_detail(self, customer: Dict[str, Any], user_id: int, username: str) -> None:
        # Fetch notes
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''
                SELECT notes.id AS note_id, notes.content, notes.created_at, users.username AS author
                FROM notes
                LEFT JOIN users ON notes.user_id = users.id
                WHERE notes.customer_id = ?
                ORDER BY notes.created_at DESC
            ''', (customer['id'],))
            notes = cur.fetchall()
        logged_in, _, _ = self.parse_session()
        body = html_header(f'Klant: {customer["name"]}', logged_in, username)
        body += f'''
        <div class="d-flex justify-content-between align-items-center mt-4">
            <h2>{html.escape(customer['name'])}</h2>
            <div>
                <a href="/customers/edit?id={customer['id']}" class="btn btn-secondary">Bewerk</a>
                <a href="/customers/delete?id={customer['id']}" class="btn btn-danger" onclick="return confirm('Weet je zeker dat je deze klant wilt verwijderen?');">Verwijder</a>
            </div>
        </div>
        <div class="mt-3">
            <h5>Contactgegevens</h5>
            <p><strong>Naam:</strong> {html.escape(customer['name'])}</p>
            <p><strong>E‑mail:</strong> {html.escape(customer['email'])}</p>
            {f'<p><strong>Telefoon:</strong> {html.escape(customer["phone"])} </p>' if customer['phone'] else ''}
            {f'<p><strong>Adres:</strong> {html.escape(customer["address"])} </p>' if customer['address'] else ''}
            {f'<p><strong>Bedrijf:</strong> {html.escape(customer["company"])} </p>' if customer['company'] else ''}
            <p><strong>Aangemaakt op:</strong> {customer['created_at']}</p>
        </div>
        <hr>
        <div class="mt-4">
            <h5>Notities</h5>
            <form method="post" class="mb-3">
                <div class="mb-3">
                    <label for="content" class="form-label">Nieuwe notitie</label>
                    <textarea class="form-control" id="content" name="content" rows="3" placeholder="Schrijf hier een notitie..." required></textarea>
                </div>
                <button type="submit" class="btn btn-primary">Opslaan</button>
            </form>
            <ul class="list-group">
        '''
        if notes:
            for note in notes:
                body += f'''<li class="list-group-item">
                    {html.escape(note['content'])}
                    <small class="text-muted d-block">{note['created_at']} {f'door {html.escape(note["author"])}' if note['author'] else ''}</small>
                    <a href="/notes/delete?id={note['note_id']}&customer_id={customer['id']}" class="btn btn-sm btn-link text-danger float-end" onclick="return confirm('Weet je zeker dat je deze notitie wilt verwijderen?');">Verwijder</a>
                </li>'''
        else:
            body += '<li class="list-group-item">Er zijn nog geen notities.</li>'
        body += '</ul></div>'
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))


def run_server() -> None:
    init_db()
    with socketserver.TCPServer((HOST, PORT), CRMRequestHandler) as httpd:
        print(f'Starting CRM server on http://{HOST}:{PORT}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        print('Server shutting down...')


if __name__ == '__main__':
    run_server()