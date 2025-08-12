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
import hashlib
from typing import Tuple, Dict, Any, Optional, List


# Configuration constants
import os

HOST = '0.0.0.0'
# Use the PORT environment variable if provided (e.g. by hosting platforms like Render).
PORT = int(os.environ.get('PORT', '8000'))
DB_PATH = os.path.join(os.path.dirname(__file__), 'crm.db')

# In‑memory session store: maps session_id -> user_id
sessions: Dict[str, int] = {}

# ---------------------------------------------------------------------------
# User/administration helpers
# ---------------------------------------------------------------------------
def users_exist() -> bool:
    """Return True if at least one user record exists in the database."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM users')
        return cur.fetchone()[0] > 0


def is_admin(user_id: int) -> bool:
    """Simple admin check: treat the very first user (id=1) as the admin.

    This function can be extended to support a proper role system (e.g.,
    storing a role column in the users table).  For now we assume the
    account created first is the administrator and is allowed to add new
    users.
    """
    return user_id == 1


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
        # Create customers table.  Includes optional tags column to allow
        # categorisation of customers (comma separated values).  If the
        # table already exists but the tags column is missing, add it.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                phone TEXT,
                address TEXT,
                company TEXT,
                tags TEXT,
                -- Category indicates whether this record is a true client ('klant')
                -- or part of the broader network ('netwerk').  Default is 'klant'.
                category TEXT DEFAULT 'klant',
                -- created_by stores the user ID of the account that added this customer.
                created_by INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
            );
        ''')
        # If an existing customers table lacks the tags column (e.g. from
        # earlier versions), add it dynamically.
        try:
            cur.execute('SELECT tags FROM customers LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE customers ADD COLUMN tags TEXT')

        # Ensure the category column exists.  Older database versions may not
        # include this column; add it with default 'klant' if missing.
        try:
            cur.execute('SELECT category FROM customers LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE customers ADD COLUMN category TEXT DEFAULT 'klant'")

        # Ensure the created_by column exists.  It stores the user who added the
        # customer.  If absent, add it as INTEGER.
        try:
            cur.execute('SELECT created_by FROM customers LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE customers ADD COLUMN created_by INTEGER')
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

        # Create tasks table for managing to‑dos per customer.  Each task
        # records a title, an optional description, an optional due date and
        # a status (open/completed).  Tasks are linked to both the customer
        # they belong to and the user who created them.  Cascade deletes
        # ensure tasks are removed when their customer or creator is deleted.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                due_date DATE,
                status TEXT NOT NULL DEFAULT 'open',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                customer_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        # Create interactions table.  This table records each interaction (e.g.
        # call, email, message) associated with a customer.  Each record has
        # a type, an optional note and a timestamp.  Interactions are
        # optional and can be added through the customer detail page.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS interactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                interaction_type TEXT NOT NULL,
                note TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                customer_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
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
        # Generate a salted hash for the password.  We store the salt and hash
        # together separated by a dollar sign so we can verify later.  This
        # avoids storing passwords in plain text and provides basic
        # protection against rainbow table attacks.
        salt = secrets.token_hex(16)
        pwd_hash = hashlib.sha256((salt + password).encode('utf-8')).hexdigest()
        stored_password = f"{salt}${pwd_hash}"
        cur.execute(
            'INSERT INTO users (username, email, password) VALUES (?, ?, ?)',
            (username, email, stored_password)
        )
        conn.commit()
        return True, 'Account aangemaakt. Je kunt nu inloggen.'


def verify_user(identifier: str, password: str) -> Optional[Dict[str, Any]]:
    """Verify user credentials. Returns user dict if valid."""
    user = get_user_by_username_or_email(identifier)
    if not user:
        return None
    # The stored password contains salt and hash separated by a dollar sign.
    stored = user['password']
    if '$' not in stored:
        return None
    salt, pwd_hash = stored.split('$', 1)
    provided_hash = hashlib.sha256((salt + password).encode('utf-8')).hexdigest()
    if provided_hash == pwd_hash:
        return user
    return None


def html_header(title: str, logged_in: bool, username: str | None = None, user_id: int | None = None) -> str:
    """Return the HTML header and navigation bar.

    The header contains a simple responsive navigation bar and embeds a small
    stylesheet to provide a modern look without relying on external CSS
    frameworks.  The color palette is inspired by the provided mobile app
    screenshot (pink/magenta accents).
    """
    # Inline CSS: provides a light background, coloured header bar and basic
    # card styles.  Avoid external dependencies by including everything in
    # the page.  Feel free to customise these values to better match your
    # preferred palette.
    styles = '''
    body { margin: 0; font-family: Arial, sans-serif; background-color: #f8f9fa; padding-top: 56px; }
    .navbar { background-color: #c2185b; color: #fff; position: fixed; top: 0; width: 100%; height: 56px; display: flex; align-items: center; padding: 0 1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1); z-index: 1000; }
    .navbar a { color: #fff; text-decoration: none; margin-right: 1rem; }
    .navbar .spacer { flex-grow: 1; }
    .container { max-width: 960px; margin: 0 auto; padding: 1rem; }
    .card { background-color: #ffffff; border-radius: 8px; padding: 1rem; margin-bottom: 1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .section-title { font-size: 1.2rem; font-weight: bold; margin-bottom: 0.5rem; }
    .action-buttons a { display: inline-block; border: 2px solid #c2185b; border-radius: 24px; padding: 0.3rem 0.8rem; color: #c2185b; text-decoration: none; margin-right: 0.5rem; font-size: 0.9rem; }
    .action-buttons a:hover { background-color: #c2185b; color: #fff; }
    .icon { margin-right: 0.5rem; }
    '''
    # Determine navigation links based on login state.  We omit the
    # registration link unless there are no users yet; see users_exist() below.
    if logged_in:
        # Left side: dashboard, customers.  If user is admin, include users link.
        nav_links = ["<a href='/dashboard'>Dashboard</a>", "<a href='/customers'>Klanten</a>"]
        try:
            uid_int = int(user_id) if user_id is not None else None
        except Exception:
            uid_int = None
        if uid_int is not None and is_admin(uid_int):
            nav_links.append("<a href='/users'>Gebruikers</a>")
        nav_links_left = ''.join(nav_links)
        nav_links_right = f"<span>Ingelogd als {html.escape(username)}</span> <a href='/logout'>Uitloggen</a>"
    else:
        nav_links_left = ''
        nav_links_right = "<a href='/login'>Inloggen</a>"
    return f'''<!doctype html>
<html lang="nl">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)}</title>
    <style>{styles}</style>
</head>
<body>
<nav class="navbar">
    <a href="/">CRM</a>
    <div class="spacer"></div>
    {nav_links_left}
    <div class="spacer"></div>
    {nav_links_right}
</nav>
<div class="container">
'''


def html_footer() -> str:
    """Return the HTML footer.  Simply closes the container and page.

    External scripts are omitted because all styling is embedded inline.
    """
    return '</div></body></html>'


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
            # Registration is restricted.  If there is already at least one
            # user in the database, only an authenticated admin can create
            # additional accounts.  Otherwise (no users yet), allow the
            # first account to be created publicly.
            if users_exist():
                if not logged_in or not is_admin(user_id):
                    # Deny access to non‑admin users
                    self.respond_redirect('/login')
                    return
            # At this point, either there are no users yet (initial setup)
            # or the requester is an admin.  Proceed with registration.
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
                    # After admin creates a user, redirect back to dashboard
                    if logged_in:
                        self.respond_redirect('/dashboard')
                    else:
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
                    # Set the session cookie.  Include HttpOnly and SameSite
                    # attributes to mitigate cross‑site scripting and
                    # request forgery attacks.  We don't include the
                    # ``Secure`` attribute because the app may run on
                    # plain HTTP in development environments.  Hosting
                    # providers like Render serve over HTTPS and will
                    # automatically upgrade the cookie to secure.
                    self.send_response(302)
                    self.send_header('Location', '/dashboard')
                    self.send_header(
                        'Set-Cookie',
                        f'session_id={session_id}; Path=/; HttpOnly; SameSite=Lax'
                    )
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
            # Overwrite cookie to expire it.  Preserve SameSite and
            # HttpOnly attributes for consistency.  ``Max‑Age=0`` causes
            # browsers to remove the cookie immediately.
            self.send_header(
                'Set-Cookie',
                'session_id=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax'
            )
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
                tags = params.get('tags', [''])[0].strip()
                category = params.get('category', ['klant'])[0].strip() or 'klant'
                if not name or not email_c:
                    self.render_customer_form(None, error='Naam en e‑mail zijn verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT id FROM customers WHERE email = ?', (email_c,))
                    if cur.fetchone():
                        self.render_customer_form(None, error='Er bestaat al een klant met dit e‑mailadres.')
                        return
                    cur.execute('''INSERT INTO customers (name, email, phone, address, company, tags, category, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                (name,
                                 email_c,
                                 phone or None,
                                 address or None,
                                 company or None,
                                 tags or None,
                                 category,
                                 user_id))
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
                tags = params.get('tags', [''])[0].strip()
                category = params.get('category', ['klant'])[0].strip() or 'klant'
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
                    cur.execute('''UPDATE customers
                                    SET name=?,
                                        email=?,
                                        phone=?,
                                        address=?,
                                        company=?,
                                        tags=?,
                                        category=?,
                                        updated_at=CURRENT_TIMESTAMP
                                    WHERE id = ?''',
                                (name,
                                 email_c,
                                 phone or None,
                                 address or None,
                                 company or None,
                                 tags or None,
                                 category,
                                 cid_int))
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
        # Task management routes
        elif path == '/tasks/add':
            # Add a new task for a customer.  Requires logged in user.  Expects
            # POST data with title, optional description and due_date and query
            # parameter customer_id specifying the customer.  After adding the
            # task, redirect back to the customer detail page.
            if not logged_in:
                self.respond_redirect('/login')
                return
            cid = query_params.get('customer_id', [None])[0]
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
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                due_date = params.get('due_date', [''])[0].strip()
                if not title:
                    # Re-render customer page with error message
                    customer = self.get_customer(cid_int)
                    self.render_customer_detail(customer, user_id, username,
                                                task_error='Titel is verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute('''INSERT INTO tasks (title, description, due_date, customer_id, user_id) VALUES (?, ?, ?, ?, ?)''',
                                (title, description or None, due_date or None, cid_int, user_id))
                    conn.commit()
                self.send_response(302)
                self.send_header('Location', f'/customers/view?id={cid_int}')
                self.end_headers()
            else:
                self.respond_not_found()
        elif path == '/tasks/complete':
            # Mark a task as completed.
            if not logged_in:
                self.respond_redirect('/login')
                return
            tid = query_params.get('id', [None])[0]
            cid = query_params.get('customer_id', [None])[0]
            if not tid or not cid:
                self.respond_not_found()
                return
            try:
                tid_int = int(tid)
                cid_int = int(cid)
            except ValueError:
                self.respond_not_found()
                return
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute('UPDATE tasks SET status = ? WHERE id = ?', ('completed', tid_int))
                conn.commit()
            self.send_response(302)
            self.send_header('Location', f'/customers/view?id={cid_int}')
            self.end_headers()
        elif path == '/tasks/delete':
            # Delete a task
            if not logged_in:
                self.respond_redirect('/login')
                return
            tid = query_params.get('id', [None])[0]
            cid = query_params.get('customer_id', [None])[0]
            if not tid or not cid:
                self.respond_not_found()
                return
            try:
                tid_int = int(tid)
                cid_int = int(cid)
            except ValueError:
                self.respond_not_found()
                return
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute('DELETE FROM tasks WHERE id = ?', (tid_int,))
                conn.commit()
            self.send_response(302)
            self.send_header('Location', f'/customers/view?id={cid_int}')
            self.end_headers()
        elif path == '/interactions/add':
            # Add a new interaction (call, email, message) for a customer.  Requires
            # logged in user.  Expects POST data with interaction_type and note
            # and query parameter customer_id specifying the customer.
            if not logged_in:
                self.respond_redirect('/login')
                return
            cid = query_params.get('customer_id', [None])[0]
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
                interaction_type = params.get('interaction_type', [''])[0].strip()
                note = params.get('note', [''])[0].strip()
                if not interaction_type:
                    customer = self.get_customer(cid_int)
                    # For now we reuse render_customer_detail with an error message
                    self.render_customer_detail(customer, user_id, username, task_error='Interactietype is verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute('''INSERT INTO interactions (interaction_type, note, customer_id, user_id) VALUES (?, ?, ?, ?)''',
                                (interaction_type, note or None, cid_int, user_id))
                    conn.commit()
                self.send_response(302)
                self.send_header('Location', f'/customers/view?id={cid_int}')
                self.end_headers()
        elif path == '/users':
            # List all users.  Only admin can view this page.  Non-admins are redirected.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            # Fetch users
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('SELECT id, username, email, created_at FROM users ORDER BY id ASC')
                users = cur.fetchall()
            self.render_user_list(users, username)
        elif path == '/users/add':
            # Admin can add a new user via this route.  GET displays form, POST processes.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                new_username = params.get('username', [''])[0].strip()
                new_email = params.get('email', [''])[0].strip()
                new_password = params.get('password', [''])[0]
                if not new_username or not new_email or not new_password:
                    self.render_user_form(error='Alle velden zijn verplicht.', logged_in=True, username=username)
                    return
                success, msg = create_user(new_username, new_email, new_password)
                if success:
                    self.respond_redirect('/users')
                else:
                    self.render_user_form(error=msg, logged_in=True, username=username)
            else:
                self.render_user_form(logged_in=True, username=username)
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
        </form>
        '''
        # Only show a registration hint if there are no users yet.  This
        # allows the first account to be created while preventing
        # self‑service registration once the system is initialised.
        if not users_exist():
            body += '<p class="mt-3">Geen account? <a href="/register">Registreren</a></p>'
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
        body = html_header('Dashboard', True, username, user_id)
        body += '<h2 class="mt-4">Dashboard</h2>'
        # Summary card
        body += f'''<div class="card">
            <div class="section-title">Overzicht</div>
            <p>Totaal aantal klanten: <strong>{total_customers}</strong></p>
        </div>'''
        # Recent notes
        notes_section = ''
        if notes:
            for note in notes:
                snippet = (note['content'][:100] + '…') if len(note['content']) > 100 else note['content']
                notes_section += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0;">
                    <strong>{html.escape(note['customer_name'])}</strong><br>
                    {html.escape(snippet)}
                    <div style="font-size:0.8rem; color:#666;">{note['created_at']}</div>
                </div>'''
        else:
            notes_section = '<p>Er zijn nog geen notities.</p>'
        body += f'''<div class="card">
            <div class="section-title">Recente notities</div>
            {notes_section}
        </div>'''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    # Admin user management views
    def render_user_list(self, users: List[sqlite3.Row], username: str) -> None:
        """Render a list of users for admin."""
        # The admin user id is always 1.  Pass it so that the nav can include the Gebruikers link.
        body = html_header('Gebruikersbeheer', True, username, 1)
        body += '<h2 class="mt-4">Gebruikers</h2>'
        body += '<div class="card">'
        body += '<div class="section-title">Huidige gebruikers</div>'
        if users:
            for user in users:
                body += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0;">
                    <strong>{html.escape(user['username'])}</strong> ({html.escape(user['email'])})
                    <div style="font-size:0.8rem; color:#666;">Aangemaakt op {user['created_at']}</div>
                </div>'''
        else:
            body += '<p>Er zijn nog geen gebruikers.</p>'
        body += '</div>'
        body += f'''<div class="card">
            <div class="section-title">Nieuwe gebruiker toevoegen</div>
            <form method="post" action="/users/add">
                <label>Gebruikersnaam<br><input type="text" name="username" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
                <label>E‑mail<br><input type="email" name="email" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
                <label>Wachtwoord<br><input type="password" name="password" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
                <button type="submit" style="background-color:#c2185b; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Gebruiker toevoegen</button>
            </form>
        </div>'''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_user_form(self, error: str | None = None, logged_in: bool = False, username: str | None = None) -> None:
        """Render a standalone user creation form with error message for admin."""
        body = html_header('Nieuwe gebruiker', logged_in, username, 1 if logged_in else None)
        body += '<h2 class="mt-4">Nieuwe gebruiker</h2>'
        if error:
            body += f'<div class="alert alert-danger mt-2">{html.escape(error)}</div>'
        body += '''<form method="post" class="mt-3">
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
            <button type="submit" class="btn btn-primary">Gebruiker toevoegen</button>
            <a href="/users" class="btn btn-link">Annuleren</a>
        </form>'''
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
                # Build a LIKE pattern for filtering by name, email, company or tags
                like = f'%{search}%'
                cur.execute('''SELECT * FROM customers
                               WHERE name LIKE ? OR email LIKE ? OR company LIKE ? OR tags LIKE ?
                               ORDER BY name ASC''',
                            (like, like, like, like))
            else:
                cur.execute('SELECT * FROM customers ORDER BY name ASC')
            customers = cur.fetchall()
        logged_in, _, username = self.parse_session()
        # Determine user_id for nav; parse from session
        _, uid, _ = self.parse_session()
        body = html_header('Klanten', logged_in, username, uid)
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
                <a href="/customers/add" class="btn btn-primary">Klant/Netwerk toevoegen</a>
            </div>
        </div>
        <table class="table table-striped table-hover mt-3">
            <thead>
                <tr>
                    <th>Naam</th>
                    <th>Bedrijf</th>
                    <th>Type</th>
                    <th>Tags</th>
                    <th>E‑mail</th>
                    <th>Telefoon</th>
                    <th>Toegevoegd door</th>
                    <th class="text-end">Acties</th>
                </tr>
            </thead>
            <tbody>
        '''
        if customers:
            for cust in customers:
                # Format tags as comma separated list or dash if none
                tags_display = ', '.join([html.escape(tag.strip()) for tag in (cust['tags'] or '').split(',')]) if cust['tags'] else '-'
                # Determine category (klant/netwerk) and capitalise first letter
                category_display = (cust['category'] or 'klant').capitalize() if 'category' in cust.keys() else 'Klant'
                # Determine creator username
                creator = '-'  # default if no user
                try:
                    if cust['created_by']:
                        u = get_user_by_id(cust['created_by'])
                        if u:
                            creator = html.escape(u['username'])
                except Exception:
                    creator = '-'
                body += f'''<tr>
                    <td><a href="/customers/view?id={cust['id']}">{html.escape(cust['name'])}</a></td>
                    <td>{html.escape(cust['company'] or '-')}</td>
                    <td>{category_display}</td>
                    <td>{tags_display}</td>
                    <td>{html.escape(cust['email'])}</td>
                    <td>{html.escape(cust['phone'] or '-')}</td>
                    <td>{creator}</td>
                    <td class="text-end">
                        <a href="/customers/edit?id={cust['id']}" class="btn btn-sm btn-secondary">Bewerk</a>
                        <a href="/customers/delete?id={cust['id']}" class="btn btn-sm btn-danger" onclick="return confirm('Weet je zeker dat je deze klant wilt verwijderen?');">Verwijder</a>
                    </td>
                </tr>'''
        else:
            # Adjust colspan to match the number of columns in the table
            body += '<tr><td colspan="8" class="text-center">Geen klanten gevonden.</td></tr>'
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
        # Determine user id for navigation
        logged, uid, _ = self.parse_session()
        body = html_header(page_title, logged_in, username, uid)
        body += f'<h2 class="mt-4">{page_title}</h2>'
        if error:
            body += f'<div class="alert alert-danger mt-2">{html.escape(error)}</div>'
        name = customer['name'] if customer else ''
        email = customer['email'] if customer else ''
        phone = customer['phone'] if customer else ''
        address = customer['address'] if customer else ''
        company = customer['company'] if customer else ''
        tags = customer['tags'] if customer else ''
        category = customer['category'] if customer else 'klant'
        action = '/customers/edit?id={}'.format(customer['id']) if customer else '/customers/add'
        # Wrap the form in a card for better visual separation.  The card uses
        # Bootstrap classes but will also look clean when the custom inline
        # stylesheet (in html_header) is used.  Fields are divided into two
        # columns to mirror the mobile design from the provided screenshot.
        body += f'''
        <div class="card shadow-sm mt-4">
            <div class="card-body">
                <form method="post">
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
                            <div class="mb-3">
                                <label for="tags" class="form-label">Tags (gescheiden door komma)</label>
                                <input type="text" class="form-control" id="tags" name="tags" value="{html.escape(tags or '')}">
                            </div>
                            <div class="mb-3">
                                <label for="category" class="form-label">Type</label>
                                <select class="form-select" id="category" name="category">
                                    <option value="klant" {'selected' if category == 'klant' else ''}>Klant</option>
                                    <option value="netwerk" {'selected' if category == 'netwerk' else ''}>Netwerk</option>
                                </select>
                            </div>
                        </div>
                    </div>
                    <div class="mt-3 d-flex justify-content-between">
                        <div>
                            <button type="submit" class="btn btn-primary">Opslaan</button>
                            <a href="/customers" class="btn btn-link">Annuleren</a>
                        </div>
                    </div>
                </form>
            </div>
        </div>
        '''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_customer_detail(self, customer: Dict[str, Any], user_id: int, username: str, task_error: str | None = None) -> None:
        """Render the detail view for a single customer, including notes and tasks."""
        # Fetch notes and tasks for this customer
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # Notes
            cur.execute('''
                SELECT notes.id AS note_id, notes.content, notes.created_at, users.username AS author
                FROM notes
                LEFT JOIN users ON notes.user_id = users.id
                WHERE notes.customer_id = ?
                ORDER BY notes.created_at DESC
            ''', (customer['id'],))
            notes = cur.fetchall()
            # Tasks
            cur.execute('''
                SELECT tasks.id AS task_id, tasks.title, tasks.description, tasks.due_date, tasks.status, tasks.created_at, users.username AS author
                FROM tasks
                JOIN users ON tasks.user_id = users.id
                WHERE tasks.customer_id = ?
                ORDER BY CASE tasks.status WHEN 'open' THEN 0 ELSE 1 END, COALESCE(tasks.due_date, '') ASC, tasks.created_at ASC
            ''', (customer['id'],))
            tasks = cur.fetchall()
            # Interactions
            cur.execute('''
                SELECT interactions.id AS interaction_id, interactions.interaction_type, interactions.note, interactions.created_at, users.username AS author
                FROM interactions
                JOIN users ON interactions.user_id = users.id
                WHERE interactions.customer_id = ?
                ORDER BY interactions.created_at DESC
            ''', (customer['id'],))
            interactions = cur.fetchall()
        logged_in, _, _ = self.parse_session()
        body = html_header(f'Klant: {customer["name"]}', logged_in, username, user_id)
        # ----- Profile card -----
        actions_html = []
        # Provide call and email links only if data is present
        if customer['phone']:
            actions_html.append(f"<a href='tel:{html.escape(customer['phone'])}'><span class='icon'>&#128222;</span>Bel</a>")
        actions_html.append(f"<a href='mailto:{html.escape(customer['email'])}'><span class='icon'>&#9993;</span>Email</a>")
        # The message action is a placeholder; adapt as needed
        actions_html.append(f"<a href='mailto:{html.escape(customer['email'])}'><span class='icon'>&#128172;</span>Bericht</a>")
        actions_block = ' '.join(actions_html)
        body += f'''<div class="card">
            <div style="display:flex; justify-content: space-between; align-items:center;">
                <div>
                    <h2>{html.escape(customer['name'])}</h2>
                    {f'<p>{html.escape(customer["company"])}<br><small>{html.escape(customer["address"] or "")}</small></p>' if customer.get('company') or customer.get('address') else ''}
                </div>
                <div>
                    <a href="/customers/edit?id={customer['id']}" class="action-buttons" style="margin-right:0.5rem;">Bewerk</a>
                    <a href="/customers/delete?id={customer['id']}" class="action-buttons" onclick="return confirm('Weet je zeker dat je deze klant wilt verwijderen?');">Verwijder</a>
                </div>
            </div>
            <div class="action-buttons" style="margin-top:0.5rem;">{actions_block}</div>
        </div>'''
        # ----- Contact details card -----
        tags_html = ''
        if customer.get('tags'):
            tags_html = '<p><span class="icon">&#128279;</span>' + ', '.join([html.escape(tag.strip()) for tag in customer['tags'].split(',')]) + '</p>'
        # Determine creator and category for display
        creator_name = '-'
        try:
            if customer.get('created_by'):
                creator_user = get_user_by_id(customer['created_by'])
                if creator_user:
                    creator_name = html.escape(creator_user['username'])
        except Exception:
            creator_name = '-'
        category_display = (customer.get('category') or 'klant').capitalize()
        body += f'''<div class="card">
            <div class="section-title">Contactgegevens</div>
            <p><span class="icon">&#9993;</span>{html.escape(customer['email'])}</p>
            {f'<p><span class="icon">&#128222;</span>{html.escape(customer["phone"])} </p>' if customer['phone'] else ''}
            {f'<p><span class="icon">&#127968;</span>{html.escape(customer["address"])} </p>' if customer['address'] else ''}
            {f'<p><span class="icon">&#128188;</span>{html.escape(customer["company"])} </p>' if customer['company'] else ''}
            {tags_html}
            <p><span class="icon">&#128221;</span>Type: {category_display}</p>
            <p><span class="icon">&#128100;</span>Toegevoegd door: {creator_name}</p>
            <p><span class="icon">&#128197;</span>Aangemaakt op {customer['created_at']}</p>
        </div>'''
        # ----- Tasks card -----
        # Show task error if present
        tasks_section = ''
        if task_error:
            tasks_section += f'<div class="alert alert-danger">{html.escape(task_error)}</div>'
        # Task form
        tasks_section += f'''<form method="post" action="/tasks/add?customer_id={customer['id']}" style="margin-bottom:1rem;">
            <label>Titel<br><input type="text" name="title" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <label>Vervaldatum<br><input type="date" name="due_date" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <label>Beschrijving<br><input type="text" name="description" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <button type="submit" style="background-color:#c2185b; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Taak toevoegen</button>
        </form>'''
        # Task list
        if tasks:
            for task in tasks:
                status_label = 'Voltooid' if task['status'] == 'completed' else 'Open'
                status_color = '#388e3c' if task['status'] == 'completed' else '#fbc02d'
                due = task['due_date'] or '-'
                description = f"<br><small>{html.escape(task['description'])}</small>" if task['description'] else ''
                actions = []
                if task['status'] == 'open':
                    actions.append(f"<a href='/tasks/complete?id={task['task_id']}&customer_id={customer['id']}' style='color:#388e3c;'>Markeer voltooid</a>")
                actions.append(f"<a href='/tasks/delete?id={task['task_id']}&customer_id={customer['id']}' style='color:#c2185b;' onclick=\"return confirm('Weet je zeker dat je deze taak wilt verwijderen?');\">Verwijder</a>")
                action_html = ' | '.join(actions)
                tasks_section += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0;">
                    <span style="color:{status_color}; font-weight:bold;">{status_label}</span>
                    <strong style="margin-left:0.5rem;">{html.escape(task['title'])}</strong> (Vervaldatum: {html.escape(due)}){description}
                    <div style="font-size:0.8rem; color:#666;">Aangemaakt op {task['created_at']} door {html.escape(task['author'])}</div>
                    <div style="font-size:0.8rem;">{action_html}</div>
                </div>'''
        else:
            tasks_section += '<p>Er zijn nog geen taken.</p>'
        body += f'''<div class="card">
            <div class="section-title">Taken</div>
            {tasks_section}
        </div>'''
        # ----- Notes card -----
        notes_section = ''
        notes_section += f'''<form method="post" style="margin-bottom:1rem;">
            <label>Nieuwe notitie<br><textarea name="content" rows="3" required style="width:100%; padding:0.4rem;"></textarea></label><br>
            <button type="submit" style="background-color:#c2185b; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Opslaan</button>
        </form>'''
        if notes:
            for note in notes:
                author_part = f"door {html.escape(note['author'])}" if note['author'] else ''
                notes_section += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0;">
                    {html.escape(note['content'])}
                    <div style="font-size:0.8rem; color:#666;">{note['created_at']} {author_part}</div>
                    <div style="font-size:0.8rem;"><a href='/notes/delete?id={note['note_id']}&customer_id={customer['id']}' style='color:#c2185b;' onclick="return confirm('Weet je zeker dat je deze notitie wilt verwijderen?');">Verwijder</a></div>
                </div>'''
        else:
            notes_section += '<p>Er zijn nog geen notities.</p>'
        body += f'''<div class="card">
            <div class="section-title">Notities</div>
            {notes_section}
        </div>'''

        # ----- Interactions card -----
        # Form for new interaction
        interactions_section = f'''<form method="post" action="/interactions/add?customer_id={customer['id']}" style="margin-bottom:1rem;">
            <label>Type interactie<br>
                <select name="interaction_type" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;">
                    <option value="">Selecteer...</option>
                    <option value="call">Bellen</option>
                    <option value="email">E‑mail</option>
                    <option value="message">Bericht</option>
                    <option value="meeting">Meeting</option>
                </select>
            </label>
            <label>Notitie (optioneel)<br>
                <input type="text" name="note" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <button type="submit" style="background-color:#c2185b; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Interacte toevoegen</button>
        </form>'''
        # List interactions
        if interactions:
            for interaction in interactions:
                type_label = {
                    'call': 'Bellen',
                    'email': 'E‑mail',
                    'message': 'Bericht',
                    'meeting': 'Meeting'
                }.get(interaction['interaction_type'], interaction['interaction_type'])
                note_part = f"<br><small>{html.escape(interaction['note'])}</small>" if interaction['note'] else ''
                interactions_section += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0;">
                    <strong>{html.escape(type_label)}</strong>{note_part}
                    <div style="font-size:0.8rem; color:#666;">{interaction['created_at']} door {html.escape(interaction['author'])}</div>
                </div>'''
        else:
            interactions_section += '<p>Er zijn nog geen interacties.</p>'
        body += f'''<div class="card">
            <div class="section-title">Interacties</div>
            {interactions_section}
        </div>'''
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