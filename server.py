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

# Additional imports for CSV/XLSX parsing in import feature
import io
import csv
import re
try:
    import openpyxl  # voor .xlsx-bestanden
except Exception:
    openpyxl = None

# Allowed base columns for import
ALLOWED_BASE_COLS = {
    'name', 'email', 'phone', 'address', 'company', 'tags', 'category', 'custom_fields'
}

# Mapping from common Dutch column names to internal English names (case-insensitive).
HEADER_MAP_NL_EN = {
    'naam': 'name',
    'bedrijf': 'company',
    'e-mail': 'email',
    'email': 'email',
    'mail': 'email',
    'telefoon': 'phone',
    'telefoonnr': 'phone',
    'telefoonnummer': 'phone',
    'mobiel': 'phone',
    'adres': 'address',
    'straat': 'address',
    'tags': 'tags',
    'label': 'tags',
    'type': 'category',
    'categorie': 'category',
    'custom_fields': 'custom_fields',
    'extra': 'custom_fields',
}

def _norm(s: str) -> str:
    """Normalize cell values: return stripped string or empty string."""
    return (s or '').strip()

def _norm_key(s: str) -> str:
    """Normalize header keys: lower-case, strip spaces/punctuation for matching."""
    k = (s or '').strip().lower()
    k = re.sub(r'\s+', ' ', k)
    k = k.replace(':', '').replace(';', '').replace('#', '').replace('\u00ad', '')
    return k

def _map_header(h: str, dynamic_fields_lc: set[str]) -> Optional[str]:
    """Map a raw header to its normalized internal name (English or cf_ dynamic)."""
    h0 = _norm_key(h)
    if h0 in HEADER_MAP_NL_EN:
        return HEADER_MAP_NL_EN[h0]
    if h0.startswith('cf_'):
        fld = h0[3:]
        # Only allow dynamic fields defined in DB
        return f"cf_{fld}" if fld in dynamic_fields_lc else None
    if h0 in ALLOWED_BASE_COLS:
        return h0
    return None

def parse_import_file(file_bytes: bytes, filename: str, dynamic_fields: List[str]) -> List[Dict[str, str]]:
    """
    Read a CSV or XLSX file and return a list of row dictionaries with only allowed columns.
    - Unknown columns are ignored.
    - Supports Dutch or English headers.
    - Collects dynamic fields prefixed with cf_ into a JSON object later.
    """
    # Lowercase dynamic field names for matching cf_ headers
    dyn_lc = {d.lower().strip() for d in dynamic_fields}

    # Read headers and rows based on file type
    if filename.lower().endswith('.xlsx'):
        if not openpyxl:
            raise RuntimeError("Excel-import vereist openpyxl op de server.")
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        iterator = ws.iter_rows(values_only=True)
        try:
            headers = [str(h or '').strip() for h in next(iterator)]
        except StopIteration:
            return []
        rows = [list(r) for r in iterator]
    else:
        # Attempt to auto-detect delimiter (comma or semicolon or tab)
        text = file_bytes.decode('utf-8-sig', errors='replace')
        sample = text[:2048]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t'])
            delim = dialect.delimiter
        except Exception:
            delim = ';' if sample.count(';') >= sample.count(',') else ','
        reader = csv.reader(io.StringIO(text), delimiter=delim)
        data = list(reader)
        if not data:
            return []
        headers = [h.strip() for h in data[0]]
        rows = data[1:]

    # Map headers to internal names; Unknown columns get None
    mapped_headers = [_map_header(h, dyn_lc) for h in headers]
    result: List[Dict[str, str]] = []

    for r in rows:
        rowmap: Dict[str, str] = {}
        # Collect allowed columns
        for idx, mk in enumerate(mapped_headers):
            if mk is None or idx >= len(r):
                continue
            val = r[idx]
            if val is None:
                continue
            rowmap[mk] = _norm(str(val))
        # Extract dynamic custom fields (cf_<name>)
        custom_data: Dict[str, str] = {}
        for k in list(rowmap.keys()):
            if k.startswith('cf_'):
                custom_data[k[3:]] = rowmap.pop(k)
        # Merge custom_fields column if provided (can contain JSON or key=value lines)
        raw_cf = rowmap.pop('custom_fields', '')
        if raw_cf:
            try:
                import json
                if raw_cf.strip().startswith('{'):
                    custom_data.update(json.loads(raw_cf))
                else:
                    for line in raw_cf.splitlines():
                        if '=' in line:
                            k2, v2 = line.split('=', 1)
                            custom_data[_norm(k2)] = _norm(v2)
            except Exception:
                pass
        # Normalize category to 'klant' or 'netwerk'
        cat = (rowmap.get('category') or '').lower()
        if cat in ('klant', 'client', 'customer'):
            rowmap['category'] = 'klant'
        elif cat in ('netwerk', 'network', 'partner', 'relatie'):
            rowmap['category'] = 'netwerk'
        else:
            rowmap['category'] = 'klant'
        # Normalize tags: use comma as separator; unify semicolon if needed
        tags_val = rowmap.get('tags')
        if tags_val:
            sep = ';' if tags_val.count(';') >= tags_val.count(',') else ','
            rowmap['tags'] = ','.join([_norm(t) for t in tags_val.split(sep) if _norm(t)])
        # Keep dynamic custom JSON under special key for insertion later
        if custom_data:
            import json
            rowmap['__custom_json'] = json.dumps(custom_data)
        # Skip rows missing both name and email; rows missing one of them are allowed.
        # A missing name will be replaced with a placeholder during import.
        if not rowmap.get('name') and not rowmap.get('email'):
            continue
        result.append(rowmap)
    return result


# Configuration constants
import os

HOST = '0.0.0.0'
# Use the PORT environment variable if provided (e.g. by hosting platforms like Render).
PORT = int(os.environ.get('PORT', '8000'))
# Path to the SQLite database file.
#
# We attempt to use a persistent disk mount if it exists. When deploying
# on Render with a persistent disk, set the disk's mount path to
# '/var/data' (or another absolute path) and Render will mount your disk
# there. If that directory exists, we store our database in it so that
# data persists across deploys. Otherwise we fall back to the script
# directory.
PERSISTENT_DIR = '/var/data'
if os.path.isdir(PERSISTENT_DIR):
    # Ensure the directory exists (Render creates it automatically when a disk is attached)
    # but create it locally if running without a disk.
    os.makedirs(PERSISTENT_DIR, exist_ok=True)
    DB_PATH = os.path.join(PERSISTENT_DIR, 'crm.db')
else:
    DB_PATH = os.path.join(os.path.dirname(__file__), 'crm.db')

# In‑memory session store: maps session_id -> user_id
sessions: Dict[str, int] = {}

# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------
def log_action(user_id: Optional[int], action: str, table: str, row_id: Optional[int], details: str = '') -> None:
    """Record an audit log entry.

    Args:
        user_id: The ID of the user performing the action (may be None for
            unauthenticated actions).
        action: A short string describing the action (e.g. 'create', 'update',
            'delete').
        table: The name of the table being modified (customers, tasks, notes,
            interactions).
        row_id: The primary key of the affected row (if applicable).
        details: Optional textual description of what changed.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            'INSERT INTO audit_logs (user_id, action, table_name, row_id, details) VALUES (?, ?, ?, ?, ?)',
            (user_id, action, table, row_id, details)
        )
        conn.commit()

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
                -- custom_fields stores a JSON object with arbitrary key/value pairs
                -- for additional attributes (e.g. LinkedIn URL, birthday, interests).
                custom_fields TEXT,
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

        # Ensure the custom_fields column exists.  It stores a JSON string of
        # arbitrary fields for a customer.  Older databases may not have
        # this column; add it if missing.
        try:
            cur.execute('SELECT custom_fields FROM customers LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE customers ADD COLUMN custom_fields TEXT')
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

        # Create audit_logs table for tracking changes.  Each log entry
        # includes the user performing the action, the type of action (add, edit,
        # delete), the target table, the affected row ID and optional details.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                table_name TEXT NOT NULL,
                row_id INTEGER,
                details TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
            );
        ''')

        # Create customer_fields table to hold dynamic field definitions.  Each
        # entry has a unique name (internal key) and a label (display name).
        # Admins can manage these fields via the /fields interface.  Values
        # for these fields are stored per customer in the custom_fields JSON.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS customer_fields (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL
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


def get_custom_field_definitions() -> List[sqlite3.Row]:
    """Return a list of dynamic customer field definitions.

    Each definition has `id`, `name` and `label` columns.  Admins can
    add or remove these definitions via the /fields page.  The order is
    determined by the `id` column (insertion order).
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('SELECT * FROM customer_fields ORDER BY id ASC')
        return cur.fetchall()


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
    # Inline CSS definitions.  In addition to layout and colour
    # variables defined earlier, include rules for tables so that
    # listings (e.g. the customers page) span the full width and have
    # generous padding.  Without Bootstrap, we need to style table
    # elements manually to improve readability.
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
    /* Table styling: ensure full-width tables with consistent padding. */
    table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
    th, td { padding: 0.6rem 0.75rem; text-align: left; border-bottom: 1px solid #dee2e6; }
    th { background-color: #f2f2f2; font-weight: bold; }
    tr:nth-child(even) td { background-color: #f9f9f9; }
    .text-end { text-align: right; }
    .btn { display: inline-block; padding: 0.3rem 0.75rem; border: none; border-radius: 4px; font-size: 0.9rem; cursor: pointer; text-decoration: none; }
    .btn-primary { background-color: #c2185b; color: #fff; }
    .btn-secondary { background-color: #6c757d; color: #fff; }
    .btn-danger { background-color: #dc3545; color: #fff; }
    .btn-sm { font-size: 0.8rem; padding: 0.2rem 0.6rem; }
    .form-control { padding: 0.4rem 0.6rem; border: 1px solid #ced4da; border-radius: 4px; width: 100%; }
    .btn-outline-success { border: 2px solid #198754; color: #198754; background: transparent; border-radius: 4px; padding: 0.3rem 0.7rem; }
    .btn-outline-success:hover { background-color: #198754; color: #fff; }
    .d-flex { display: flex; }
    .me-2 { margin-right: 0.5rem; }
    .text-end { text-align: right; }
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
            nav_links.append("<a href='/fields'>Velden</a>")
            nav_links.append("<a href='/reports'>Rapporten</a>")
        # Import link accessible to all logged-in users
        nav_links.append("<a href='/import'>Importeren</a>")
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

    def _parse_multipart(self, body: bytes, boundary: str) -> Dict[str, Tuple[str, bytes]]:
        """
        Parse a multipart/form-data request body and return a mapping of form field names
        to (filename, content) tuples. Only handles simple cases and assumes that each
        part is delineated by the specified boundary. Trailing CRLF and boundary markers
        are stripped from the content.

        Args:
            body: The raw request body bytes containing the multipart payload.
            boundary: The boundary string specified in the Content-Type header (without
                the leading --).

        Returns:
            A dictionary where keys are form field names and values are (filename, content)
            pairs. The filename may be None if the part did not include a filename.
        """
        files: Dict[str, Tuple[str, bytes]] = {}
        delim = ('--' + boundary).encode()
        # Split the body by the boundary. Each valid part will be between boundary markers.
        for part in body.split(delim):
            # Skip empty segments and the closing marker
            if not part or part in (b'--\r\n', b'--'):
                continue
            # Each part begins with CRLF
            # Separate headers from content using double CRLF
            if b'\r\n\r\n' not in part:
                continue
            header_block, content_block = part.split(b'\r\n\r\n', 1)
            # Decode headers
            header_lines = header_block.decode(errors='ignore').strip().split('\r\n')
            name = None
            filename = None
            for line in header_lines:
                if ':' not in line:
                    continue
                key, value = line.split(':', 1)
                key = key.lower().strip()
                value = value.strip()
                if key == 'content-disposition':
                    # Example: form-data; name="file"; filename="contacts.csv"
                    for item in value.split(';'):
                        item = item.strip()
                        if item.startswith('name='):
                            name = item.split('=', 1)[1].strip('"')
                        elif item.startswith('filename='):
                            filename = item.split('=', 1)[1].strip('"')
            if name:
                # Remove trailing CRLF and boundary markers from content
                content = content_block.rstrip(b'\r\n--')
                files[name] = (filename, content)
        return files

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
                # Collect dynamic field values.  Dynamic field inputs use the
                # prefix 'cf_' followed by the field name.  Additionally, the
                # raw custom_fields textarea (if present) allows JSON or
                # key=value pairs to be specified.  We merge both sources.
                import json
                raw_custom = params.get('custom_fields', [''])[0].strip()
                custom_dict: Dict[str, Any] = {}
                # Merge values from dynamic fields definitions
                try:
                    for field_def in get_custom_field_definitions():
                        key = field_def['name']
                        val = params.get(f'cf_{key}', [''])[0].strip()
                        if val:
                            custom_dict[key] = val
                except Exception:
                    pass
                # Merge any raw custom fields.  Accept JSON or key=value per line.
                if raw_custom:
                    try:
                        data = json.loads(raw_custom)
                        if isinstance(data, dict):
                            custom_dict.update({str(k): str(v) for k, v in data.items()})
                    except Exception:
                        for line in raw_custom.splitlines():
                            if '=' in line:
                                k, v = line.split('=', 1)
                                custom_dict[k.strip()] = v.strip()
                custom_fields = json.dumps(custom_dict) if custom_dict else None
                if not name or not email_c:
                    self.render_customer_form(None, error='Naam en e‑mail zijn verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT id FROM customers WHERE email = ?', (email_c,))
                    if cur.fetchone():
                        self.render_customer_form(None, error='Er bestaat al een klant met dit e‑mailadres.')
                        return
                    cur.execute('''INSERT INTO customers (name, email, phone, address, company, tags, category, created_by, custom_fields) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (name,
                                 email_c,
                                 phone or None,
                                 address or None,
                                 company or None,
                                 tags or None,
                                 category,
                                 user_id,
                                 custom_fields))
                    cid_new = cur.lastrowid
                    conn.commit()
                # Log the creation
                log_action(user_id, 'create', 'customers', cid_new, f"name={name}")
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
                # Parse dynamic and raw custom fields.  Merge into a dict and
                # encode as JSON for storage.  Supports JSON or key=value lines
                # for raw custom_fields textarea.
                import json
                raw_custom = params.get('custom_fields', [''])[0].strip()
                custom_dict: Dict[str, Any] = {}
                try:
                    for field_def in get_custom_field_definitions():
                        key = field_def['name']
                        val = params.get(f'cf_{key}', [''])[0].strip()
                        if val:
                            custom_dict[key] = val
                except Exception:
                    pass
                if raw_custom:
                    try:
                        data = json.loads(raw_custom)
                        if isinstance(data, dict):
                            custom_dict.update({str(k): str(v) for k, v in data.items()})
                    except Exception:
                        for line in raw_custom.splitlines():
                            if '=' in line:
                                k, v = line.split('=', 1)
                                custom_dict[k.strip()] = v.strip()
                custom_fields = json.dumps(custom_dict) if custom_dict else None
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
                                        custom_fields=?,
                                        updated_at=CURRENT_TIMESTAMP
                                    WHERE id = ?''',
                                (name,
                                 email_c,
                                 phone or None,
                                 address or None,
                                 company or None,
                                 tags or None,
                                 category,
                                 custom_fields,
                                 cid_int))
                    conn.commit()
                # Log the update
                log_action(user_id, 'update', 'customers', cid_int, f"name={name}")
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
            # Log deletion
            log_action(user_id, 'delete', 'customers', cid_int)
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
                        note_id = cur.lastrowid
                        conn.commit()
                    # Log note creation
                    log_action(user_id, 'create', 'notes', note_id)
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
            # Log note deletion
            log_action(user_id, 'delete', 'notes', nid_int)
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
                    task_id = cur.lastrowid
                    conn.commit()
                # Log the task creation
                log_action(user_id, 'create', 'tasks', task_id, f"title={title}")
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
            # Log completion
            log_action(user_id, 'update', 'tasks', tid_int, 'status=completed')
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
            # Log deletion
            log_action(user_id, 'delete', 'tasks', tid_int)
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
                    inter_id = cur.lastrowid
                    conn.commit()
                # Log new interaction
                log_action(user_id, 'create', 'interactions', inter_id, f"type={interaction_type}")
                self.send_response(302)
                self.send_header('Location', f'/customers/view?id={cid_int}')
                self.end_headers()
        elif path == '/export':
            # Export customers to CSV.  Only admin can download the export.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            # Prepare CSV using csv module to ensure proper escaping
            import csv, io
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('SELECT c.*, u.username AS creator_name FROM customers c LEFT JOIN users u ON c.created_by = u.id ORDER BY c.id ASC')
                rows = cur.fetchall()
            header = ['id','name','email','phone','address','company','tags','category','created_by','creator_name','custom_fields','created_at','updated_at']
            output_io = io.StringIO()
            writer = csv.writer(output_io)
            writer.writerow(header)
            for r in rows:
                writer.writerow([r[h] if r[h] is not None else '' for h in header])
            content = output_io.getvalue().encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="customers_export.csv"')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        elif path == '/audit':
            # Display audit logs.  Only admin can view.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            # Fetch logs
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('''SELECT a.*, u.username FROM audit_logs a LEFT JOIN users u ON a.user_id = u.id ORDER BY a.created_at DESC LIMIT 200''')
                logs = cur.fetchall()
            self.render_audit_logs(logs, username)
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
        elif path == '/fields':
            # List and manage dynamic customer fields.  Admins only.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            # Show list of custom field definitions and add form
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('SELECT * FROM customer_fields ORDER BY id ASC')
                fields = cur.fetchall()
            self.render_fields_list(fields, username)
        elif path == '/fields/delete':
            # Delete a field definition by id.  Only admin.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            fid = query_params.get('id', [None])[0]
            try:
                fid_int = int(fid) if fid else None
            except Exception:
                fid_int = None
            if fid_int:
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute('DELETE FROM customer_fields WHERE id = ?', (fid_int,))
                    conn.commit()
                # Log deletion of field (table name customer_fields, row_id)
                log_action(user_id, 'delete', 'customer_fields', fid_int)
            self.respond_redirect('/fields')
        elif path == '/import':
            # Import page: allow all logged-in users to import customers.
            if not logged_in:
                # Redirect unauthenticated users to login
                self.respond_redirect('/login')
                return
            # Show upload form
            if method == 'GET':
                self.render_import_form(username)
                return
            # POST: handle uploaded file
            # Wrap entire handler in a try/except so unexpected errors don't crash the server.
            try:
                ctype = self.headers.get('Content-Type', '') or ''
                if 'multipart/form-data' not in ctype:
                    self.render_import_form(username, error='Ongeldig formulier.')
                    return
                # Enforce max upload size (5 MB)
                try:
                    content_length = int(self.headers.get('Content-Length', '0'))
                except Exception:
                    content_length = 0
                if content_length > 5 * 1024 * 1024:
                    self.render_import_form(username, error='Bestand is te groot (max 5MB).')
                    return
                # Read request body
                body = self.rfile.read(content_length)
                # Extract boundary
                try:
                    boundary = ctype.split('boundary=')[1]
                except Exception:
                    self.render_import_form(username, error='Ongeldige multipart-indeling.')
                    return
                # Parse multipart form data safely
                try:
                    files = self._parse_multipart(body, boundary)
                except Exception:
                    self.render_import_form(username, error='Fout bij het verwerken van het uploadformulier.')
                    return
                # Validate file field
                file_tuple = files.get('file')
                if not file_tuple or file_tuple[1] is None:
                    self.render_import_form(username, error='Selecteer een bestand om te importeren.')
                    return
                filename, file_bytes = file_tuple
                if not filename:
                    filename = 'upload.csv'
                # Get dynamic field names from DB (may be empty)
                try:
                    dyn_defs = get_custom_field_definitions()
                    dyn_names = [d['name'] for d in dyn_defs]
                except Exception:
                    dyn_names = []
                # Parse the uploaded file (CSV or XLSX)
                try:
                    rows = parse_import_file(file_bytes, filename, dyn_names)
                except Exception as e:
                    self.render_import_form(username, error=f'Importfout: {e}')
                    return
                # Insert rows into DB
                imported = 0
                errors: List[str] = []
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    for row in rows:
                        # Remove extracted JSON before insertion
                        custom_json = row.pop('__custom_json', None)
                        # Determine a base name. If missing or empty, use a placeholder.
                        raw_name = (row.get('name') or '').strip()
                        base_name = raw_name if raw_name else 'Naam onbekend'
                        # Ensure the name is unique by appending a number when necessary.
                        final_name = base_name
                        suffix = 1
                        # Query existing customers for name collisions
                        while True:
                            cur.execute('SELECT COUNT(*) FROM customers WHERE name = ?', (final_name,))
                            existing_count = cur.fetchone()[0]
                            if existing_count == 0:
                                break
                            suffix += 1
                            final_name = f"{base_name} {suffix}"
                        # Prepare values for insertion
                        try:
                            cur.execute(
                                'INSERT INTO customers (name, email, phone, address, company, tags, category, created_by, custom_fields)\n                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                (
                                    final_name,
                                    row.get('email'),
                                    row.get('phone'),
                                    row.get('address'),
                                    row.get('company'),
                                    row.get('tags'),
                                    row.get('category'),
                                    user_id,
                                    custom_json,
                                ),
                            )
                            cust_id = cur.lastrowid
                            conn.commit()
                            imported += 1
                            # Audit log for each created customer
                            log_action(user_id, 'create', 'customers', cust_id, 'import')
                        except sqlite3.IntegrityError:
                            # Likely a unique constraint on email; record error and continue
                            errors.append(f"E‑mail al aanwezig: {row.get('email')}")
                            continue
                # Show result page
                self.render_import_result(username, imported, errors)
            except Exception as e:
                # Catch any unexpected exception and display as error
                self.render_import_form(username, error=f'Onbekende fout: {e}')
                return
        elif path == '/fields/add':
            # Process new field creation.  Admin only.  Accepts POST with name and label.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                fname = params.get('name', [''])[0].strip()
                flabel = params.get('label', [''])[0].strip()
                if not fname or not flabel:
                    # re-render with error
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.row_factory = sqlite3.Row
                        cur = conn.cursor()
                        cur.execute('SELECT * FROM customer_fields ORDER BY id ASC')
                        fields = cur.fetchall()
                    self.render_fields_list(fields, username, error='Naam en label zijn verplicht.')
                    return
                # insert
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    try:
                        cur.execute('INSERT INTO customer_fields (name, label) VALUES (?, ?)', (fname, flabel))
                        fid = cur.lastrowid
                        conn.commit()
                        # log action
                        log_action(user_id, 'create', 'customer_fields', fid, f"name={fname}")
                    except sqlite3.IntegrityError:
                        # duplicate name
                        with sqlite3.connect(DB_PATH) as conn2:
                            conn2.row_factory = sqlite3.Row
                            cur2 = conn2.cursor()
                            cur2.execute('SELECT * FROM customer_fields ORDER BY id ASC')
                            fields = cur2.fetchall()
                        self.render_fields_list(fields, username, error='Naam bestaat al.')
                        return
                self.respond_redirect('/fields')
            else:
                self.respond_redirect('/fields')
        elif path == '/reports':
            # Display reports/dashboard for admin.  Only admin can view.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            # Fetch statistics: customers by category, tasks by status, interactions by type
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                # Customers by category
                cur.execute('SELECT category, COUNT(*) AS count FROM customers GROUP BY category')
                customer_stats = cur.fetchall()
                # Tasks by status
                cur.execute('SELECT status, COUNT(*) AS count FROM tasks GROUP BY status')
                task_stats = cur.fetchall()
                # Interactions by type
                cur.execute('SELECT interaction_type, COUNT(*) AS count FROM interactions GROUP BY interaction_type')
                interaction_stats = cur.fetchall()
            self.render_reports(username, customer_stats, task_stats, interaction_stats)
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
        # Count customers, get recent notes and tasks due soon
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM customers')
            total_customers = cur.fetchone()[0]
            # Recent notes
            cur.execute('''
                SELECT notes.id AS note_id, notes.content, notes.created_at, customers.name AS customer_name
                FROM notes
                JOIN customers ON notes.customer_id = customers.id
                ORDER BY notes.created_at DESC
                LIMIT 5
            ''')
            notes = cur.fetchall()
            # Tasks due within next 7 days for this user and still open
            cur.execute('''
                SELECT tasks.id AS task_id, tasks.title, tasks.due_date, customers.name AS customer_name
                FROM tasks
                JOIN customers ON tasks.customer_id = customers.id
                WHERE tasks.user_id = ?
                  AND tasks.status = 'open'
                  AND tasks.due_date IS NOT NULL
                  AND DATE(tasks.due_date) <= DATE('now', '+7 day')
                  AND DATE(tasks.due_date) >= DATE('now')
                ORDER BY tasks.due_date ASC
                LIMIT 5
            ''', (user_id,))
            due_tasks = cur.fetchall()
        body = html_header('Dashboard', True, username, user_id)
        body += '<h2 class="mt-4">Dashboard</h2>'
        # Summary card
        body += f'''<div class="card">
            <div class="section-title">Overzicht</div>
            <p>Totaal aantal klanten: <strong>{total_customers}</strong></p>
        </div>'''
        # Tasks due soon section
        tasks_html = ''
        if due_tasks:
            for t in due_tasks:
                date_str = t['due_date'] if t['due_date'] else ''
                tasks_html += f"<div style='border-bottom:1px solid #eee; padding:0.5rem 0;'><strong>{html.escape(t['title'])}</strong> - {html.escape(t['customer_name'])}<br><small>Vervaldatum: {date_str}</small></div>"
        else:
            tasks_html = '<p>Geen taken die binnenkort vervallen.</p>'
        body += f'''<div class="card">
            <div class="section-title">Taken komende 7 dagen</div>
            {tasks_html}
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

    def render_fields_list(self, fields: List[sqlite3.Row], username: str, error: str | None = None) -> None:
        """Render the list of dynamic customer fields and a form to add new ones."""
        # Determine current user id for nav bar; parse session
        logged_in, uid, _ = self.parse_session()
        body = html_header('Velden beheren', logged_in, username, uid)
        body += '<h2 class="mt-4">Aanpasbare velden</h2>'
        if error:
            body += f'<div class="alert alert-danger mt-2">{html.escape(error)}</div>'
        # Show existing fields in a card
        body += '<div class="card">'
        body += '<div class="section-title">Huidige velden</div>'
        if fields:
            for f in fields:
                fid = f['id']
                label = html.escape(f['label'])
                name = html.escape(f['name'])
                body += f'<div style="border-bottom:1px solid #eee; padding:0.5rem 0;">'
                body += f'<strong>{label}</strong> <small>({name})</small>'
                body += f'<a href="/fields/delete?id={fid}" style="color:#c2185b; float:right;" onclick="return confirm(\'Weet je zeker dat je dit veld wilt verwijderen?\');">Verwijder</a>'
                body += '</div>'
        else:
            body += '<p>Er zijn nog geen extra velden.</p>'
        body += '</div>'
        # Form to add new field
        body += '<div class="card">'
        body += '<div class="section-title">Nieuw veld toevoegen</div>'
        body += '''<form method="post" action="/fields/add">
                <div class="mb-3">
                    <label for="name" class="form-label">Interne naam (alleen letters/cijfers, geen spaties)</label>
                    <input type="text" class="form-control" id="name" name="name" required>
                </div>
                <div class="mb-3">
                    <label for="label" class="form-label">Label (weergave)</label>
                    <input type="text" class="form-control" id="label" name="label" required>
                </div>
                <button type="submit" class="btn btn-primary">Veld toevoegen</button>
            </form>'''
        body += '</div>'
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_import_form(self, username: str, error: str | None = None) -> None:
        """Render a form to upload a CSV file for import."""
        logged_in, uid, _ = self.parse_session()
        body = html_header('Importeren', logged_in, username, uid)
        body += '<h2 class="mt-4">Klantgegevens importeren</h2>'
        if error:
            body += f'<div class="alert alert-danger mt-2">{html.escape(error)}</div>'
        body += '''<div class="card">
            <div class="section-title">CSV/XLSX‑bestand uploaden</div>
            <form method="post" action="/import" enctype="multipart/form-data">
                <div class="mb-3">
                    <input type="file" name="file" accept=".csv,.xlsx" required>
                </div>
                <button type="submit" class="btn btn-primary">Importeer</button>
            </form>
            <p class="mt-2"><small>Het bestand moet kolomnamen bevatten. Zowel Nederlandse (Naam, Bedrijf, E‑mail, Telefoon, Adres, Tags, Type) als Engelse varianten (name, company, email, phone, address, tags, category) worden herkend. Voor dynamische velden gebruik cf_veldnaam. Onbekende kolommen worden genegeerd.</small></p>
        </div>'''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_import_result(self, username: str, imported: int, errors: List[str]) -> None:
        """Render the result page after importing customers."""
        logged_in, uid, _ = self.parse_session()
        body = html_header('Importresultaat', logged_in, username, uid)
        body += '<h2 class="mt-4">Import resultaat</h2>'
        body += f'<div class="card"><p>{imported} klanten geïmporteerd.</p>'
        if errors:
            body += '<div class="mt-3"><strong>Fouten:</strong><ul>'
            for e in errors:
                body += f'<li>{html.escape(e)}</li>'
            body += '</ul></div>'
        body += '</div>'
        body += '<p class="mt-3"><a href="/customers" class="btn btn-primary">Terug naar klanten</a></p>'
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_reports(self, username: str, customer_stats: List[sqlite3.Row], task_stats: List[sqlite3.Row], interaction_stats: List[sqlite3.Row]) -> None:
        """Render a simple reports dashboard with aggregated statistics."""
        logged_in, uid, _ = self.parse_session()
        body = html_header('Rapporten', logged_in, username, uid)
        body += '<h2 class="mt-4">Rapporten</h2>'
        # Helper to generate bar list
        def build_bars(items, title):
            # Determine max count for scaling
            max_count = max([row['count'] for row in items], default=1)
            html_sections = f'<div class="card"><div class="section-title">{title}</div>'
            if items:
                for row in items:
                    label = html.escape(str(row[0]).capitalize())
                    count = row['count']
                    width = int((count / max_count) * 100)
                    html_sections += f'''<div style="margin:0.3rem 0;">
                        <strong>{label}</strong> ({count})
                        <div style="background-color:#e9ecef; border-radius:4px; overflow:hidden; height:8px;">
                            <div style="width:{width}%; background-color:#c2185b; height:100%;"></div>
                        </div>
                    </div>'''
            else:
                html_sections += '<p>Geen gegevens.</p>'
            html_sections += '</div>'
            return html_sections
        # Build each card
        body += build_bars(customer_stats, 'Klanten per type')
        body += build_bars(task_stats, 'Taken per status')
        body += build_bars(interaction_stats, 'Interacties per type')
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
        # Load custom fields (stored as JSON string) and present as a JSON
        # representation in the form for editing.  If no custom fields
        # exist, leave blank.  When saving, the raw text will be saved to
        # the database as is (expects valid JSON or simple key=value lines).
        # Prepare dynamic custom field values.  We parse the existing
        # custom_fields JSON (if present) to prefill inputs for each defined
        # custom field.  If parsing fails, we treat it as an empty dict.
        raw_custom_fields = ''
        existing_custom: Dict[str, Any] = {}
        if customer and customer.get('custom_fields'):
            try:
                raw_custom_fields = customer['custom_fields']
                import json
                existing_custom = json.loads(customer['custom_fields']) if customer['custom_fields'] else {}
                if not isinstance(existing_custom, dict):
                    existing_custom = {}
            except Exception:
                raw_custom_fields = customer['custom_fields']
                existing_custom = {}
        action = '/customers/edit?id={}'.format(customer['id']) if customer else '/customers/add'
        # Build HTML for dynamic fields.  Each field definition creates
        # its own input.  Values are prefilled from existing_custom.
        dynamic_fields_html = ''
        try:
            for field_def in get_custom_field_definitions():
                key = field_def['name']
                label = field_def['label']
                value = existing_custom.get(key, '')
                dynamic_fields_html += f'''<div class="mb-3">
                    <label for="cf_{html.escape(key)}" class="form-label">{html.escape(label)}</label>
                    <input type="text" class="form-control" id="cf_{html.escape(key)}" name="cf_{html.escape(key)}" value="{html.escape(str(value))}">
                </div>'''
        except Exception:
            dynamic_fields_html = ''

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
                            {dynamic_fields_html}
                            <div class="mb-3">
                                <label for="custom_fields" class="form-label">Extra velden (JSON of key=value per regel)</label>
                                <textarea class="form-control" id="custom_fields" name="custom_fields" rows="3">{html.escape(raw_custom_fields)}</textarea>
                                <small class="form-text text-muted">Voer extra eigenschappen in als JSON (bijv. {{"linkedin": "http://...", "verjaardag": "2025-10-20"}}) of als key=value per regel.</small>
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
        # Build custom fields HTML
        custom_html = ''
        try:
            if customer.get('custom_fields'):
                import json
                # Attempt to parse JSON
                data = json.loads(customer['custom_fields'])
                if isinstance(data, dict):
                    for key, value in data.items():
                        custom_html += f"<p><span class='icon'>&#128196;</span>{html.escape(str(key).capitalize())}: {html.escape(str(value))}</p>"
                else:
                    # If not dict, just show raw
                    custom_html = f"<p>{html.escape(str(data))}</p>"
        except Exception:
            # fall back to plain text
            if customer.get('custom_fields'):
                custom_html = f"<p>{html.escape(customer['custom_fields'])}</p>"

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
            {custom_html}
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

    def render_audit_logs(self, logs: List[sqlite3.Row], username: str) -> None:
        """Render a simple audit log list for admins.

        Each entry shows who performed an action, what they did and when. Only the most recent
        200 actions are displayed.
        """
        logged_in, _, _ = self.parse_session()
        body = html_header('Audit logs', logged_in, username, 1)
        body += '<h2 class="mt-4">Audit logs</h2>'
        body += '<div class="card"><table><thead><tr>'
        body += '<th>ID</th><th>Gebruiker</th><th>Actie</th><th>Tabel</th><th>Rij‑ID</th><th>Details</th><th>Tijdstip</th>'
        body += '</tr></thead><tbody>'
        if logs:
            for log in logs:
                user_display = html.escape(log['username']) if log['username'] else '-'
                body += f"<tr><td>{log['id']}</td><td>{user_display}</td><td>{html.escape(log['action'])}</td><td>{html.escape(log['table_name'])}</td><td>{log['row_id'] if log['row_id'] is not None else ''}</td><td>{html.escape(log['details'] or '')}</td><td>{log['created_at']}</td></tr>"
        else:
            body += '<tr><td colspan="7">Geen logboeken gevonden.</td></tr>'
        body += '</tbody></table></div>'
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
