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
import datetime
import time
import threading
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
    'name', 'email', 'phone', 'address', 'company', 'tags', 'category', 'custom_fields', 'website', 'industry', 'company_size', 'region'
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
        'website': 'website',
    'branche': 'industry',
    'grootte': 'company_size',
    'regio': 'region',
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
        if not rowmap.get('name') and not rowmap.get('email') and not rowmap.get('company'):
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
                        website TEXT,
        industry TEXT,
        company_size TEXT,
        region TEXT,
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
        # Ensure relation_type column exists (intern/extern).
        try:
            cur.execute('SELECT relation_type FROM customers LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE customers ADD COLUMN relation_type TEXT DEFAULT 'extern'")
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
        # Add reminder_sent column to tasks table if missing (0 = not sent, 1 = sent)
        try:
            cur.execute("ALTER TABLE tasks ADD COLUMN reminder_sent INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
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
        # Add contact_date to interactions if missing (allows backdating contacts)
        try:
            cur.execute('SELECT contact_date FROM interactions LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE interactions ADD COLUMN contact_date DATE')

        # Create documents table for storing SharePoint links per customer.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
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

        # Create customer_users table for many-to-many customer-user linking.
        # Allows multiple users (account managers) to be linked to one customer.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS customer_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                linked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(customer_id, user_id),
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


def get_linked_user_ids(customer_id: int) -> List[int]:
    """Return the list of user IDs linked to a customer."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute('SELECT user_id FROM customer_users WHERE customer_id = ?', (customer_id,))
        return [row[0] for row in cur.fetchall()]


def check_and_create_reminders() -> None:
    """Maintain one open reminder task per customer-user link.

    The reminder due date is always: last_contact_date + 90 days.
    - New customer linked today  → reminder due in 90 days.
    - Contact logged 2 months ago → reminder due in 1 month.
    - New contact logged today  → existing reminder pushed forward 90 days.
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('''
            SELECT cu.customer_id, cu.user_id, c.name AS customer_name
            FROM customer_users cu
            JOIN customers c ON cu.customer_id = c.id
        ''')
        links = cur.fetchall()
        for link in links:
            cid = link['customer_id']
            uid = link['user_id']
            customer_name = link['customer_name']
            # Look up the accountmanager's username for display in the task
            cur.execute('SELECT username FROM users WHERE id = ?', (uid,))
            user_row = cur.fetchone()
            account_name = user_row['username'] if user_row else f'gebruiker {uid}'
            # Last contact = most recent activity across notes, interactions and tasks.
            # For interactions we use contact_date when set (supports backdating),
            # otherwise fall back to created_at. Notes and tasks always use created_at.
            cur.execute('''
                SELECT MAX(last_contact) AS last_contact FROM (
                    SELECT MAX(DATE(created_at)) AS last_contact
                      FROM notes WHERE customer_id = ?
                    UNION ALL
                    SELECT MAX(COALESCE(contact_date, DATE(created_at))) AS last_contact
                      FROM interactions WHERE customer_id = ?
                    UNION ALL
                    SELECT MAX(DATE(created_at)) AS last_contact
                      FROM tasks
                     WHERE customer_id = ? AND title NOT LIKE 'Herinnering:%'
                )
            ''', (cid, cid, cid))
            row = cur.fetchone()
            last_contact = row['last_contact'] if row else None
            # Fall back to customer creation date (new customers also need follow-up)
            if not last_contact:
                cur.execute('SELECT created_at FROM customers WHERE id = ?', (cid,))
                cust_row = cur.fetchone()
                last_contact = cust_row['created_at'] if cust_row else None
            if not last_contact:
                continue
            # Reminder due = last contact + 90 days
            try:
                last_dt = datetime.datetime.strptime(last_contact[:10], '%Y-%m-%d')
            except ValueError:
                continue
            reminder_due = last_dt + datetime.timedelta(days=90)
            due_str = reminder_due.strftime('%Y-%m-%d')
            last_str = last_dt.strftime('%d-%m-%Y')
            # Check for existing open reminder for this customer+user
            cur.execute('''
                SELECT id, due_date FROM tasks
                WHERE customer_id = ? AND user_id = ? AND status = 'open'
                  AND title LIKE 'Herinnering:%'
            ''', (cid, uid))
            existing = cur.fetchone()
            description = (
                f'Taak voor {account_name}: neem contact op met {customer_name}. '
                f'Laatste contact: {last_str}.'
            )
            if existing:
                # Update due_date and description if last contact changed
                if existing['due_date'] != due_str:
                    cur.execute(
                        'UPDATE tasks SET due_date = ?, description = ? WHERE id = ?',
                        (due_str, description, existing['id'])
                    )
            else:
                # Create new reminder with correct due date
                cur.execute('''
                    INSERT INTO tasks (title, description, due_date, customer_id, user_id)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    f'Herinnering: neem contact op met {customer_name}',
                    description,
                    due_str, cid, uid
                ))
        conn.commit()


def _reminder_loop() -> None:
    """Background thread: run reminder checks once daily."""
    while True:
        try:
            check_and_create_reminders()
        except Exception as e:
            print(f'[Reminder] Fout bij controleren herinneringen: {e}')
        time.sleep(86400)  # 24 uur


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
        nav_links.append("<a href='/tasks/search'>Taken zoeken</a>")
        nav_links_left = ''.join(nav_links)
        profile_link = f"<a href='/users/profile?id={user_id}'>Mijn profiel</a>" if user_id else ''
        nav_links_right = f"{profile_link} <span style='color:rgba(255,255,255,0.6)'>|</span> <span>Ingelogd als {html.escape(username)}</span> <a href='/logout'>Uitloggen</a>"
        nav_search = '''<form method="get" action="/customers" style="display:flex;align-items:center;margin:0 1rem;">
            <input type="search" name="q" placeholder="&#128269; Klant zoeken..." style="padding:0.25rem 0.6rem;border:none;border-radius:4px 0 0 4px;font-size:0.85rem;width:160px;outline:none;">
            <button type="submit" style="padding:0.25rem 0.6rem;background:#a3154e;color:#fff;border:none;border-radius:0 4px 4px 0;cursor:pointer;font-size:0.85rem;">&#10132;</button>
        </form>'''
    else:
        nav_links_left = ''
        nav_links_right = "<a href='/login'>Inloggen</a>"
        nav_search = ''
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
    {nav_search}
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
            relation_filter = query_params.get('relatie', [''])[0].strip()
            if relation_filter not in ('intern', 'extern'):
                relation_filter = ''
            sort_col = query_params.get('sort', ['name'])[0].strip()
            sort_dir = query_params.get('dir', ['asc'])[0].strip()
            if sort_col not in ('name', 'company', 'category', 'relation_type', 'created_at'):
                sort_col = 'name'
            if sort_dir not in ('asc', 'desc'):
                sort_dir = 'asc'
            self.render_customers(search, relation_filter, sort_col, sort_dir)
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
                relation_type = params.get('relation_type', ['extern'])[0].strip()
                if relation_type not in ('intern', 'extern'):
                    relation_type = 'extern'
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
                    cur.execute('''INSERT INTO customers (name, email, phone, address, company, tags, category, relation_type, created_by, custom_fields) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (name,
                                 email_c,
                                 phone or None,
                                 address or None,
                                 company or None,
                                 tags or None,
                                 category,
                                 relation_type,
                                 user_id,
                                 custom_fields))
                    cid_new = cur.lastrowid
                    conn.commit()
                # Log the creation
                log_action(user_id, 'create', 'customers', cid_new, f"name={name}")
                # Save customer-user links (many-to-many)
                linked_user_ids = params.get('linked_users', [])
                if linked_user_ids:
                    with sqlite3.connect(DB_PATH) as conn2:
                        cur2 = conn2.cursor()
                        for uid_str in linked_user_ids:
                            try:
                                cur2.execute(
                                    'INSERT OR IGNORE INTO customer_users (customer_id, user_id) VALUES (?, ?)',
                                    (cid_new, int(uid_str))
                                )
                            except (ValueError, sqlite3.Error):
                                pass
                        conn2.commit()
                # Immediately create reminder tasks for linked users
                try:
                    check_and_create_reminders()
                except Exception:
                    pass
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
                relation_type = params.get('relation_type', ['extern'])[0].strip()
                if relation_type not in ('intern', 'extern'):
                    relation_type = 'extern'
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
                                        relation_type=?,
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
                                 relation_type,
                                 custom_fields,
                                 cid_int))
                    conn.commit()
                # Log the update
                log_action(user_id, 'update', 'customers', cid_int, f"name={name}")
                # Update customer-user links: replace existing with new selection
                linked_user_ids = params.get('linked_users', [])
                with sqlite3.connect(DB_PATH) as conn2:
                    cur2 = conn2.cursor()
                    cur2.execute('DELETE FROM customer_users WHERE customer_id = ?', (cid_int,))
                    for uid_str in linked_user_ids:
                        try:
                            cur2.execute(
                                'INSERT OR IGNORE INTO customer_users (customer_id, user_id) VALUES (?, ?)',
                                (cid_int, int(uid_str))
                            )
                        except (ValueError, sqlite3.Error):
                            pass
                    conn2.commit()
                # Immediately create reminder tasks for newly linked users
                try:
                    check_and_create_reminders()
                except Exception:
                    pass
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
        elif path == '/customers/bulk':
            # Bulk action on selected customers (POST only).
            if not logged_in:
                self.respond_redirect('/login')
                return
            if method != 'POST':
                self.respond_redirect('/customers')
                return
            length = int(self.headers.get('Content-Length', 0))
            data = self.rfile.read(length).decode('utf-8')
            params = urllib.parse.parse_qs(data)
            action = params.get('bulk_action', [''])[0].strip()
            # selected_ids is a list of values from multiple checkboxes
            selected = params.get('selected_ids', [])
            cid_list = []
            for s in selected:
                try:
                    cid_list.append(int(s))
                except ValueError:
                    pass
            if cid_list and action:
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    if action in ('intern', 'extern'):
                        for cid_b in cid_list:
                            cur.execute('UPDATE customers SET relation_type=? WHERE id=?', (action, cid_b))
                            log_action(user_id, 'update', 'customers', cid_b, f'bulk relation_type={action}')
                    elif action == 'add_tag':
                        tag_val = params.get('bulk_tag', [''])[0].strip()
                        if tag_val:
                            for cid_b in cid_list:
                                cur.execute('SELECT tags FROM customers WHERE id=?', (cid_b,))
                                row = cur.fetchone()
                                existing = (row[0] or '') if row else ''
                                tags_list = [t.strip() for t in existing.split(',') if t.strip()]
                                if tag_val not in tags_list:
                                    tags_list.append(tag_val)
                                cur.execute('UPDATE customers SET tags=? WHERE id=?', (','.join(tags_list), cid_b))
                                log_action(user_id, 'update', 'customers', cid_b, f'bulk add_tag={tag_val}')
                    elif action == 'link_user':
                        uid_val = params.get('bulk_user_id', [''])[0].strip()
                        if uid_val.isdigit():
                            uid_int_b = int(uid_val)
                            for cid_b in cid_list:
                                try:
                                    cur.execute('INSERT OR IGNORE INTO customer_users (customer_id, user_id) VALUES (?,?)', (cid_b, uid_int_b))
                                    log_action(user_id, 'create', 'customer_users', cid_b, f'bulk link user={uid_int_b}')
                                except Exception:
                                    pass
                    conn.commit()
                try:
                    check_and_create_reminders()
                except Exception:
                    pass
            self.respond_redirect('/customers')
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
                try:
                    check_and_create_reminders()
                except Exception:
                    pass
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
                assigned_raw = params.get('assigned_user_id', [''])[0].strip()
                try:
                    assigned_uid = int(assigned_raw) if assigned_raw else user_id
                except ValueError:
                    assigned_uid = user_id
                if not title:
                    # Re-render customer page with error message
                    customer = self.get_customer(cid_int)
                    self.render_customer_detail(customer, user_id, username,
                                                task_error='Titel is verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute('''INSERT INTO tasks (title, description, due_date, customer_id, user_id) VALUES (?, ?, ?, ?, ?)''',
                                (title, description or None, due_date or None, cid_int, assigned_uid))
                    task_id = cur.lastrowid
                    conn.commit()
                # Log the task creation
                log_action(user_id, 'create', 'tasks', task_id, f"title={title}")
                try:
                    check_and_create_reminders()
                except Exception:
                    pass
                self.send_response(302)
                self.send_header('Location', f'/customers/view?id={cid_int}')
                self.end_headers()
            else:
                self.respond_not_found()
        elif path == '/tasks/resolve':
            # Resolve a task: mark complete AND log an interaction in one step.
            if not logged_in:
                self.respond_redirect('/login')
                return
            tid = query_params.get('id', [None])[0]
            from_page = query_params.get('from', ['dashboard'])[0]
            # Whitelist allowed from_page values to prevent open redirect
            if from_page not in ('dashboard', 'users/profile', 'tasks/search'):
                from_page = 'dashboard'
            if not tid:
                self.respond_not_found()
                return
            try:
                tid_int = int(tid)
            except ValueError:
                self.respond_not_found()
                return
            # Fetch task details
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('''
                    SELECT t.id, t.title, t.description, t.customer_id,
                           c.name AS customer_name
                    FROM tasks t JOIN customers c ON t.customer_id = c.id
                    WHERE t.id = ?
                ''', (tid_int,))
                task_row = cur.fetchone()
            if not task_row:
                self.respond_not_found()
                return
            task_data = dict(task_row)
            if method == 'GET':
                self.render_resolve_form(tid_int, task_data, user_id, username, from_page)
            elif method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                interaction_type = params.get('interaction_type', [''])[0].strip()
                note = params.get('note', [''])[0].strip()
                contact_date = params.get('contact_date', [''])[0].strip() or None
                if not interaction_type:
                    self.render_resolve_form(tid_int, task_data, user_id, username, from_page, error='Kies een contactmoment type.')
                    return
                cid_int = task_data['customer_id']
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    # Mark task complete
                    cur.execute('UPDATE tasks SET status = ? WHERE id = ?', ('completed', tid_int))
                    # Log the interaction
                    cur.execute(
                        'INSERT INTO interactions (interaction_type, note, contact_date, customer_id, user_id) VALUES (?, ?, ?, ?, ?)',
                        (interaction_type, note or None, contact_date, cid_int, user_id)
                    )
                    inter_id = cur.lastrowid
                    conn.commit()
                log_action(user_id, 'update', 'tasks', tid_int, 'status=completed via resolve')
                log_action(user_id, 'create', 'interactions', inter_id, f'type={interaction_type} via resolve')
                try:
                    check_and_create_reminders()
                except Exception:
                    pass
                # Redirect back to from_page
                if from_page == 'users/profile':
                    self.respond_redirect(f'/users/profile?id={user_id}')
                elif from_page == 'tasks/search':
                    self.respond_redirect('/tasks/search')
                else:
                    self.respond_redirect('/dashboard')
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
                contact_date = params.get('contact_date', [''])[0].strip() or None
                if not interaction_type:
                    customer = self.get_customer(cid_int)
                    self.render_customer_detail(customer, user_id, username, task_error='Interactietype is verplicht.')
                    return
                with sqlite3.connect(DB_PATH) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        'INSERT INTO interactions (interaction_type, note, contact_date, customer_id, user_id) VALUES (?, ?, ?, ?, ?)',
                        (interaction_type, note or None, contact_date, cid_int, user_id)
                    )
                    inter_id = cur.lastrowid
                    conn.commit()
                # Log new interaction
                log_action(user_id, 'create', 'interactions', inter_id, f"type={interaction_type}")
                # Recalculate reminders immediately so backdated contacts take effect now
                try:
                    check_and_create_reminders()
                except Exception:
                    pass
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
        elif path == '/tasks/search':
            # Search tasks by title/description with optional user and status filters.
            if not logged_in:
                self.respond_redirect('/login')
                return
            q = query_params.get('q', [''])[0].strip()
            filter_uid_raw = query_params.get('user_id', [''])[0]
            filter_status = query_params.get('status', [''])[0].strip()
            try:
                filter_uid = int(filter_uid_raw) if filter_uid_raw else None
            except ValueError:
                filter_uid = None
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                conditions = ["1=1"]
                args = []
                if q:
                    conditions.append('(t.title LIKE ? OR t.description LIKE ?)')
                    like = f'%{q}%'
                    args.extend([like, like])
                if filter_uid:
                    conditions.append('t.user_id = ?')
                    args.append(filter_uid)
                if filter_status in ('open', 'completed'):
                    conditions.append('t.status = ?')
                    args.append(filter_status)
                where = ' AND '.join(conditions)
                cur.execute(f'''
                    SELECT t.id AS task_id, t.title, t.description, t.due_date, t.status, t.created_at,
                           c.name AS customer_name, c.id AS customer_id,
                           u.username AS assigned_to
                    FROM tasks t
                    JOIN customers c ON t.customer_id = c.id
                    JOIN users u ON t.user_id = u.id
                    WHERE {where}
                    ORDER BY COALESCE(t.due_date,'9999-12-31') ASC, t.created_at DESC
                    LIMIT 200
                ''', args)
                results = cur.fetchall()
                cur.execute('SELECT id, username FROM users ORDER BY username ASC')
                all_users = cur.fetchall()
            today_iso = datetime.date.today().isoformat()
            body = html_header('Taken zoeken', True, username, user_id)
            body += '<h2 class="mt-4">&#128269; Taken zoeken</h2>'
            user_opts = '<option value="">Alle gebruikers</option>'
            for u in all_users:
                sel = 'selected' if filter_uid == u['id'] else ''
                user_opts += f'<option value="{u["id"]}" {sel}>{html.escape(u["username"])}</option>'
            stat_opts = f'<option value="">Alle statussen</option><option value="open" {"selected" if filter_status=="open" else ""}>Open</option><option value="completed" {"selected" if filter_status=="completed" else ""}>Voltooid</option>'
            body += f'''<div class="card" style="padding:0.75rem 1rem;">
                <form method="GET" action="/tasks/search" style="display:flex;gap:0.75rem;align-items:flex-end;flex-wrap:wrap;">
                    <div>
                        <label style="font-size:0.85rem;font-weight:bold;">Zoekterm</label><br>
                        <input type="search" name="q" value="{html.escape(q)}" placeholder="Taakttitel of omschrijving..." style="padding:0.35rem 0.6rem;border:1px solid #ced4da;border-radius:4px;min-width:220px;">
                    </div>
                    <div>
                        <label style="font-size:0.85rem;font-weight:bold;">Gebruiker</label><br>
                        <select name="user_id" style="padding:0.35rem 0.5rem;border:1px solid #ced4da;border-radius:4px;">{user_opts}</select>
                    </div>
                    <div>
                        <label style="font-size:0.85rem;font-weight:bold;">Status</label><br>
                        <select name="status" style="padding:0.35rem 0.5rem;border:1px solid #ced4da;border-radius:4px;">{stat_opts}</select>
                    </div>
                    <button type="submit" class="btn btn-primary">Zoeken</button>
                    <a href="/tasks/search" style="color:#c2185b;font-size:0.9rem;padding:0.4rem 0;">Wis filter</a>
                </form>
            </div>'''
            body += f'<div class="card"><div class="section-title">Resultaten ({len(results)})</div>'
            if results:
                body += '<table><thead><tr><th>Taak</th><th>Klant</th><th>Toegewezen aan</th><th>Vervaldatum</th><th>Status</th></tr></thead><tbody>'
                for t in results:
                    is_overdue = t['due_date'] and t['due_date'] < today_iso and t['status'] == 'open'
                    date_color = '#dc3545' if is_overdue else '#555'
                    status_badge = '<span style="background:#e8f5e9;color:#388e3c;border-radius:4px;padding:0.1rem 0.4rem;font-size:0.8rem;">Voltooid</span>' if t['status'] == 'completed' else '<span style="background:#fff8e1;color:#f57f17;border-radius:4px;padding:0.1rem 0.4rem;font-size:0.8rem;">Open</span>'
                    desc = f'<br><small style="color:#888;">{html.escape(t["description"])}</small>' if t['description'] else ''
                    resolve_btn = f' <a href="/tasks/resolve?id={t["task_id"]}&from=tasks/search" style="background:#198754;color:#fff;border-radius:4px;padding:0.1rem 0.4rem;font-size:0.75rem;text-decoration:none;">&#10003;</a>' if t['status'] == 'open' else ''
                    body += f'''<tr>
                        <td>{html.escape(t["title"])}{desc}{resolve_btn}</td>
                        <td><a href="/customers/view?id={t["customer_id"]}" style="color:#c2185b;">{html.escape(t["customer_name"])}</a></td>
                        <td>{html.escape(t["assigned_to"])}</td>
                        <td style="color:{date_color};">{t["due_date"] or "-"}</td>
                        <td>{status_badge}</td>
                    </tr>'''
                body += '</tbody></table>'
            else:
                body += '<p>Geen taken gevonden.</p>'
            body += '</div>'
            body += html_footer()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(body.encode('utf-8'))
        elif path == '/tasks/export':
            # Export all tasks to CSV.
            if not logged_in:
                self.respond_redirect('/login')
                return
            import csv, io as _io
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('''
                    SELECT t.id, t.title, t.description, t.status, t.due_date, t.created_at,
                           c.name AS customer_name, u.username AS assigned_to
                    FROM tasks t
                    JOIN customers c ON t.customer_id = c.id
                    JOIN users u ON t.user_id = u.id
                    ORDER BY t.created_at DESC
                ''')
                rows = cur.fetchall()
            header = ['id','title','description','status','due_date','customer_name','assigned_to','created_at']
            out = _io.StringIO()
            writer = csv.writer(out)
            writer.writerow(header)
            for r in rows:
                writer.writerow([r[h] if r[h] is not None else '' for h in header])
            content = out.getvalue().encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv; charset=utf-8')
            self.send_header('Content-Disposition', 'attachment; filename="taken_export.csv"')
            self.send_header('Content-Length', str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        elif path == '/tasks/archive':
            # Global archive of all completed tasks.
            if not logged_in:
                self.respond_redirect('/login')
                return
            # Optional filter by user
            filter_uid_raw = query_params.get('user_id', [None])[0]
            try:
                filter_uid = int(filter_uid_raw) if filter_uid_raw else None
            except ValueError:
                filter_uid = None
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                if filter_uid:
                    cur.execute('''
                        SELECT t.id AS task_id, t.title, t.description, t.due_date, t.created_at,
                               c.name AS customer_name, c.id AS customer_id,
                               u.username AS assigned_to
                        FROM tasks t
                        JOIN customers c ON t.customer_id = c.id
                        JOIN users u ON t.user_id = u.id
                        WHERE t.status = 'completed' AND t.user_id = ?
                        ORDER BY t.created_at DESC
                    ''', (filter_uid,))
                else:
                    cur.execute('''
                        SELECT t.id AS task_id, t.title, t.description, t.due_date, t.created_at,
                               c.name AS customer_name, c.id AS customer_id,
                               u.username AS assigned_to
                        FROM tasks t
                        JOIN customers c ON t.customer_id = c.id
                        JOIN users u ON t.user_id = u.id
                        WHERE t.status = 'completed'
                        ORDER BY t.created_at DESC
                    ''')
                done_tasks = cur.fetchall()
                cur.execute('SELECT id, username FROM users ORDER BY username ASC')
                all_users = cur.fetchall()
            # Build page
            body = html_header('Archief voltooide taken', True, username, user_id)
            body += '<h2 class="mt-4">&#10003; Archief voltooide taken</h2>'
            # Filter bar
            user_opts = '<option value="">Alle gebruikers</option>'
            for u in all_users:
                sel = 'selected' if filter_uid == u['id'] else ''
                user_opts += f'<option value="{u["id"]}" {sel}>{html.escape(u["username"])}</option>'
            body += f'''<div class="card" style="padding:0.75rem 1rem;">
                <form method="GET" action="/tasks/archive" style="display:flex;gap:1rem;align-items:center;flex-wrap:wrap;">
                    <label style="margin:0;">Filteren op gebruiker:
                        <select name="user_id" onchange="this.form.submit()" style="margin-left:0.4rem;padding:0.3rem 0.5rem;border:1px solid #ced4da;border-radius:4px;">
                            {user_opts}
                        </select>
                    </label>
                    <a href="/tasks/archive" style="color:#c2185b;font-size:0.9rem;">Wis filter</a>
                </form>
            </div>'''
            body += f'<div class="card"><div class="section-title">Voltooide taken ({len(done_tasks)}) <a href="/tasks/export" style="float:right;font-size:0.85rem;color:#c2185b;font-weight:normal;">&#8659; Exporteer alle taken (CSV)</a></div>'
            if done_tasks:
                body += '<table><thead><tr><th>Taak</th><th>Klant</th><th>Toegewezen aan</th><th>Vervaldatum</th><th>Afgerond op</th></tr></thead><tbody>'
                for t in done_tasks:
                    desc = f'<br><small style="color:#888;">{html.escape(t["description"])}</small>' if t['description'] else ''
                    body += f'''<tr>
                        <td>{html.escape(t["title"])}{desc}</td>
                        <td><a href="/customers/view?id={t["customer_id"]}" style="color:#c2185b;">{html.escape(t["customer_name"])}</a></td>
                        <td>{html.escape(t["assigned_to"])}</td>
                        <td style="color:#555;">{t["due_date"] or "-"}</td>
                        <td style="color:#888;font-size:0.85rem;">{t["created_at"][:10]}</td>
                    </tr>'''
                body += '</tbody></table>'
            else:
                body += '<p>Geen voltooide taken gevonden.</p>'
            body += '</div>'
            body += html_footer()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.end_headers()
            self.wfile.write(body.encode('utf-8'))
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
        elif path == '/users/delete':
            # Delete a user account.  Admin only.  Cannot delete the admin (id=1).
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            uid_del = query_params.get('id', [None])[0]
            try:
                uid_del_int = int(uid_del) if uid_del else None
            except ValueError:
                uid_del_int = None
            if not uid_del_int:
                self.respond_redirect('/users')
                return
            if uid_del_int == 1:
                # Protect the admin account from deletion
                self.respond_redirect('/users')
                return
            with sqlite3.connect(DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute('DELETE FROM users WHERE id = ?', (uid_del_int,))
                conn.commit()
            log_action(user_id, 'delete', 'users', uid_del_int)
            self.respond_redirect('/users')
        elif path == '/users/profile':
            # Personal dashboard: show tasks, customers and recent interactions for one user.
            # Accessible to the user themselves or to admin.
            if not logged_in:
                self.respond_redirect('/login')
                return
            profile_id_str = query_params.get('id', [str(user_id)])[0]
            try:
                profile_id = int(profile_id_str)
            except ValueError:
                self.respond_not_found()
                return
            # Only admin or the user themselves may view a profile
            if profile_id != user_id and not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            profile_user = get_user_by_id(profile_id)
            if not profile_user:
                self.respond_not_found()
                return
            self.render_user_profile(profile_user, user_id, username)
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
                        raw_name = (row.get('name') or row.get('company') or '').strip()
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
        this_month = datetime.date.today().strftime('%Y-%m')
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM customers')
            total_customers = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM tasks WHERE status='open'")
            total_open_tasks = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM tasks WHERE status='open' AND due_date < DATE('now')")
            total_overdue = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM interactions WHERE strftime('%Y-%m', COALESCE(contact_date, created_at)) = ?", (this_month,))
            interactions_this_month = cur.fetchone()[0]
            # Per-user stats: open tasks, overdue, interactions this month
            cur.execute('''
                SELECT u.id, u.username,
                    SUM(CASE WHEN t.status='open' THEN 1 ELSE 0 END) AS open_tasks,
                    SUM(CASE WHEN t.status='open' AND t.due_date < DATE('now') THEN 1 ELSE 0 END) AS overdue_tasks
                FROM users u
                LEFT JOIN tasks t ON t.user_id = u.id
                GROUP BY u.id ORDER BY u.username ASC
            ''')
            user_task_stats = cur.fetchall()
            cur.execute('''
                SELECT u.id,
                    COUNT(i.id) AS interactions_month
                FROM users u
                LEFT JOIN interactions i ON i.user_id = u.id
                    AND strftime('%Y-%m', COALESCE(i.contact_date, i.created_at)) = ?
                GROUP BY u.id
            ''', (this_month,))
            user_inter_stats = {row['id']: row['interactions_month'] for row in cur.fetchall()}
            cur.execute('SELECT created_by, COUNT(*) AS cnt FROM customers WHERE created_by IS NOT NULL GROUP BY created_by')
            user_customer_stats = {row['created_by']: row['cnt'] for row in cur.fetchall()}
            # Recent notes by this user only
            cur.execute('''
                SELECT notes.id AS note_id, notes.content, notes.created_at, customers.name AS customer_name
                FROM notes
                JOIN customers ON notes.customer_id = customers.id
                WHERE notes.user_id = ?
                ORDER BY notes.created_at DESC
                LIMIT 5
            ''', (user_id,))
            notes = cur.fetchall()
            # All open tasks (general overview): overdue + upcoming 14 days
            cur.execute('''
                SELECT tasks.id AS task_id, tasks.title, tasks.due_date,
                       customers.name AS customer_name, customers.id AS customer_id,
                       users.username AS assigned_to
                FROM tasks
                JOIN customers ON tasks.customer_id = customers.id
                JOIN users ON tasks.user_id = users.id
                WHERE tasks.status = 'open'
                  AND tasks.due_date IS NOT NULL
                  AND DATE(tasks.due_date) <= DATE('now', '+14 day')
                ORDER BY tasks.due_date ASC
                LIMIT 20
            ''')
            due_tasks = cur.fetchall()
        body = html_header('Dashboard', True, username, user_id)
        body += '<h2 class="mt-4">Dashboard</h2>'
        # Stats row
        def _stat(val, label, color='#c2185b'):
            return f'<div class="card" style="flex:1;min-width:130px;text-align:center;padding:0.75rem;"><div style="font-size:1.8rem;font-weight:bold;color:{color};">{val}</div><div style="font-size:0.85rem;color:#555;">{label}</div></div>'
        body += f'<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
        body += _stat(total_customers, 'Klanten')
        body += _stat(total_open_tasks, 'Open taken', '#f57f17')
        body += _stat(total_overdue, 'Verlopen taken', '#dc3545' if total_overdue else '#388e3c')
        body += _stat(interactions_this_month, 'Interacties deze maand', '#1565c0')
        body += '</div>'
        # Per-user stats table — collapsible
        body += '<details style="margin-bottom:0.75rem;"><summary style="cursor:pointer;font-weight:bold;padding:0.6rem 1rem;background:#fff;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">&#128200; Statistieken per gebruiker (' + this_month + ')</summary>'
        body += '<div class="card" style="margin-top:0.25rem;">'
        body += '<table><thead><tr><th>Gebruiker</th><th>Open taken</th><th>Verlopen taken</th><th>Interacties deze maand</th><th>Ingevoerde klanten</th></tr></thead><tbody>'
        for us in user_task_stats:
            inter_m = user_inter_stats.get(us['id'], 0)
            cust_m = user_customer_stats.get(us['id'], 0)
            overdue_col = '#dc3545' if (us['overdue_tasks'] or 0) > 0 else '#388e3c'
            body += f'''<tr>
                <td><a href="/users/profile?id={us['id']}" style="color:#c2185b;">{html.escape(us['username'])}</a></td>
                <td>{us['open_tasks'] or 0}</td>
                <td style="color:{overdue_col};font-weight:bold;">{us['overdue_tasks'] or 0}</td>
                <td>{inter_m}</td>
                <td>{cust_m}</td>
            </tr>'''
        body += '</tbody></table></div></details>'
        # Tasks due soon section
        today_iso = datetime.date.today().isoformat()
        tasks_html = ''
        if due_tasks:
            for t in due_tasks:
                date_str = t['due_date'] if t['due_date'] else ''
                is_overdue = date_str < today_iso
                date_color = '#dc3545' if is_overdue else '#555'
                overdue_label = ' <span style="background:#dc3545;color:#fff;font-size:0.75rem;border-radius:3px;padding:0.1rem 0.4rem;">verlopen</span>' if is_overdue else ''
                cust_link = f"<a href='/customers/view?id={t['customer_id']}' style='color:#c2185b;font-weight:bold;'>{html.escape(t['customer_name'])}</a>"
                assigned_to = html.escape(t['assigned_to']) if t['assigned_to'] else ''
                resolve_btn = f"<a href='/tasks/resolve?id={t['task_id']}&from=dashboard' style='float:right;background:#198754;color:#fff;border-radius:4px;padding:0.15rem 0.55rem;font-size:0.8rem;text-decoration:none;'>&#10003; Resolve</a>"
                tasks_html += f"<div style='border-bottom:1px solid #eee; padding:0.5rem 0;'>{resolve_btn}{html.escape(t['title'])}{overdue_label}<br>{cust_link} &middot; <small style='color:#888;'>{assigned_to}</small> &middot; <small style='color:{date_color};'>&#128197; {date_str}</small></div>"
        else:
            tasks_html = '<p>Geen openstaande taken.</p>'
        body += f'''<div class="card">
            <div class="section-title">Openstaande taken (komende 14 dagen + verlopen) <a href="/tasks/archive" style="float:right;font-size:0.85rem;color:#c2185b;font-weight:normal;">&#128451; Archief voltooide taken</a></div>
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
                is_admin_user = user['id'] == 1
                delete_btn = '' if is_admin_user else f'<a href="/users/delete?id={user["id"]}" class="btn btn-sm btn-danger" style="float:right;margin-left:0.5rem;" onclick="return confirm(\'Weet je zeker dat je {html.escape(user["username"])} wilt verwijderen?\');">Verwijder</a>'
                body += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0; display:flex; justify-content:space-between; align-items:center;">
                    <div>
                        <strong>{html.escape(user['username'])}</strong> ({html.escape(user['email'])})
                        {'<span style="font-size:0.75rem;background:#c2185b;color:#fff;border-radius:4px;padding:0.1rem 0.4rem;margin-left:0.5rem;">admin</span>' if is_admin_user else ''}
                        <div style="font-size:0.8rem; color:#666;">Aangemaakt op {user['created_at'][:10]}</div>
                    </div>
                    <div>
                        <a href="/users/profile?id={user['id']}" class="btn btn-sm btn-secondary">Profiel</a>
                        {delete_btn}
                    </div>
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

    def render_resolve_form(self, task_id: int, task: Dict[str, Any], user_id: int, username: str, from_page: str, error: str = '') -> None:
        """Render the resolve-task form: mark complete + log interaction in one step."""
        today = datetime.date.today().isoformat()
        error_html = f'<div style="color:#dc3545;margin-bottom:0.75rem;">{html.escape(error)}</div>' if error else ''
        body = html_header('Taak afronden', True, username, user_id)
        body += f'''<div class="container"><div class="card" style="max-width:560px;margin:2rem auto;">
        <h3 style="margin-top:0;">&#10003; Taak afronden</h3>
        {error_html}
        <p><strong>{html.escape(task["title"])}</strong><br>
        <small style="color:#666;">Klant: <a href="/customers/view?id={task["customer_id"]}" style="color:#c2185b;">{html.escape(task["customer_name"])}</a></small></p>
        <form method="POST" action="/tasks/resolve?id={task_id}&from={html.escape(from_page)}">
            <div style="margin-bottom:0.75rem;">
                <label style="font-weight:bold;">Contactmoment type *</label><br>
                <select name="interaction_type" class="form-control" required>
                    <option value="">-- Kies type --</option>
                    <option value="call">Bellen</option>
                    <option value="email">E-mail</option>
                    <option value="message">Bericht</option>
                    <option value="meeting">Meeting</option>
                </select>
            </div>
            <div style="margin-bottom:0.75rem;">
                <label style="font-weight:bold;">Datum contact</label><br>
                <input type="date" name="contact_date" class="form-control" value="{today}">
            </div>
            <div style="margin-bottom:1rem;">
                <label style="font-weight:bold;">Notitie (optioneel)</label><br>
                <textarea name="note" class="form-control" rows="3" placeholder="Wat is er besproken?"></textarea>
            </div>
            <button type="submit" class="btn btn-primary">&#10003; Afronden &amp; interactie opslaan</button>
            <a href="/{html.escape(from_page)}" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
        </form>
        </div></div>'''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_user_profile(self, profile_user: Dict[str, Any], viewer_id: int, viewer_username: str) -> None:
        """Render the personal dashboard for a specific user account."""
        pid = profile_user['id']
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # Open tasks assigned to this user
            cur.execute('''
                SELECT t.id AS task_id, t.title, t.due_date, t.status, t.description,
                       c.name AS customer_name, c.id AS customer_id
                FROM tasks t
                JOIN customers c ON t.customer_id = c.id
                WHERE t.user_id = ? AND t.status = 'open'
                ORDER BY COALESCE(t.due_date, '9999-12-31') ASC
            ''', (pid,))
            open_tasks = cur.fetchall()
            # Completed tasks (last 20)
            cur.execute('''
                SELECT t.id AS task_id, t.title, t.due_date, c.name AS customer_name, c.id AS customer_id
                FROM tasks t
                JOIN customers c ON t.customer_id = c.id
                WHERE t.user_id = ? AND t.status = 'completed'
                ORDER BY t.created_at DESC LIMIT 20
            ''', (pid,))
            done_tasks = cur.fetchall()
            # Customers linked to this user
            cur.execute('''
                SELECT c.id, c.name, c.company, c.email, c.phone, c.category
                FROM customer_users cu
                JOIN customers c ON cu.customer_id = c.id
                WHERE cu.user_id = ?
                ORDER BY c.name ASC
            ''', (pid,))
            linked_customers = cur.fetchall()
            # Recent interactions logged by this user
            cur.execute('''
                SELECT i.interaction_type, i.note, i.contact_date, i.created_at,
                       c.name AS customer_name, c.id AS customer_id
                FROM interactions i
                JOIN customers c ON i.customer_id = c.id
                WHERE i.user_id = ?
                ORDER BY COALESCE(i.contact_date, DATE(i.created_at)) DESC
                LIMIT 30
            ''', (pid,))
            interactions = cur.fetchall()
            # Notes added by this user
            cur.execute('''
                SELECT n.content, n.created_at, c.name AS customer_name, c.id AS customer_id
                FROM notes n
                JOIN customers c ON n.customer_id = c.id
                WHERE n.user_id = ?
                ORDER BY n.created_at DESC
                LIMIT 30
            ''', (pid,))
            user_notes = cur.fetchall()
            # Customers added by this user
            cur.execute('''
                SELECT id, name, company, category, created_at
                FROM customers
                WHERE created_by = ?
                ORDER BY created_at DESC
            ''', (pid,))
            added_customers = cur.fetchall()
        body = html_header(f'Profiel: {profile_user["username"]}', True, viewer_username, viewer_id)
        body += f'<h2 class="mt-4">&#128100; {html.escape(profile_user["username"])}</h2>'
        body += f'<p style="color:#666;">{html.escape(profile_user["email"])} &middot; Account aangemaakt op {profile_user["created_at"][:10]}</p>'
        # Stats row
        overdue = [t for t in open_tasks if t['due_date'] and t['due_date'] < datetime.date.today().isoformat()]
        body += f'''<div style="display:flex;gap:1rem;margin-bottom:1rem;flex-wrap:wrap;">
            <div class="card" style="flex:1;min-width:140px;text-align:center;">
                <div style="font-size:2rem;font-weight:bold;color:#c2185b;">{len(open_tasks)}</div>
                <div>Open taken</div>
            </div>
            <div class="card" style="flex:1;min-width:140px;text-align:center;">
                <div style="font-size:2rem;font-weight:bold;color:{'#dc3545' if overdue else '#388e3c'};">{len(overdue)}</div>
                <div>Verlopen taken</div>
            </div>
            <div class="card" style="flex:1;min-width:140px;text-align:center;">
                <div style="font-size:2rem;font-weight:bold;color:#1976d2;">{len(linked_customers)}</div>
                <div>Gekoppelde klanten</div>
            </div>
            <div class="card" style="flex:1;min-width:140px;text-align:center;">
                <div style="font-size:2rem;font-weight:bold;color:#7b1fa2;">{len(interactions)}</div>
                <div>Recente interacties</div>
            </div>
        </div>'''
        # Open tasks
        body += '<div class="card"><div class="section-title">Open taken</div>'
        if open_tasks:
            for t in open_tasks:
                due = t['due_date'] or '-'
                is_overdue = t['due_date'] and t['due_date'] < datetime.date.today().isoformat()
                due_color = '#dc3545' if is_overdue else '#555'
                desc = f'<br><small style="color:#666;">{html.escape(t["description"])}</small>' if t['description'] else ''
                resolve_btn = f"<a href='/tasks/resolve?id={t['task_id']}&from=users/profile' style='background:#198754;color:#fff;border-radius:4px;padding:0.15rem 0.55rem;font-size:0.8rem;text-decoration:none;margin-left:0.5rem;'>&#10003; Resolve</a>"
                body += f'''<div style="border-bottom:1px solid #eee;padding:0.5rem 0;">
                    <a href="/customers/view?id={t['customer_id']}" style="color:#c2185b;font-weight:bold;">{html.escape(t['customer_name'])}</a>
                    &mdash; {html.escape(t['title'])}{desc}
                    <span style="float:right;color:{due_color};font-size:0.85rem;">&#128197; {due} {resolve_btn}</span>
                </div>'''
        else:
            body += '<p style="color:#388e3c;">Geen open taken. &#10003;</p>'
        body += '</div>'
        # Linked customers
        body += '<div class="card"><div class="section-title">Gekoppelde klanten</div>'
        if linked_customers:
            body += '<table><thead><tr><th>Naam</th><th>Bedrijf</th><th>Type</th><th>E-mail</th><th>Telefoon</th></tr></thead><tbody>'
            for c in linked_customers:
                body += f'''<tr>
                    <td><a href="/customers/view?id={c['id']}" style="color:#c2185b;">{html.escape(c['name'])}</a></td>
                    <td>{html.escape(c['company'] or '-')}</td>
                    <td>{html.escape((c['category'] or 'klant').capitalize())}</td>
                    <td>{html.escape(c['email'])}</td>
                    <td>{html.escape(c['phone'] or '-')}</td>
                </tr>'''
            body += '</tbody></table>'
        else:
            body += '<p>Geen gekoppelde klanten.</p>'
        body += '</div>'
        # Recent interactions
        body += '<div class="card"><div class="section-title">Recente interacties</div>'
        type_labels = {'call': 'Bellen', 'email': 'E-mail', 'message': 'Bericht', 'meeting': 'Meeting'}
        if interactions:
            for i in interactions:
                date_str = i['contact_date'] or i['created_at'][:10]
                type_label = type_labels.get(i['interaction_type'], i['interaction_type'])
                note_part = f' — <em>{html.escape(i["note"])}</em>' if i['note'] else ''
                body += f'''<div style="border-bottom:1px solid #eee;padding:0.4rem 0;">
                    <small style="color:#888;">{date_str}</small>
                    <strong style="margin-left:0.5rem;">{type_label}</strong>
                    &middot; <a href="/customers/view?id={i['customer_id']}" style="color:#c2185b;">{html.escape(i['customer_name'])}</a>{note_part}
                </div>'''
        else:
            body += '<p>Nog geen interacties geregistreerd.</p>'
        body += '</div>'
        # Notes by this user (collapsible)
        body += '<details style="margin-bottom:1rem;"><summary style="cursor:pointer;font-weight:bold;padding:0.6rem 1rem;background:#fff;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">&#128221; Toegevoegde notities (' + str(len(user_notes)) + ')</summary><div class="card" style="margin-top:0.25rem;">'
        if user_notes:
            for n in user_notes:
                snippet = (n['content'][:120] + '…') if len(n['content']) > 120 else n['content']
                body += f'''<div style="border-bottom:1px solid #eee;padding:0.4rem 0;">
                    <small style="color:#888;">{n['created_at'][:10]}</small>
                    &middot; <a href="/customers/view?id={n['customer_id']}" style="color:#c2185b;">{html.escape(n['customer_name'])}</a>
                    <br><span style="color:#333;">{html.escape(snippet)}</span>
                </div>'''
        else:
            body += '<p>Nog geen notities toegevoegd.</p>'
        body += '</div></details>'
        # Customers added by this user (collapsible)
        body += '<details style="margin-bottom:1rem;"><summary style="cursor:pointer;font-weight:bold;padding:0.6rem 1rem;background:#fff;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">&#127970; Toegevoegde klanten (' + str(len(added_customers)) + ')</summary><div class="card" style="margin-top:0.25rem;">'
        if added_customers:
            for c in added_customers:
                body += f'''<div style="border-bottom:1px solid #eee;padding:0.4rem 0;">
                    <a href="/customers/view?id={c['id']}" style="color:#c2185b;font-weight:bold;">{html.escape(c['name'])}</a>
                    {(' &middot; ' + html.escape(c['company'])) if c['company'] else ''}
                    <span style="font-size:0.8rem;color:#888;float:right;">{(c['category'] or 'klant').capitalize()} &middot; {c['created_at'][:10]}</span>
                </div>'''
        else:
            body += '<p>Nog geen klanten toegevoegd.</p>'
        body += '</div></details>'
        # Completed tasks (collapsed summary)
        if done_tasks:
            body += '<details><summary style="cursor:pointer;font-weight:bold;padding:0.6rem 1rem;background:#fff;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">&#10003; Voltooide taken (' + str(len(done_tasks)) + ')</summary><div class="card" style="margin-top:0.25rem;">'
            for t in done_tasks:
                body += f'''<div style="border-bottom:1px solid #eee;padding:0.4rem 0;color:#888;">
                    &#10003; {html.escape(t['title'])} &middot;
                    <a href="/customers/view?id={t['customer_id']}" style="color:#aaa;">{html.escape(t['customer_name'])}</a>
                </div>'''
            body += '</div></details>'
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

    def render_customers(self, search: str, relation_filter: str = '', sort_col: str = 'name', sort_dir: str = 'asc') -> None:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            conditions = []
            args: list = []
            if search:
                like = f'%{search}%'
                conditions.append('(name LIKE ? OR email LIKE ? OR company LIKE ? OR tags LIKE ?)')
                args.extend([like, like, like, like])
            if relation_filter:
                conditions.append('relation_type = ?')
                args.append(relation_filter)
            where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
            safe_col = sort_col if sort_col in ('name','company','category','relation_type','created_at') else 'name'
            safe_dir = 'ASC' if sort_dir == 'asc' else 'DESC'
            cur.execute(f'SELECT * FROM customers {where} ORDER BY LOWER({safe_col}) {safe_dir}', args)
            customers = cur.fetchall()
            cur.execute('SELECT id, username FROM users ORDER BY username ASC')
            all_users_bulk = cur.fetchall()
        logged_in, _, username = self.parse_session()
        _, uid, _ = self.parse_session()
        body = html_header('Klanten', logged_in, username, uid)
        body += '<h2 class="mt-4">Klanten</h2>'
        q_enc = html.escape(search)
        # Helper: build base URL keeping current filters
        def _base_url(extra_params=''):
            parts = []
            if search: parts.append(f'q={q_enc}')
            if relation_filter: parts.append(f'relatie={relation_filter}')
            if extra_params: parts.append(extra_params)
            return '/customers' + ('?' + '&'.join(parts) if parts else '')
        # Filter buttons Intern / Extern
        def _tab(label, val):
            active = relation_filter == val
            base_style = 'display:inline-block;padding:0.35rem 1.1rem;border-radius:20px;border:2px solid #c2185b;text-decoration:none;font-size:0.9rem;margin-right:0.4rem;'
            style = base_style + ('background:#c2185b;color:#fff;font-weight:bold;' if active else 'color:#c2185b;')
            href = f'/customers?relatie={val}' + (f'&q={q_enc}' if search else '') + (f'&sort={sort_col}&dir={sort_dir}' if sort_col != 'name' or sort_dir != 'asc' else '')
            return f'<a href="{href}" style="{style}">{label}</a>'
        alle_active = not relation_filter
        alle_style = 'display:inline-block;padding:0.35rem 1.1rem;border-radius:20px;border:2px solid #c2185b;text-decoration:none;font-size:0.9rem;margin-right:0.4rem;'
        alle_style += 'background:#c2185b;color:#fff;font-weight:bold;' if alle_active else 'color:#c2185b;'
        alle_href = '/customers' + (f'?q={q_enc}' if search else '')
        filter_btns = f'<a href="{alle_href}" style="{alle_style}">Alle</a>' + _tab('Extern', 'extern') + _tab('Intern', 'intern')
        body += f'''
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.75rem;margin-top:1rem;margin-bottom:0.75rem;">
            <div>{filter_btns}</div>
            <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;">
                <form method="get" class="d-flex" role="search" style="margin:0;">
                    {'<input type="hidden" name="relatie" value="' + relation_filter + '">' if relation_filter else ''}
                    {'<input type="hidden" name="sort" value="' + sort_col + '"><input type="hidden" name="dir" value="' + sort_dir + '">' if sort_col != 'name' or sort_dir != 'asc' else ''}
                    <input class="form-control me-2" type="search" name="q" placeholder="Zoeken" value="{q_enc}" style="min-width:180px;">
                    <button class="btn btn-outline-success" type="submit">Zoek</button>
                </form>
                <a href="/customers/add" class="btn btn-primary">+ Toevoegen</a>
            </div>
        </div>'''
        # Bulk action bar
        user_opts_bulk = '<option value="">-- Kies gebruiker --</option>' + ''.join(f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in all_users_bulk)
        body += f'''<div id="bulk-bar" style="display:none;background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:0.6rem 1rem;margin-bottom:0.5rem;display:flex;gap:0.75rem;align-items:center;flex-wrap:wrap;">
            <strong id="bulk-count">0 geselecteerd</strong>
            <button type="button" onclick="bulkAction('intern')" class="btn btn-sm" style="background:#e3f0ff;color:#1565c0;border:1px solid #1565c0;">Intern</button>
            <button type="button" onclick="bulkAction('extern')" class="btn btn-sm" style="background:#f0f0f0;color:#555;border:1px solid #aaa;">Extern</button>
            <span style="color:#aaa;">|</span>
            <input type="text" id="bulk-tag-input" placeholder="Tag toevoegen..." style="padding:0.25rem 0.5rem;border:1px solid #ced4da;border-radius:4px;font-size:0.85rem;">
            <button type="button" onclick="bulkAction('add_tag')" class="btn btn-sm btn-secondary">+ Tag</button>
            <span style="color:#aaa;">|</span>
            <select id="bulk-user-select" style="padding:0.25rem 0.5rem;border:1px solid #ced4da;border-radius:4px;font-size:0.85rem;">{user_opts_bulk}</select>
            <button type="button" onclick="bulkAction('link_user')" class="btn btn-sm btn-primary">Koppel</button>
        </div>
        <form method="post" action="/customers/bulk" id="bulk-form">
            <input type="hidden" name="bulk_action" id="bulk-action-input">
            <input type="hidden" name="bulk_tag" id="bulk-tag-hidden">
            <input type="hidden" name="bulk_user_id" id="bulk-user-hidden">
        '''
        # Sort helper: build th with sort link
        def _th(label, col):
            arrow = ''
            if sort_col == col:
                arrow = ' &#9650;' if sort_dir == 'asc' else ' &#9660;'
                new_dir = 'desc' if sort_dir == 'asc' else 'asc'
            else:
                new_dir = 'asc'
            parts = [f'sort={col}', f'dir={new_dir}']
            if search: parts.append(f'q={q_enc}')
            if relation_filter: parts.append(f'relatie={relation_filter}')
            href = '/customers?' + '&'.join(parts)
            return f'<th><a href="{href}" style="color:inherit;text-decoration:none;">{label}{arrow}</a></th>'
        body += f'''<table class="table table-striped table-hover mt-1">
            <thead>
                <tr>
                    <th style="width:32px;"><input type="checkbox" id="select-all" onclick="toggleAll(this)" title="Alles selecteren"></th>
                    {_th('Naam','name')}
                    {_th('Bedrijf','company')}
                    {_th('Type','category')}
                    {_th('Relatie','relation_type')}
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
                tags_display = ', '.join([html.escape(tag.strip()) for tag in (cust['tags'] or '').split(',')]) if cust['tags'] else '-'
                category_display = (cust['category'] or 'klant').capitalize() if 'category' in cust.keys() else 'Klant'
                rel = (cust['relation_type'] or 'extern') if 'relation_type' in cust.keys() else 'extern'
                rel_color = '#1565c0' if rel == 'intern' else '#555'
                rel_label = f'<span style="background:{"#e3f0ff" if rel == "intern" else "#f0f0f0"};color:{rel_color};border-radius:12px;padding:0.15rem 0.6rem;font-size:0.82rem;font-weight:bold;">{rel.capitalize()}</span>'
                creator = '-'
                try:
                    if cust['created_by']:
                        u = get_user_by_id(cust['created_by'])
                        if u:
                            creator = html.escape(u['username'])
                except Exception:
                    creator = '-'
                body += f'''<tr>
                    <td><input type="checkbox" name="selected_ids" value="{cust['id']}" class="row-cb" onchange="updateBulk()"></td>
                    <td><a href="/customers/view?id={cust['id']}">{html.escape(cust['name'])}</a></td>
                    <td>{html.escape(cust['company'] or '-')}</td>
                    <td>{category_display}</td>
                    <td>{rel_label}</td>
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
            body += '<tr><td colspan="10" class="text-center">Geen klanten gevonden.</td></tr>'
        body += '</tbody></table></form>'
        body += '''<script>
function toggleAll(cb){document.querySelectorAll('.row-cb').forEach(c=>c.checked=cb.checked);updateBulk();}
function updateBulk(){
    var checked=document.querySelectorAll('.row-cb:checked').length;
    var bar=document.getElementById('bulk-bar');
    document.getElementById('bulk-count').textContent=checked+' geselecteerd';
    bar.style.display=checked>0?'flex':'none';
}
function bulkAction(action){
    var checked=document.querySelectorAll('.row-cb:checked');
    if(!checked.length){alert('Selecteer eerst klanten.');return;}
    document.getElementById('bulk-action-input').value=action;
    document.getElementById('bulk-tag-hidden').value=document.getElementById('bulk-tag-input').value;
    document.getElementById('bulk-user-hidden').value=document.getElementById('bulk-user-select').value;
    document.getElementById('bulk-form').submit();
}
</script>'''
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
        # Load all users and currently linked users for this customer
        with sqlite3.connect(DB_PATH) as conn_u:
            conn_u.row_factory = sqlite3.Row
            cur_u = conn_u.cursor()
            cur_u.execute('SELECT id, username FROM users ORDER BY username ASC')
            all_users = cur_u.fetchall()
        linked_ids = get_linked_user_ids(customer['id']) if customer else []
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
        relation_type = (customer.get('relation_type') or 'extern') if customer else 'extern'
        website = customer.get('website', '') if customer else ''
        industry = customer.get('industry', '') if customer else ''
        company_size = customer.get('company_size', '') if customer else ''
        region = customer.get('region', '') if customer else ''
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

        # Build user pill-toggle HTML (CSS checkbox hack: no JS needed)
        if all_users:
            pill_items = []
            for u in all_users:
                uid_val = u['id']
                uname = html.escape(u['username'])
                checked = 'checked' if uid_val in linked_ids else ''
                pill_items.append(
                    f'<span class="user-pill">'
                    f'<input type="checkbox" name="linked_users" value="{uid_val}" id="upill_{uid_val}" {checked}>'
                    f'<label for="upill_{uid_val}">{uname}</label>'
                    f'</span>'
                )
            users_checkboxes_html = ''.join(pill_items)
        else:
            users_checkboxes_html = '<em>Geen gebruikers gevonden.</em>'

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
                            <div class="mb-3">
                                <label class="form-label">Relatie</label><br>
                                <span class="user-pill">
                                    <input type="radio" name="relation_type" value="extern" id="rel_extern" {'checked' if relation_type != 'intern' else ''}>
                                    <label for="rel_extern">Extern</label>
                                </span>
                                <span class="user-pill">
                                    <input type="radio" name="relation_type" value="intern" id="rel_intern" {'checked' if relation_type == 'intern' else ''}>
                                    <label for="rel_intern">Intern</label>
                                </span>
                            </div>
                            {dynamic_fields_html}
                            <div class="mb-3">
                                <label for="custom_fields" class="form-label">Extra velden (JSON of key=value per regel)</label>
                                <textarea class="form-control" id="custom_fields" name="custom_fields" rows="3">{html.escape(raw_custom_fields)}</textarea>
                                <small class="form-text text-muted">Voer extra eigenschappen in als JSON (bijv. {{"linkedin": "http://...", "verjaardag": "2025-10-20"}}) of als key=value per regel.</small>
                            </div>
                            <div class="mb-3">
                                <label class="form-label"><strong>Accountmanagers</strong></label>
                                <style>
                                    .user-pill input[type=checkbox],.user-pill input[type=radio]{{display:none}}
                                    .user-pill label{{display:inline-block;padding:0.35rem 1rem;border-radius:20px;border:2px solid #c2185b;color:#c2185b;cursor:pointer;margin:0.25rem 0.25rem 0.25rem 0;font-size:0.9rem;transition:background 0.15s,color 0.15s}}
                                    .user-pill label:hover{{background:#fce4ec}}
                                    .user-pill input[type=checkbox]:checked+label,.user-pill input[type=radio]:checked+label{{background:#c2185b;color:#fff;font-weight:bold}}
                                </style>
                                <div style="margin-top:0.3rem;">
                                    {users_checkboxes_html}
                                </div>
                                <small class="form-text text-muted">Klik op een naam om die accountmanager te koppelen. Gekoppelde managers ontvangen na 90 dagen geen contact automatisch een herinnering.</small>
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
            # All users for task assignment dropdown
            cur.execute('SELECT id, username FROM users ORDER BY username ASC')
            all_users_for_task = cur.fetchall()
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
        # Build linked users HTML
        linked_users_html = ''
        try:
            linked_uids = get_linked_user_ids(customer['id'])
            if linked_uids:
                names = []
                for luid in linked_uids:
                    lu = get_user_by_id(luid)
                    if lu:
                        names.append(html.escape(lu['username']))
                if names:
                    linked_users_html = '<p><span class="icon">&#128101;</span>Accountmanagers: ' + ', '.join(names) + '</p>'
        except Exception:
            pass
        body += f'''<div class="card">
            <div class="section-title">Contactgegevens</div>
            <p><span class="icon">&#9993;</span>{html.escape(customer['email'])}</p>
            {f'<p><span class="icon">&#128222;</span>{html.escape(customer["phone"])} </p>' if customer['phone'] else ''}
            {f'<p><span class="icon">&#127968;</span>{html.escape(customer["address"])} </p>' if customer['address'] else ''}
            {f'<p><span class="icon">&#128188;</span>{html.escape(customer["company"])} </p>' if customer['company'] else ''}
            {tags_html}
            <p><span class="icon">&#128221;</span>Type: {category_display} &middot; {(customer.get('relation_type') or 'extern').capitalize()}</p>
            <p><span class="icon">&#128100;</span>Toegevoegd door: {creator_name}</p>
            <p><span class="icon">&#128197;</span>Aangemaakt op {customer['created_at']}</p>
            {linked_users_html}
            {custom_html}
        </div>'''
        # ----- Activity timeline (collapsible) -----
        type_icon = {'call': '📞', 'email': '📧', 'message': '💬', 'meeting': '🤝'}
        type_label_map = {'call': 'Bellen', 'email': 'E-mail', 'message': 'Bericht', 'meeting': 'Meeting'}
        timeline_items = []
        for t in tasks:
            date_val = t['due_date'] or t['created_at'][:10]
            status_col = '#388e3c' if t['status'] == 'completed' else '#f57f17'
            timeline_items.append((date_val, f'<span style="color:{status_col};">📋</span> <strong>{html.escape(t["title"])}</strong> <small style="color:#888;">(Taak · {html.escape(t["author"])} · {date_val})</small>'))
        for n in notes:
            timeline_items.append((n['created_at'][:10], f'📝 {html.escape((n["content"][:80] + "…") if len(n["content"]) > 80 else n["content"])} <small style="color:#888;">(Notitie · {html.escape(n["author"] or "")} · {n["created_at"][:10]})</small>'))
        for i in interactions:
            d = i['created_at'][:10]
            lbl = type_label_map.get(i['interaction_type'], i['interaction_type'])
            icon = type_icon.get(i['interaction_type'], '🔔')
            note_part = f' — {html.escape(i["note"])}' if i['note'] else ''
            timeline_items.append((d, f'{icon} <strong>{lbl}</strong>{note_part} <small style="color:#888;">(Interactie · {html.escape(i["author"])} · {d})</small>'))
        timeline_items.sort(key=lambda x: x[0], reverse=True)
        tl_html = ''.join(f'<div style="border-bottom:1px solid #eee;padding:0.4rem 0;">{item}</div>' for _, item in timeline_items) if timeline_items else '<p style="color:#888;">Nog geen activiteit.</p>'
        body += f'<details style="margin-bottom:0.75rem;"><summary style="cursor:pointer;font-weight:bold;padding:0.6rem 1rem;background:#fff;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">📋 Activiteitenoverzicht ({len(timeline_items)})</summary><div class="card" style="margin-top:0.25rem;">{tl_html}</div></details>'
        # ----- Tasks card -----
        # Show task error if present
        tasks_section = ''
        if task_error:
            tasks_section += f'<div class="alert alert-danger">{html.escape(task_error)}</div>'
        # Task form — build user options for assignment dropdown
        user_options = '<option value="">Mezelf</option>'
        for u in all_users_for_task:
            uname = html.escape(u['username'])
            user_options += f'<option value="{u["id"]}">{uname}</option>'
        tasks_section += f'''<form method="post" action="/tasks/add?customer_id={customer['id']}" style="margin-bottom:1rem;">
            <label>Titel<br><input type="text" name="title" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <label>Vervaldatum<br><input type="date" name="due_date" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <label>Beschrijving<br><input type="text" name="description" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <label>Toewijzen aan<br><select name="assigned_user_id" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;">{user_options}</select></label>
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
                    <div style="font-size:0.8rem; color:#666;">Aangemaakt op {task['created_at']} &middot; Toegewezen aan: <strong>{html.escape(task['author'])}</strong></div>
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
        today_str = datetime.date.today().isoformat()
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
            <label>Datum contact<br>
                <input type="date" name="contact_date" value="{today_str}" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;">
            </label>
            <small style="display:block;margin-bottom:0.4rem;color:#666;">Pas de datum aan als het contact eerder plaatsvond — de herinnering wordt dan automatisch berekend vanaf die datum.</small>
            <label>Notitie (optioneel)<br>
                <input type="text" name="note" style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
            <button type="submit" style="background-color:#c2185b; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Interactie toevoegen</button>
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
    # Start background thread that checks for 90-day reminder tasks once per day
    reminder_thread = threading.Thread(target=_reminder_loop, daemon=True)
    reminder_thread.start()
    with socketserver.TCPServer((HOST, PORT), CRMRequestHandler) as httpd:
        print(f'Starting CRM server on http://{HOST}:{PORT}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        print('Server shutting down...')


if __name__ == '__main__':
    run_server()





