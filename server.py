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

# CSRF tokens: user_id -> csrf_token (generated at login)
csrf_tokens: Dict[int, str] = {}

# Login rate limiting: IP -> (fail_count, lockout_until_timestamp)
login_lockouts: Dict[str, tuple] = {}
_MAX_LOGIN_ATTEMPTS = 5
_LOGIN_LOCKOUT_SECS = 30


def _check_login_allowed(ip: str) -> tuple:
    """Return (allowed: bool, wait_secs: int)."""
    now = time.time()
    entry = login_lockouts.get(ip)
    if not entry:
        return True, 0
    count, lockout_until = entry
    if lockout_until > now:
        return False, int(lockout_until - now)
    del login_lockouts[ip]
    return True, 0


def _record_login_failure(ip: str) -> None:
    now = time.time()
    entry = login_lockouts.get(ip)
    count = entry[0] + 1 if entry and entry[1] <= now else 1
    lockout_until = now + _LOGIN_LOCKOUT_SECS if count >= _MAX_LOGIN_ATTEMPTS else 0.0
    login_lockouts[ip] = (count, lockout_until)


def _record_login_success(ip: str) -> None:
    login_lockouts.pop(ip, None)

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
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM users')
        return cur.fetchone()[0] > 0


def is_admin(user_id: int) -> bool:
    """Check if user is admin via the is_admin column. User id=1 is always admin."""
    if user_id == 1:
        return True
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute('SELECT is_admin FROM users WHERE id = ?', (user_id,))
        row = cur.fetchone()
        return bool(row and row[0])


def is_comm_member(user_id: int) -> bool:
    """Return True if user is in the communication team or is admin."""
    if user_id is None:
        return False
    if is_admin(user_id):
        return True
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute('SELECT is_comm FROM users WHERE id = ?', (user_id,))
        row = cur.fetchone()
        return bool(row and row[0])


def is_gov_member(user_id: int) -> bool:
    """Return True if user is in the governance team or is admin."""
    if user_id is None:
        return False
    if is_admin(user_id):
        return True
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        cur = conn.cursor()
        cur.execute('SELECT is_governance FROM users WHERE id = ?', (user_id,))
        row = cur.fetchone()
        return bool(row and row[0])


def init_db() -> None:
    """Initialize the SQLite database if it doesn't already exist."""
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
        # Ensure is_admin column exists on users table
        try:
            cur.execute('SELECT is_admin FROM users LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE users ADD COLUMN is_admin INTEGER DEFAULT 0')
            cur.execute('UPDATE users SET is_admin = 1 WHERE id = 1')
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
        # Ensure role column exists (for intern contacts: Docent/Onderzoeker/etc.)
        try:
            cur.execute('SELECT role FROM customers LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE customers ADD COLUMN role TEXT")
        # Clear 'klant'/'netwerk' category for intern contacts (replaced by role field)
        cur.execute("UPDATE customers SET category=NULL WHERE relation_type='intern' AND (category='klant' OR category='netwerk')")
        # One-time migration: clean slate — delete all open tasks
        cur.execute("CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY)")
        if not cur.execute("SELECT 1 FROM _migrations WHERE name='clean_open_tasks_2026'").fetchone():
            cur.execute("DELETE FROM tasks WHERE status='open'")
            cur.execute("INSERT INTO _migrations (name) VALUES ('clean_open_tasks_2026')")
        # One-time migration: koppel Anouk aan alle interne relaties zonder accountmanager
        if not cur.execute("SELECT 1 FROM _migrations WHERE name='link_anouk_intern_2026'").fetchone():
            anouk = cur.execute("SELECT id FROM users WHERE lower(username)='anouk' LIMIT 1").fetchone()
            if anouk:
                anouk_id = anouk[0]
                cur.execute('''
                    INSERT OR IGNORE INTO customer_users (customer_id, user_id)
                    SELECT c.id, ?
                    FROM customers c
                    WHERE c.relation_type = 'intern'
                      AND NOT EXISTS (
                          SELECT 1 FROM customer_users cu WHERE cu.customer_id = c.id
                      )
                ''', (anouk_id,))
            cur.execute("INSERT INTO _migrations (name) VALUES ('link_anouk_intern_2026')")
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
        # Ensure is_comm column exists on users table (communication team flag)
        try:
            cur.execute('SELECT is_comm FROM users LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE users ADD COLUMN is_comm INTEGER DEFAULT 0')
        # Comm tasks: standalone tasks for the communication team (not linked to customers)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS comm_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'backlog',
                due_date DATE,
                assigned_to INTEGER,
                created_by INTEGER NOT NULL,
                goal_id INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (assigned_to) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (goal_id) REFERENCES comm_goals(id) ON DELETE SET NULL
            );
        ''')
        # Comm goals: team objectives with optional target date
        cur.execute('''
            CREATE TABLE IF NOT EXISTS comm_goals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                target_date DATE,
                status TEXT NOT NULL DEFAULT 'actief',
                created_by INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        # Add priority column to comm_tasks if missing
        try:
            cur.execute('SELECT priority FROM comm_tasks LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE comm_tasks ADD COLUMN priority TEXT DEFAULT 'medium'")
        # Add tags column to comm_tasks if missing
        try:
            cur.execute('SELECT tags FROM comm_tasks LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE comm_tasks ADD COLUMN tags TEXT')
        # Add reminder_note column to comm_tasks if missing
        try:
            cur.execute('SELECT reminder_note FROM comm_tasks LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE comm_tasks ADD COLUMN reminder_note TEXT')
        # Comments on comm tasks
        cur.execute('''
            CREATE TABLE IF NOT EXISTS comm_task_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                content TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (task_id) REFERENCES comm_tasks(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        # Comm dates: important dates, events, deadlines, milestones
        cur.execute('''
            CREATE TABLE IF NOT EXISTS comm_dates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                date DATE NOT NULL,
                type TEXT NOT NULL DEFAULT 'event',
                created_by INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        # Comm content calendar: content items per platform/channel
        cur.execute('''
            CREATE TABLE IF NOT EXISTS comm_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                platform TEXT DEFAULT 'overig',
                publish_date DATE,
                status TEXT NOT NULL DEFAULT 'idee',
                assigned_to INTEGER,
                created_by INTEGER NOT NULL,
                tags TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (assigned_to) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        # Comm profiles: extended profile info per comm team member
        cur.execute('''
            CREATE TABLE IF NOT EXISTS comm_profiles (
                user_id INTEGER PRIMARY KEY,
                role_title TEXT,
                bio TEXT,
                skills TEXT,
                avatar_color TEXT DEFAULT '#5C7A5A',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        # Events Gov: governance checks for events
        cur.execute('''
            CREATE TABLE IF NOT EXISTS events_gov_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT,
                event_context TEXT,
                assigned_to INTEGER,
                status TEXT NOT NULL DEFAULT 'open',
                due_date DATE,
                priority TEXT DEFAULT 'medium',
                created_by INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (assigned_to) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        # Add board_status to comm_content if missing (allows showing content on kanban board)
        try:
            cur.execute('SELECT board_status FROM comm_content LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE comm_content ADD COLUMN board_status TEXT')

        # Ensure is_governance column exists on users table (governance team flag)
        try:
            cur.execute('SELECT is_governance FROM users LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE users ADD COLUMN is_governance INTEGER DEFAULT 0')

        # Governance tables
        cur.execute('''
            CREATE TABLE IF NOT EXISTS governance_persons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phase TEXT NOT NULL DEFAULT 'startpunt',
                tags TEXT,
                notes TEXT,
                created_by INTEGER NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS governance_card_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phase TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                order_index INTEGER DEFAULT 0
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS governance_card_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                order_index INTEGER DEFAULT 0,
                FOREIGN KEY (card_id) REFERENCES governance_card_templates(id) ON DELETE CASCADE
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS governance_progress (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                completed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                completed_by INTEGER,
                UNIQUE(person_id, item_id),
                FOREIGN KEY (person_id) REFERENCES governance_persons(id) ON DELETE CASCADE,
                FOREIGN KEY (item_id) REFERENCES governance_card_items(id) ON DELETE CASCADE
            );
        ''')
        # Add norm and middelen columns to governance_card_items if missing
        try:
            cur.execute('SELECT norm FROM governance_card_items LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE governance_card_items ADD COLUMN norm TEXT')
        try:
            cur.execute('SELECT middelen FROM governance_card_items LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE governance_card_items ADD COLUMN middelen TEXT')
        # Add project_type to governance_persons if missing
        try:
            cur.execute('SELECT project_type FROM governance_persons LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE governance_persons ADD COLUMN project_type TEXT DEFAULT ''")
        # Add note to governance_progress if missing
        try:
            cur.execute('SELECT note FROM governance_progress LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE governance_progress ADD COLUMN note TEXT')
        # Add project_type to governance_card_templates if missing
        try:
            cur.execute('SELECT project_type FROM governance_card_templates LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE governance_card_templates ADD COLUMN project_type TEXT DEFAULT NULL")
            # Backfill from title: titles like "Projectkaart X: Communicatie" → project_type='communicatie'
            for pt in ['communicatie', 'werkveld', 'evenementen', 'onderwijs']:
                cur.execute("UPDATE governance_card_templates SET project_type=? WHERE lower(title) LIKE ?", (pt, f'%{pt}%'))
        # Add consent_given to governance_persons if missing
        try:
            cur.execute('SELECT consent_given FROM governance_persons LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE governance_persons ADD COLUMN consent_given INTEGER DEFAULT 0')
        # Add verbinding to customers if missing (ambassadeur, betrokken, niet betrokken)
        try:
            cur.execute('SELECT verbinding FROM customers LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute("ALTER TABLE customers ADD COLUMN verbinding TEXT")
        # Create governance_notes table if missing
        cur.execute('''
            CREATE TABLE IF NOT EXISTS governance_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id INTEGER NOT NULL,
                note_type TEXT NOT NULL DEFAULT 'coaching',
                content TEXT NOT NULL,
                created_by INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (person_id) REFERENCES governance_persons(id) ON DELETE CASCADE
            );
        ''')

        # Seed all governance card templates (one check per phase)
        _phases_items = {
            'startpunt': {
                'order': 1,
                'cards': [
                    ('Projectkaart 1: Communicatie', 1, [
                        ('Projecttitel', None, None),
                        ('Doel van het project', None, None),
                        ('Onderwijsinstelling', None, None),
                        ('Link naar AI-onboardingsbot', None, None),
                        ('Link naar het overdrachtsdocument', None, None),
                        ('Belangrijkste deliverables', None, None),
                        ('Betrokken stakeholders', None, None),
                    ]),
                    ('Projectkaart 1: Werkveld', 2, [
                        ('Projecttitel', None, None),
                        ('Doel van het project', None, None),
                        ('Onderwijsinstelling', None, None),
                        ('Link naar AI-onboardingsbot', None, None),
                        ('Belangrijkste deliverables', None, None),
                        ('Betrokken stakeholders', None, None),
                    ]),
                    ('Projectkaart 1: Evenementen', 3, [
                        ('Projecttitel', None, None),
                        ('Doel van het project', None, None),
                        ('Onderwijsinstelling', None, None),
                        ('Link naar AI-onboardingsbot', None, None),
                        ('Link naar het overdrachtsdocument', None, None),
                        ('Belangrijkste deliverables', None, None),
                        ('Betrokken stakeholders', None, None),
                    ]),
                    ('Projectkaart 1: Onderwijs', 4, [
                        ('Projecttitel', None, None),
                        ('Doel van het project', None, None),
                        ('Onderwijsinstelling', None, None),
                        ('Link naar AI-onboardingsbot', None, None),
                        ('Belangrijkste deliverables', None, None),
                        ('Betrokken stakeholders', None, None),
                    ]),
                ],
            },
            'empathize': {
                'order': 2,
                'cards': [
                    ('Projectkaart 2: Communicatie', 1, None),
                    ('Projectkaart 2: Werkveld', 2, None),
                    ('Projectkaart 2: Evenementen', 3, None),
                    ('Projectkaart 2: Onderwijs', 4, None),
                ],
                'shared_items': [
                    ('Doelen en succescriteria van deze fase zijn opgesteld',
                     'Er is een kort document (max. 1 A4) op Teams waarin de student beschrijft wat hij/zij onderzocht wil hebben, bereiken en ophalen in deze fase. Dit is afgestemd met de DS specialist.',
                     None),
                    ('Gebruikersonderzoek uitvoeren (intern en extern)',
                     'Minimaal 3 interviews met stakeholders zijn afgenomen en samengevat (met citaten of kerninzichten).',
                     'Interviewvragenlijst (HGO-format), opnames / Teams-call'),
                    ('Observaties in de praktijk zijn uitgevoerd',
                     'Minimaal één praktijkobservatie is gedocumenteerd, inclusief datum, setting, observaties en reflectie.',
                     'Observatieformulier'),
                    ('Analyse van bestaand materiaal is uitgevoerd (desk research)',
                     'Ten minste drie bestaande bronnen zijn geanalyseerd en samengevat in eigen woorden, met verwijzing naar relevante HGO-content.',
                     'HGO-kennisbank, ToekomstTV, Teams-projectarchief, HAN studie centrum'),
                    ("Empathy Mapping en Persona's zijn opgesteld",
                     "Minstens één uitgewerkte empathy map en één persona (met naam, quote, behoeften, frustraties, context) zijn gedeeld in het clusterkanaal.",
                     'Canva | Miro | PowerPoint | Word'),
                    ('Stakeholders zijn gesproken',
                     'Er is een overzicht gemaakt van alle relevante stakeholders met status van contact (gesproken, gepland, niet bereikbaar). Minimaal 3 zijn inhoudelijk gesproken.',
                     'Stakeholderoverzicht | Teams | Planner'),
                    ('Data over de fase is verzameld en verwerkt in de online werkomgeving op Teams',
                     "Alle interviews, observaties, analyses en persona's zijn opgeslagen in de juiste mapstructuur op Teams en gedeeld met het cluster.",
                     None),
                    ('Persoonlijke Teams planner aan de hand van Design Thinking is ingevuld',
                     'De student heeft in Microsoft Planner minimaal 5 taken aangemaakt per fase die gekoppeld zijn aan de DS-fases. Taken zijn voorzien van beschrijving en deadlines.',
                     'Microsoft Planner, Scrum en deconstructie'),
                    ('Progres meetings zijn ingepland met DS-specialist en mits nodig afdelingshoofd voor iedere fase van het project',
                     'De student heeft via Outlook 1 check-in gepland met DS-specialist en bij complexe projecten ook met de afdelingshoofd. Notulen/reflectie worden gedeeld in Teams. Bij de uitnodiging van de meeting zit een agenda.',
                     'Outlook Agenda, Microsoft Teams'),
                ],
            },
            'define': {
                'order': 3,
                'cards': [
                    ('Projectkaart 3: Communicatie', 1, None),
                    ('Projectkaart 3: Werkveld', 2, None),
                    ('Projectkaart 3: Evenementen', 3, None),
                    ('Projectkaart 3: Onderwijs', 4, None),
                ],
                'shared_items': [
                    ('Doelen en succescriteria van deze fase zijn opgesteld',
                     'Er is een kort document (max. 1 A4) op Teams waarin de student beschrijft wat hij/zij onderzocht wil hebben, bereiken en ophalen in deze fase. Dit is afgestemd met de DS-specialist.',
                     None),
                    ('Probleemdefinitie is geformuleerd op basis van data & inzichten',
                     'De stagiaire heeft een beargumenteerde probleemdefinitie geschreven van max. 10 regels, gebaseerd op data uit de Empathise-fase. Deze is gedeeld in Teams en terug te vinden in het projectdossier.',
                     None),
                    ('Er is een huidige schets van het proces/event en een gewenste schets van het nieuwe resultaat',
                     None,
                     'Alle data uit je empathize fase'),
                    ('Stakeholdermap is gemaakt',
                     'Er is een visuele of tekstuele stakeholdermap gemaakt met minimaal 6 actoren, inclusief rol, invloed en betrokkenheid. Deze is opgeslagen en gedeeld in Teams.',
                     'Stakeholder Canvas, Miro / Canva / PowerPoint'),
                    ('Kaders zijn bepaald vanuit HAN-koers & HGO-visie',
                     'De koppeling met ten minste één HAN-koersdoel (Slim, Schoon, Sociaal of Wereldburgerschap) en de HGO-visie is expliciet beschreven en zichtbaar in de probleemdefinitie of begeleidend verslag.',
                     'HAN Koersbeeld-document, Kennisdocument HGO'),
                    ('Probleemdefinitie is uitgewerkt met mogelijke samenwerking',
                     'De student heeft de probleemstelling gepresenteerd in een clusteroverleg of progressmeeting en heeft opties voor samenwerking benoemd. Feedback is verwerkt in een bijgewerkte versie.',
                     'Pitchdeck of poster, feedbackformulier, PowerPoint | Canva, Teams of Word'),
                    ('Data over de fase is verzameld en verwerkt in de online werkomgeving op Teams',
                     "Alle interviews, observaties, analyses en persona's zijn opgeslagen in de juiste mapstructuur op Teams en gedeeld met het cluster.",
                     None),
                    ('Het eerste school product is ingeleverd',
                     'Als school een plan van aanpak, projectplan, methodiek, draaiboek etc is gemaakt en ingeleverd.',
                     'Alle data van de empathise en define fases van DS'),
                ],
            },
            'ideate': {
                'order': 4,
                'cards': [
                    ('Projectkaart 4: Communicatie', 1, None),
                    ('Projectkaart 4: Werkveld', 2, None),
                    ('Projectkaart 4: Evenementen', 3, None),
                    ('Projectkaart 4: Onderwijs', 4, None),
                ],
                'shared_items': [
                    ('Doelen en succescriteria van deze fase zijn opgesteld',
                     'Er is een kort document (max. 1 A4) op Teams waarin de student beschrijft wat hij/zij onderzocht wil hebben, bereiken en ophalen in deze fase. Dit is afgestemd met de DS-specialist.',
                     None),
                    ('Creatieve brainstormsessie is georganiseerd',
                     "Er is minstens één brainstormsessie gefaciliteerd met teamleden of stakeholders, en de output hiervan (bv. notities, post-its, whiteboardfoto's) is vastgelegd.",
                     "Miro | Mural | Canva | Whiteboardfoto's, Werkvormen als SCAMPER, Crazy 8's, Brainwriting"),
                    ('Er zijn minimaal 3 oplossingsrichtingen/ontwerpen geschetst (divergent denken)',
                     'De student heeft drie verschillende concepten uitgewerkt, ieder met een korte beschrijving en/of visuele ondersteuning (tekening, schema, storyboard).',
                     'PowerPoint | Canva | Sketch | AI-generated visuals | Word | Designcanvas'),
                    ("Er is een feedbackronde georganiseerd ('How Might We' / Prototype Feedback)",
                     "De student heeft feedback opgehaald op de gegenereerde ideeën via interviews, peer-review of clusterpresentatie, en deze feedback is verwerkt in een overzicht.",
                     'Feedbackformulier | Miro-board | Teamoverlegnotities | Reflectieverslag'),
                    ('Het kernidee is gekozen en beargumenteerd',
                     'De student heeft een onderbouwde keuze gemaakt voor één oplossingsrichting, met een korte beargumentering (impact, haalbaarheid, aansluiting op koers & visie).',
                     'Impactmatrix | SWOT | Besliscanvas | Pitchdeck of Word-document met argumentatie'),
                    ('Data over de fase is verzameld en verwerkt in de online werkomgeving op Teams',
                     "Alle interviews, observaties, analyses en persona's zijn opgeslagen in de juiste mapstructuur op Teams en gedeeld met het cluster.",
                     None),
                ],
            },
            'prototype': {
                'order': 5,
                'cards': [
                    ('Projectkaart 5: Communicatie', 1, None),
                    ('Projectkaart 5: Werkveld', 2, None),
                    ('Projectkaart 5: Evenementen', 3, None),
                    ('Projectkaart 5: Onderwijs', 4, None),
                ],
                'shared_items': [
                    ('Doelen en succescriteria van deze fase zijn opgesteld',
                     'Er is een kort document (max. 1 A4) op Teams waarin de stagiair beschrijft wat hij/zij onderzocht wil hebben, bereiken en ophalen in deze fase. Dit is afgestemd met de DS-specialist.',
                     None),
                    ('Kern(functionaliteit) zijn gekozen',
                     'De stagiair heeft één duidelijke focus geformuleerd: welk onderdeel of gedrag moet getest worden? Dit is vastgelegd in het prototypeplan.',
                     None),
                    ('Schetsmatig prototype zijn gemaakt',
                     'Er is een eerste ruwe uitwerking gemaakt van het prototype (bv. op papier, in PowerPoint of via een visuele tool), inclusief uitleg van het doel.',
                     "Miro | Canva | Figma | PowerPoint | Foto's van schetsen | Word"),
                    ('Prototype is gebouwd (indien passend)',
                     'Indien relevant, is een werkende versie van het prototype gemaakt (bv. werkende AI-tool, interactieve lesvorm, gameconcept, communicatie-uiting).',
                     "GPT / Gemini | Figma | Canva | Workshopformat | Digitale toolomgeving | Custom GPT's (via Seyma)"),
                    ('Prototype is geïntegreerd in HGO-context',
                     'Het prototype sluit aan bij bestaande structuren binnen HGO (bijv. tools, formats, games, events). Er is overleg geweest met een clusterlead of coach over inbedding.',
                     'Productportfolio HGO | Clusteroverleg | Feedback van coach of lead | Beschrijving in Word/Teams'),
                    ('Testopzet is voorbereid',
                     'Er is een korte testopzet uitgewerkt met testdoel, doelgroep, manier van testen en verwachte output. Deze is opgeslagen op Teams.',
                     'Testplan-sjabloon, Word | Notion | Canva/PowerPoint voor visuals | Checklist observatie'),
                    ('Test is uitgevoerd en vastgelegd',
                     'Het prototype is getest met minimaal 1 echte gebruiker of groep. Data (observaties, reacties, scores) is verzameld en opgeslagen.',
                     "Observatieformulieren, feedbackformulieren, foto's/video, verslag in Word of Notion, Teams-opslag"),
                    ('Reflectie en doorontwikkeling is gemaakt',
                     'De stagiair heeft een korte reflectie geschreven over wat werkte, wat niet, en wat moet worden aangepast in de volgende fase.',
                     'Reflectiesjabloon, Word | Notion | AI (GPT) voor samenvattende analyse van testdata'),
                    ('Data over de fase is verzameld en verwerkt in de online werkomgeving op Teams',
                     "Alle interviews, observaties, analyses en persona's zijn opgeslagen in de juiste mapstructuur op Teams en gedeeld met het cluster.",
                     None),
                ],
            },
            'test': {
                'order': 6,
                'cards': [
                    ('Projectkaart 6: Communicatie', 1, None),
                    ('Projectkaart 6: Werkveld', 2, None),
                    ('Projectkaart 6: Evenementen', 3, None),
                    ('Projectkaart 6: Onderwijs', 4, None),
                ],
                'shared_items': [
                    ('Doelen en succescriteria van deze fase zijn opgesteld',
                     'Er is een kort document (max. 1 A4) op Teams waarin de student beschrijft wat hij/zij onderzocht wil hebben, bereiken en ophalen in deze fase. Dit is afgestemd met de DS-specialist.',
                     None),
                    ('Pilotplan/draaiboek is opgesteld',
                     'Er is een uitgewerkt plan met doelgroep, opzet, meetdoelen, feedbackmethodes en planning. Dit plan is gedeeld in Teams én besproken met een begeleider of partner.',
                     'Pilotplan-sjabloon, Word/Notion, Teams-opslag | Event-draaiboek (indien van toepassing)'),
                    ('Partners en stakeholders zijn betrokken',
                     'Minstens 2 relevante partners zijn actief betrokken bij de voorbereiding of uitvoering van de pilot. Er is vastgelegd wie wat doet, en communicatie is verlopen via mail, Teams of overleg.',
                     'Stakeholderoverzicht, Communicatielog, e-mailarchief, afspraken in Outlook'),
                    ('De pilot/event is georganiseerd en uitgevoerd',
                     "De pilot of testmoment is daadwerkelijk uitgevoerd. Er is bewijs (foto's, deelnemerslijst, observaties, feedback) opgeslagen in Teams. Reflectie op verloop is toegevoegd.",
                     "Draaiboek | Planning in Teams/Outlook | Foto's/video | Observatieformulieren | Feedbackformulieren"),
                    ('Communicatie- en borgingplan is opgesteld en gedeeld',
                     'De student heeft een voorstel geschreven over hoe het resultaat geborgd kan worden binnen de HGO (in portfolio, aanbod, kennisbank of als vervolgproject), inclusief communicatie naar het team.',
                     'Borgingsplan-sjabloon, PowerPoint/Canva voor interne presentatie, Word-document | Optioneel bespreking in clusteroverleg'),
                    ('Data over de fase is verzameld en verwerkt in de online werkomgeving op Teams',
                     "Alle interviews, observaties, analyses en persona's zijn opgeslagen in de juiste mapstructuur op Teams en gedeeld met het cluster.",
                     None),
                ],
            },
            'uittreden': {
                'order': 7,
                'cards': [
                    ('Projectkaart 7: Communicatie', 1, None),
                    ('Projectkaart 7: Werkveld', 2, None),
                    ('Projectkaart 7: Evenementen', 3, None),
                    ('Projectkaart 7: Onderwijs', 4, None),
                ],
                'shared_items': [
                    ('Definitieve producten of resultaten zijn opgeleverd', None, None),
                    ('Resultaat is geborgd volgens opgesteld borgingsplan', None, None),
                    ('Gereflecteerd op het resultaat en de impact', None, None),
                    ('Gereflecteerd op het proces en de samenwerking', None, None),
                ],
            },
        }
        for phase_key, phase_data in _phases_items.items():
            cur.execute('SELECT COUNT(*) FROM governance_card_templates WHERE phase=?', (phase_key,))
            if cur.fetchone()[0] == 0:
                cards = phase_data['cards']
                shared = phase_data.get('shared_items')
                for card_title, card_order, card_items in cards:
                    # Derive project_type from title (e.g. "Projectkaart 1: Communicatie")
                    _pt = None
                    for _t in ['communicatie', 'werkveld', 'evenementen', 'onderwijs']:
                        if _t in card_title.lower():
                            _pt = _t
                            break
                    cur.execute('INSERT INTO governance_card_templates (title, phase, order_index, project_type) VALUES (?, ?, ?, ?)',
                                (card_title, phase_key, card_order, _pt))
                    card_id = cur.lastrowid
                    items_to_use = card_items if card_items is not None else shared
                    for i, item_data in enumerate(items_to_use):
                        item_title, item_norm, item_middelen = item_data
                        cur.execute('INSERT INTO governance_card_items (card_id, title, norm, middelen, order_index) VALUES (?, ?, ?, ?, ?)',
                                    (card_id, item_title, item_norm, item_middelen, i))

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
        # Add reminder_paused_until to customer_users if missing.
        # When a reminder task is deleted, this is set to the reminder due date
        # so check_and_create_reminders() won't recreate it before that date.
        try:
            cur.execute('SELECT reminder_paused_until FROM customer_users LIMIT 1')
        except sqlite3.OperationalError:
            cur.execute('ALTER TABLE customer_users ADD COLUMN reminder_paused_until DATE')

        conn.commit()


def get_user_by_username_or_email(identifier: str) -> Optional[Dict[str, Any]]:
    """Retrieve a user record by username or email."""
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            'SELECT * FROM users WHERE username = ? OR email = ?',
            (identifier, identifier)
        )
        row = cur.fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('SELECT * FROM customer_fields ORDER BY id ASC')
        return cur.fetchall()


def get_linked_user_ids(customer_id: int) -> List[int]:
    """Return the list of user IDs linked to a customer."""
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('''
            SELECT cu.customer_id, cu.user_id, cu.reminder_paused_until,
                   c.name AS customer_name,
                   COALESCE(c.relation_type, 'extern') AS relation_type
            FROM customer_users cu
            JOIN customers c ON cu.customer_id = c.id
        ''')
        links = cur.fetchall()
        for link in links:
            cid = link['customer_id']
            uid = link['user_id']
            customer_name = link['customer_name']
            # Skip if reminder was manually dismissed until a future date
            paused_until = link['reminder_paused_until'] if 'reminder_paused_until' in link.keys() else None
            if paused_until and paused_until >= datetime.date.today().isoformat():
                continue
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
            reminder_days = 180 if link['relation_type'] == 'intern' else 60
            reminder_due = last_dt + datetime.timedelta(days=reminder_days)
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
    time.sleep(86400)  # Wacht eerst 24 uur — zo komen herinneringen niet direct terug na herstart
    while True:
        try:
            check_and_create_reminders()
        except Exception as e:
            print(f'[Reminder] Fout bij controleren herinneringen: {e}')
        time.sleep(86400)  # 24 uur


def create_user(username: str, email: str, password: str) -> Tuple[bool, str]:
    """Attempt to create a new user. Returns (success, message)."""
    with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    *, *::before, *::after { box-sizing: border-box; }
    body { margin: 0; font-family: Inter, Arial, sans-serif; background-color: #F7F4F0; color: #1C1713; padding-top: 56px; font-size: 0.9rem; }
    .navbar { background-color: #5C7A5A; color: #fff; position: fixed; top: 0; width: 100%; height: 56px; display: flex; align-items: center; padding: 0 1.25rem; box-shadow: 0 1px 3px rgba(0,0,0,0.12); z-index: 1000; }
    .navbar a { color: rgba(255,255,255,0.9); text-decoration: none; margin-right: 1rem; font-weight: 500; font-size: 0.875rem; }
    .navbar a:hover { color: #fff; }
    .navbar .spacer { flex-grow: 1; }
    .container { max-width: 980px; margin: 0 auto; padding: 1.25rem 1rem; }
    .card { background-color: #fff; border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem; box-shadow: 0 1px 3px rgba(0,0,0,0.06); border: 1px solid #E4DDD6; }
    .section-title { font-size: 1rem; font-weight: 600; margin-bottom: 0.75rem; color: #1C1713; }
    .action-buttons a { display: inline-block; border: 1.5px solid #5C7A5A; border-radius: 20px; padding: 0.3rem 0.85rem; color: #5C7A5A; text-decoration: none; margin-right: 0.5rem; font-size: 0.85rem; font-weight: 500; }
    .action-buttons a:hover { background-color: #5C7A5A; color: #fff; }
    .icon { margin-right: 0.5rem; }
    table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; }
    th, td { padding: 0.6rem 0.75rem; text-align: left; border-bottom: 1px solid #E4DDD6; }
    th { background-color: #F7F4F0; font-weight: 600; color: #7A6E66; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; }
    tr:hover td { background-color: #faf8f5; }
    .text-end { text-align: right; }
    .btn { display: inline-block; padding: 0.35rem 0.85rem; border: none; border-radius: 7px; font-size: 0.875rem; font-weight: 500; cursor: pointer; text-decoration: none; font-family: inherit; }
    .btn-primary { background-color: #5C7A5A; color: #fff; }
    .btn-secondary { background-color: #7A6E66; color: #fff; }
    .btn-danger { background-color: #C0392B; color: #fff; }
    .btn-sm { font-size: 0.8rem; padding: 0.2rem 0.6rem; }
    .form-control { padding: 0.45rem 0.7rem; border: 1px solid #E4DDD6; border-radius: 7px; width: 100%; font-family: inherit; font-size: 0.9rem; background: #fff; color: #1C1713; outline: none; }
    .form-control:focus { border-color: #5C7A5A; box-shadow: 0 0 0 3px rgba(92,122,90,0.12); }
    .btn-outline-success { border: 1.5px solid #198754; color: #198754; background: transparent; border-radius: 7px; padding: 0.3rem 0.7rem; }
    .btn-outline-success:hover { background-color: #198754; color: #fff; }
    .d-flex { display: flex; }
    .me-2 { margin-right: 0.5rem; }
    a { color: #5C7A5A; }
    a:hover { color: #4a6348; }
    '''
    # Determine navigation links based on login state.  We omit the
    # registration link unless there are no users yet; see users_exist() below.
    if logged_in:
        try:
            uid_int = int(user_id) if user_id is not None else None
        except Exception:
            uid_int = None
        _is_admin    = uid_int is not None and is_admin(uid_int)
        _is_comm     = uid_int is not None and is_comm_member(uid_int)
        _is_gov      = uid_int is not None and is_gov_member(uid_int)
        _restricted  = (_is_comm or _is_gov) and not _is_admin

        crm_links = ["<a href='/dashboard'>Dashboard</a>", "<a href='/customers'>Klanten</a>"]
        if _is_admin:
            crm_links.append("<a href='/users'>Gebruikers</a>")
            crm_links.append("<a href='/fields'>Velden</a>")
            crm_links.append("<a href='/reports'>Rapporten</a>")
        crm_links.append("<a href='/import'>Importeren</a>")
        crm_links.append("<a href='/tasks/search'>Taken zoeken</a>")

        if _restricted:
            # Restricted gebruikers: toon comm/gov links; CRM verbergbaar via toggle
            restricted_links = ''
            if _is_comm:
                restricted_links += "<a href='/comm/board'>&#128101; Comm</a>"
            if _is_gov:
                restricted_links += "<a href='/gov/board'>&#9881; Gov</a>"
            nav_links_left = (
                restricted_links
                + f"<span id='crm-nav-links' style='display:none;'>{''.join(crm_links)}</span>"
                + "<button id='crm-toggle-btn' onclick='toggleCRM()' "
                + "style='background:rgba(255,255,255,0.15);border:1px solid rgba(255,255,255,0.45);"
                + "color:#fff;border-radius:4px;padding:0.15rem 0.55rem;cursor:pointer;"
                + "font-size:0.8rem;margin-left:0.4rem;'>&#128279; CRM</button>"
            )
        else:
            nav_links_left = ''.join(crm_links)
            if _is_comm:
                nav_links_left += "<a href='/comm/board'>&#128101; Comm</a>"
            if _is_gov:
                nav_links_left += "<a href='/gov/board'>&#9881; Gov</a>"

        profile_link = f"<a href='/users/profile?id={user_id}'>Mijn profiel</a>" if user_id else ''
        nav_links_right = f"{profile_link} <span style='color:rgba(255,255,255,0.6)'>|</span> <span>Ingelogd als {html.escape(username)}</span> <a href='/account/password'>&#128273; Wachtwoord</a> <a href='/logout'>Uitloggen</a>"
        nav_search = '''<form method="get" action="/customers" style="display:flex;align-items:center;margin:0 1rem;">
            <input type="search" name="q" placeholder="&#128269; Klant zoeken..." style="padding:0.25rem 0.6rem;border:none;border-radius:4px 0 0 4px;font-size:0.85rem;width:160px;outline:none;">
            <button type="submit" style="padding:0.25rem 0.6rem;background:#4a6348;color:#fff;border:none;border-radius:0 4px 4px 0;cursor:pointer;font-size:0.85rem;">&#10132;</button>
        </form>'''
        popup_html = '''<div id="msg-popup" style="display:none;position:fixed;bottom:1.5rem;right:1.5rem;z-index:9999;background:#fff;border-radius:10px;box-shadow:0 4px 20px rgba(0,0,0,0.2);padding:1rem 1.2rem;min-width:280px;max-width:360px;border-left:4px solid #5C7A5A;">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.3rem;">
    <strong style="color:#5C7A5A;">&#128172; Nieuw bericht van <span id="msg-popup-from"></span></strong>
    <button onclick="closeMsgPopup()" style="background:none;border:none;cursor:pointer;font-size:1.1rem;color:#aaa;">&#10005;</button>
  </div>
  <div id="msg-popup-text" style="font-size:0.9rem;color:#333;margin-bottom:0.6rem;"></div>
  <a id="msg-popup-link" href="/messages" style="background:#5C7A5A;color:#fff;border-radius:4px;padding:0.25rem 0.8rem;text-decoration:none;font-size:0.85rem;">Bekijken</a>
</div>'''
    else:
        nav_links_left = ''
        nav_links_right = "<a href='/login'>Inloggen</a>"
        nav_search = ''
        popup_html = ''
    polling_js = '''<script>
(function() {
    var lastUnread = -1;
    function pollMessages() {
        fetch('/messages/poll')
            .then(function(r){return r.json();})
            .then(function(data){
                var badge = document.getElementById('msg-badge');
                if (badge) {
                    if (data.unread > 0) { badge.textContent = data.unread; badge.style.display = 'inline-block'; }
                    else { badge.style.display = 'none'; }
                }
                if (lastUnread >= 0 && data.unread > lastUnread && data.latest) { showMsgPopup(data.latest); }
                lastUnread = data.unread;
            }).catch(function(){});
    }
    function playMsgSound() {
        try {
            var ctx = new (window.AudioContext || window.webkitAudioContext)();
            var o = ctx.createOscillator();
            var g = ctx.createGain();
            o.connect(g); g.connect(ctx.destination);
            o.type = 'sine'; o.frequency.value = 880;
            g.gain.setValueAtTime(0.3, ctx.currentTime);
            g.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.4);
            o.start(ctx.currentTime); o.stop(ctx.currentTime + 0.4);
        } catch(e) {}
    }
    function showMsgPopup(msg) {
        var popup = document.getElementById('msg-popup');
        if (!popup) return;
        document.getElementById('msg-popup-from').textContent = msg.from;
        document.getElementById('msg-popup-text').textContent = msg.content.length > 100 ? msg.content.substring(0,100)+'...' : msg.content;
        document.getElementById('msg-popup-link').href = '/messages/conversation?with=' + msg.sender_id;
        popup.style.display = 'block';
        playMsgSound();
        clearTimeout(window._msgPopupTimer);
        window._msgPopupTimer = setTimeout(function(){ popup.style.display='none'; }, 8000);
    }
    window.closeMsgPopup = function() {
        var p = document.getElementById('msg-popup');
        if (p) p.style.display = 'none';
        clearTimeout(window._msgPopupTimer);
    };
    pollMessages();
    setInterval(pollMessages, 5000);
})();
</script>''' if logged_in and user_id else ''
    return f'''<!doctype html>
<html lang="nl">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)}</title>
    <style>{styles}</style>
    <script>
    function toggleCRM() {{
        var el = document.getElementById('crm-nav-links');
        var btn = document.getElementById('crm-toggle-btn');
        if (!el) return;
        var showing = el.style.display !== 'none';
        el.style.display = showing ? 'none' : 'inline';
        btn.innerHTML = showing ? '&#128279; CRM' : '&#10005; CRM';
        localStorage.setItem('crmNavOpen', showing ? '0' : '1');
    }}
    document.addEventListener('DOMContentLoaded', function() {{
        if (localStorage.getItem('crmNavOpen') === '1') {{
            var el = document.getElementById('crm-nav-links');
            var btn = document.getElementById('crm-toggle-btn');
            if (el) {{ el.style.display = 'inline'; btn.innerHTML = '&#10005; CRM'; }}
        }}
    }});
    </script>
</head>
<body>
{popup_html}
{polling_js}
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
                if not is_admin(user_id) and is_comm_member(user_id):
                    self.respond_redirect('/comm/board')
                elif not is_admin(user_id) and is_gov_member(user_id):
                    self.respond_redirect('/gov/board')
                else:
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
                client_ip = self.headers.get('X-Forwarded-For', self.client_address[0]).split(',')[0].strip()
                allowed, wait_secs = _check_login_allowed(client_ip)
                if not allowed:
                    self.render_login(error=f'Te veel mislukte pogingen. Wacht {wait_secs} seconden.')
                    return
                user = verify_user(identifier, password_f)
                if user:
                    _record_login_success(client_ip)
                    session_id = secrets.token_hex(16)
                    sessions[session_id] = user['id']
                    csrf_tokens[user['id']] = secrets.token_hex(32)
                    if not is_admin(user['id']) and is_comm_member(user['id']):
                        dest = '/comm/board'
                    elif not is_admin(user['id']) and is_gov_member(user['id']):
                        dest = '/gov/board'
                    else:
                        dest = '/dashboard'
                    self.send_response(302)
                    self.send_header('Location', dest)
                    self.send_header(
                        'Set-Cookie',
                        f'session_id={session_id}; Path=/; HttpOnly; Secure; SameSite=Lax'
                    )
                    self.end_headers()
                else:
                    _record_login_failure(client_ip)
                    self.render_login(error='Ongeldige inloggegevens.')
            else:
                self.render_login()
        elif path == '/account/password':
            if not logged_in:
                self.respond_redirect('/login')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                if not self._csrf_ok(params, user_id):
                    self._send_html(html_header('Fout', True, username, user_id) + '<div class="container"><p style="color:#dc3545;">Ongeldige sessie. Laad de pagina opnieuw.</p></div>' + html_footer())
                    return
                current_pw = params.get('current_password', [''])[0]
                new_pw = params.get('new_password', [''])[0]
                confirm_pw = params.get('confirm_password', [''])[0]
                error = None
                if not current_pw or not new_pw or not confirm_pw:
                    error = 'Vul alle velden in.'
                elif new_pw != confirm_pw:
                    error = 'Nieuw wachtwoord en bevestiging komen niet overeen.'
                elif len(new_pw) < 6:
                    error = 'Nieuw wachtwoord moet minimaal 6 tekens zijn.'
                else:
                    # Verify current password
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        conn.row_factory = sqlite3.Row
                        cur = conn.cursor()
                        cur.execute('SELECT password FROM users WHERE id=?', (user_id,))
                        row = cur.fetchone()
                    if not row:
                        error = 'Gebruiker niet gevonden.'
                    else:
                        stored = row['password']
                        if '$' in stored:
                            salt, stored_hash = stored.split('$', 1)
                        else:
                            salt, stored_hash = '', stored
                        check_hash = hashlib.sha256((salt + current_pw).encode('utf-8')).hexdigest()
                        if check_hash != stored_hash:
                            error = 'Huidig wachtwoord is onjuist.'
                if error:
                    body = html_header('Wachtwoord wijzigen', True, username, user_id)
                    body += f'<h2 class="mt-4">&#128273; Wachtwoord wijzigen</h2>'
                    body += f'<div class="alert" style="background:#fdecea;color:#c62828;border-radius:6px;padding:0.6rem 1rem;margin-bottom:0.75rem;">{html.escape(error)}</div>'
                    body += self._render_password_form(user_id)
                    body += html_footer()
                    self._send_html(body)
                else:
                    # Save new password
                    new_salt = hashlib.sha256(str(user_id).encode()).hexdigest()[:16]
                    new_hash = hashlib.sha256((new_salt + new_pw).encode('utf-8')).hexdigest()
                    new_stored = f'{new_salt}${new_hash}'
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE users SET password=? WHERE id=?', (new_stored, user_id))
                        conn.commit()
                    body = html_header('Wachtwoord gewijzigd', True, username, user_id)
                    body += f'<h2 class="mt-4">&#128273; Wachtwoord wijzigen</h2>'
                    body += '<div class="alert" style="background:#e8f5e9;color:#2e7d32;border-radius:6px;padding:0.6rem 1rem;margin-bottom:0.75rem;">&#10003; Wachtwoord succesvol gewijzigd.</div>'
                    body += html_footer()
                    self._send_html(body)
            else:
                body = html_header('Wachtwoord wijzigen', True, username, user_id)
                body += f'<h2 class="mt-4">&#128273; Wachtwoord wijzigen</h2>'
                body += self._render_password_form(user_id)
                body += html_footer()
                self._send_html(body)
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
                'session_id=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax'
            )
            self.end_headers()
        elif path == '/messages/poll':
            if not logged_in:
                self._send_json({'unread': 0, 'latest': None})
                return
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('SELECT COUNT(*) FROM messages WHERE recipient_id=? AND is_read=0', (user_id,))
                unread = cur.fetchone()[0]
                cur.execute('''SELECT m.sender_id, m.content, u.username AS from_user
                               FROM messages m JOIN users u ON m.sender_id=u.id
                               WHERE m.recipient_id=? AND m.is_read=0
                               ORDER BY m.created_at DESC LIMIT 1''', (user_id,))
                row = cur.fetchone()
                latest = {'sender_id': row['sender_id'], 'from': row['from_user'], 'content': row['content']} if row else None
            self._send_json({'unread': unread, 'latest': latest})
        elif path == '/messages':
            if not logged_in:
                self.respond_redirect('/login')
                return
            self.render_conversations(user_id, username)
        elif path == '/messages/conversation':
            if not logged_in:
                self.respond_redirect('/login')
                return
            other_id_str = query_params.get('with', [None])[0]
            if not other_id_str:
                self.respond_redirect('/messages')
                return
            try:
                other_id = int(other_id_str)
            except ValueError:
                self.respond_redirect('/messages')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                content = params.get('content', [''])[0].strip()
                reply_to_str = params.get('reply_to', [''])[0].strip()
                reply_to = int(reply_to_str) if reply_to_str.isdigit() else None
                if content:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        conn.execute('INSERT INTO messages (sender_id, recipient_id, content, reply_to) VALUES (?,?,?,?)',
                                     (user_id, other_id, content, reply_to))
                        conn.commit()
                self.respond_redirect(f'/messages/conversation?with={other_id}')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.execute('UPDATE messages SET is_read=1 WHERE sender_id=? AND recipient_id=? AND is_read=0',
                                 (other_id, user_id))
                    conn.commit()
                self.render_conversation(user_id, username, other_id)
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
            if sort_col not in ('name', 'company', 'category', 'relation_type', 'created_at', 'role', 'verbinding'):
                sort_col = 'name'
            if sort_dir not in ('asc', 'desc'):
                sort_dir = 'asc'
            verbinding_filter = query_params.get('verbinding', [''])[0].strip()
            if verbinding_filter not in ('ambassadeur', 'betrokken', 'niet betrokken'):
                verbinding_filter = ''
            self.render_customers(search, relation_filter, sort_col, sort_dir, verbinding_filter)
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
                role = params.get('role', [''])[0].strip() or None
                verbinding = params.get('verbinding', [''])[0].strip() or None
                if verbinding not in ('ambassadeur', 'betrokken', 'niet betrokken'):
                    verbinding = None
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT id FROM customers WHERE email = ?', (email_c,))
                    if cur.fetchone():
                        self.render_customer_form(None, error='Er bestaat al een klant met dit e‑mailadres.')
                        return
                    cur.execute('''INSERT INTO customers (name, email, phone, address, company, tags, category, relation_type, role, verbinding, created_by, custom_fields) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                (name,
                                 email_c,
                                 phone or None,
                                 address or None,
                                 company or None,
                                 tags or None,
                                 category,
                                 relation_type,
                                 role,
                                 verbinding,
                                 user_id,
                                 custom_fields))
                    cid_new = cur.lastrowid
                    conn.commit()
                # Log the creation
                log_action(user_id, 'create', 'customers', cid_new, f"name={name}")
                # Save customer-user links (many-to-many)
                linked_user_ids = params.get('linked_users', [])
                if linked_user_ids:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn2:
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
                role = params.get('role', [''])[0].strip() or None
                verbinding = params.get('verbinding', [''])[0].strip() or None
                if verbinding not in ('ambassadeur', 'betrokken', 'niet betrokken'):
                    verbinding = None
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                                        role=?,
                                        verbinding=?,
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
                                 role,
                                 verbinding,
                                 custom_fields,
                                 cid_int))
                    conn.commit()
                # Log the update
                log_action(user_id, 'update', 'customers', cid_int, f"name={name}")
                # Update customer-user links: replace existing with new selection
                linked_user_ids = params.get('linked_users', [])
                with sqlite3.connect(DB_PATH, timeout=10) as conn2:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
        elif path == '/customers/bulk-link-empty':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/customers')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                params = urllib.parse.parse_qs(self.rfile.read(length).decode('utf-8'))
                try:
                    target_uid = int(params.get('user_id', [''])[0])
                except (ValueError, IndexError):
                    self.respond_redirect('/customers')
                    return
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.execute('''
                        INSERT OR IGNORE INTO customer_users (customer_id, user_id)
                        SELECT c.id, ?
                        FROM customers c
                        WHERE NOT EXISTS (
                            SELECT 1 FROM customer_users cu WHERE cu.customer_id = c.id
                        )
                    ''', (target_uid,))
                    conn.commit()
            self.respond_redirect('/customers')
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    if action in ('intern', 'extern'):
                        cur.executemany('UPDATE customers SET relation_type=? WHERE id=?',
                                        [(action, cid_b) for cid_b in cid_list])
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
                                cur.execute('UPDATE customers SET tags=? WHERE id=?',
                                            (','.join(tags_list), cid_b))
                    elif action == 'link_user':
                        uid_val = params.get('bulk_user_id', [''])[0].strip()
                        if uid_val.isdigit():
                            uid_int_b = int(uid_val)
                            cur.executemany(
                                'INSERT OR IGNORE INTO customer_users (customer_id, user_id) VALUES (?,?)',
                                [(cid_b, uid_int_b) for cid_b in cid_list]
                            )
                    conn.commit()
                # One summary audit log entry instead of one per row
                log_action(user_id, 'update', 'customers', None,
                           f'bulk {action} on {len(cid_list)} customers')
                # Reminders worden bijgewerkt door de dagelijkse scheduler (niet hier — te zwaar)
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
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                cur = conn.cursor()
                # If this is a reminder task, pause it until its due date
                cur.execute("SELECT customer_id, user_id, due_date, title FROM tasks WHERE id=?", (tid_int,))
                t_row = cur.fetchone()
                if t_row and t_row[3] and t_row[3].startswith('Herinnering:'):
                    paused = t_row[2] if t_row[2] else '9999-12-31'
                    cur.execute("UPDATE customer_users SET reminder_paused_until=? WHERE customer_id=? AND user_id=?",
                                (paused, t_row[0], t_row[1]))
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                if filter_status == 'verlopen':
                    conditions.append("t.status = 'open' AND t.due_date < DATE('now')")
                elif filter_status in ('open', 'completed'):
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
            stat_opts = f'<option value="">Alle statussen</option><option value="open" {"selected" if filter_status=="open" else ""}>Open</option><option value="verlopen" {"selected" if filter_status=="verlopen" else ""}>Verlopen</option><option value="completed" {"selected" if filter_status=="completed" else ""}>Voltooid</option>'
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
                    <a href="/tasks/search" style="color:#5C7A5A;font-size:0.9rem;padding:0.4rem 0;">Wis filter</a>
                </form>
            </div>'''
            if is_admin(user_id):
                body += '''<div style="margin-top:0.75rem;display:flex;gap:0.5rem;justify-content:flex-end;flex-wrap:wrap;">
                    <form method="POST" action="/tasks/delete-all-open" onsubmit="return confirm('Alle openstaande taken verwijderen? Dit kan niet ongedaan worden.');">
                        <button type="submit" style="background:#dc3545;color:#fff;border:none;border-radius:4px;padding:0.35rem 1rem;font-size:0.85rem;cursor:pointer;">&#128465; Alle open taken verwijderen</button>
                    </form>
                    <form method="POST" action="/tasks/delete-overdue" onsubmit="return confirm('Alle verlopen taken verwijderen? Dit kan niet ongedaan worden.');">
                        <button type="submit" style="background:#b71c1c;color:#fff;border:none;border-radius:4px;padding:0.35rem 1rem;font-size:0.85rem;cursor:pointer;">&#128465; Verlopen taken verwijderen</button>
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
                        <td><a href="/customers/view?id={t["customer_id"]}" style="color:#5C7A5A;">{html.escape(t["customer_name"])}</a></td>
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
        elif path == '/tasks/delete-all-open':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/tasks/search')
                return
            if method == 'POST':
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    # Pause reminders for all deleted reminder tasks until their due date
                    cur.execute("""
                        SELECT customer_id, user_id, due_date FROM tasks
                        WHERE status='open' AND title LIKE 'Herinnering:%'
                    """)
                    for row in cur.fetchall():
                        paused = row[2] if row[2] else '9999-12-31'
                        cur.execute("""
                            UPDATE customer_users SET reminder_paused_until=?
                            WHERE customer_id=? AND user_id=?
                        """, (paused, row[0], row[1]))
                    conn.execute("DELETE FROM tasks WHERE status='open'")
                    conn.commit()
            self.respond_redirect('/tasks/search')
        elif path == '/tasks/delete-overdue':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/tasks/search')
                return
            if method == 'POST':
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    # Pause reminders for deleted overdue reminder tasks
                    cur.execute("""
                        SELECT customer_id, user_id, due_date FROM tasks
                        WHERE status='open' AND due_date < DATE('now') AND title LIKE 'Herinnering:%'
                    """)
                    for row in cur.fetchall():
                        paused = row[2] if row[2] else '9999-12-31'
                        cur.execute("""
                            UPDATE customer_users SET reminder_paused_until=?
                            WHERE customer_id=? AND user_id=?
                        """, (paused, row[0], row[1]))
                    conn.execute("DELETE FROM tasks WHERE status='open' AND due_date < DATE('now')")
                    conn.commit()
            self.respond_redirect('/tasks/search')
        elif path == '/tasks/export':
            # Export all tasks to CSV.
            if not logged_in:
                self.respond_redirect('/login')
                return
            import csv, io as _io
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                    <a href="/tasks/archive" style="color:#5C7A5A;font-size:0.9rem;">Wis filter</a>
                </form>
            </div>'''
            body += f'<div class="card"><div class="section-title">Voltooide taken ({len(done_tasks)}) <a href="/tasks/export" style="float:right;font-size:0.85rem;color:#5C7A5A;font-weight:normal;">&#8659; Exporteer alle taken (CSV)</a></div>'
            if done_tasks:
                body += '<table><thead><tr><th>Taak</th><th>Klant</th><th>Toegewezen aan</th><th>Vervaldatum</th><th>Afgerond op</th></tr></thead><tbody>'
                for t in done_tasks:
                    desc = f'<br><small style="color:#888;">{html.escape(t["description"])}</small>' if t['description'] else ''
                    body += f'''<tr>
                        <td>{html.escape(t["title"])}{desc}</td>
                        <td><a href="/customers/view?id={t["customer_id"]}" style="color:#5C7A5A;">{html.escape(t["customer_name"])}</a></td>
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute('SELECT id, username, email, created_at, is_admin, is_comm, is_governance FROM users ORDER BY id ASC')
                users = cur.fetchall()
            self.render_user_list(users, username, user_id)
        elif path == '/users/add':
            # Admin can add a new user via this route.  GET displays form, POST processes.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                if not self._csrf_ok(params, user_id):
                    self.respond_redirect('/users')
                    return
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute('DELETE FROM users WHERE id = ?', (uid_del_int,))
                conn.commit()
            log_action(user_id, 'delete', 'users', uid_del_int)
            self.respond_redirect('/users')
        elif path == '/users/toggle-admin':
            # Toggle admin status for a user. Only admin can do this. Cannot remove from id=1.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            uid_toggle = query_params.get('id', [None])[0]
            try:
                uid_toggle_int = int(uid_toggle) if uid_toggle else None
            except ValueError:
                uid_toggle_int = None
            if not uid_toggle_int or uid_toggle_int == 1:
                self.respond_redirect('/users')
                return
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute('SELECT is_admin FROM users WHERE id = ?', (uid_toggle_int,))
                row = cur.fetchone()
                if row:
                    new_val = 0 if row[0] else 1
                    cur.execute('UPDATE users SET is_admin = ? WHERE id = ?', (new_val, uid_toggle_int))
                    conn.commit()
                    log_action(user_id, 'update', 'users', uid_toggle_int, f'is_admin={new_val}')
            self.respond_redirect('/users')
        elif path == '/users/toggle-comm':
            # Toggle communication team membership. Admin only.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            uid_tc = query_params.get('id', [None])[0]
            try:
                uid_tc_int = int(uid_tc) if uid_tc else None
            except ValueError:
                uid_tc_int = None
            if uid_tc_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT is_comm FROM users WHERE id = ?', (uid_tc_int,))
                    row = cur.fetchone()
                    if row:
                        new_val = 0 if row[0] else 1
                        cur.execute('UPDATE users SET is_comm = ? WHERE id = ?', (new_val, uid_tc_int))
                        conn.commit()
                        log_action(user_id, 'update', 'users', uid_tc_int, f'is_comm={new_val}')
            self.respond_redirect('/users')
        elif path == '/users/toggle-governance':
            # Toggle governance team membership. Admin only.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            uid_tg = query_params.get('id', [None])[0]
            try:
                uid_tg_int = int(uid_tg) if uid_tg else None
            except ValueError:
                uid_tg_int = None
            if uid_tg_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT is_governance FROM users WHERE id = ?', (uid_tg_int,))
                    row = cur.fetchone()
                    if row:
                        new_val = 0 if row[0] else 1
                        cur.execute('UPDATE users SET is_governance = ? WHERE id = ?', (new_val, uid_tg_int))
                        conn.commit()
                        log_action(user_id, 'update', 'users', uid_tg_int, f'is_governance={new_val}')
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
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        conn.row_factory = sqlite3.Row
                        cur = conn.cursor()
                        cur.execute('SELECT * FROM customer_fields ORDER BY id ASC')
                        fields = cur.fetchall()
                    self.render_fields_list(fields, username, error='Naam en label zijn verplicht.')
                    return
                # insert
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    try:
                        cur.execute('INSERT INTO customer_fields (name, label) VALUES (?, ?)', (fname, flabel))
                        fid = cur.lastrowid
                        conn.commit()
                        # log action
                        log_action(user_id, 'create', 'customer_fields', fid, f"name={fname}")
                    except sqlite3.IntegrityError:
                        # duplicate name
                        with sqlite3.connect(DB_PATH, timeout=10) as conn2:
                            conn2.row_factory = sqlite3.Row
                            cur2 = conn2.cursor()
                            cur2.execute('SELECT * FROM customer_fields ORDER BY id ASC')
                            fields = cur2.fetchall()
                        self.render_fields_list(fields, username, error='Naam bestaat al.')
                        return
                self.respond_redirect('/fields')
            else:
                self.respond_redirect('/fields')
        elif path in ('/comm', '/comm/board'):
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            try:
                user_filter = int(query_params.get('user_filter', [0])[0])
            except (ValueError, TypeError):
                user_filter = 0
            self.render_comm_board(user_id, username, user_filter)
        elif path == '/comm/goals':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_comm_goals(user_id, username)
        elif path == '/comm/goals/add':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                target_date = params.get('target_date', [''])[0].strip() or None
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute(
                            'INSERT INTO comm_goals (title, description, target_date, created_by) VALUES (?, ?, ?, ?)',
                            (title, description or None, target_date, user_id))
                        conn.commit()
                self.respond_redirect('/comm/goals')
            else:
                self.render_comm_goal_form(user_id, username)
        elif path == '/comm/goals/reopen':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            gid = query_params.get('id', [None])[0]
            try:
                gid_int = int(gid) if gid else None
            except ValueError:
                gid_int = None
            if gid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute("UPDATE comm_goals SET status = 'actief' WHERE id = ?", (gid_int,))
                    conn.commit()
            self.respond_redirect('/comm/goals')
        elif path == '/comm/goals/complete':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            gid = query_params.get('id', [None])[0]
            try:
                gid_int = int(gid) if gid else None
            except ValueError:
                gid_int = None
            if gid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute("UPDATE comm_goals SET status = 'behaald' WHERE id = ?", (gid_int,))
                    conn.commit()
            self.respond_redirect('/comm/goals')
        elif path == '/comm/goals/delete':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            gid = query_params.get('id', [None])[0]
            try:
                gid_int = int(gid) if gid else None
            except ValueError:
                gid_int = None
            if gid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('DELETE FROM comm_goals WHERE id = ?', (gid_int,))
                    conn.commit()
            self.respond_redirect('/comm/goals')
        elif path == '/comm/tasks/add':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                due_date = params.get('due_date', [''])[0].strip() or None
                assigned_to_raw = params.get('assigned_to', [''])[0].strip()
                goal_id_raw = params.get('goal_id', [''])[0].strip()
                status = params.get('status', ['backlog'])[0].strip()
                priority = params.get('priority', ['medium'])[0].strip()
                tags = params.get('tags', [''])[0].strip() or None
                reminder_note = params.get('reminder_note', [''])[0].strip() or None
                if status not in ('backlog', 'bezig', 'klaar'):
                    status = 'backlog'
                if priority not in ('hoog', 'medium', 'laag'):
                    priority = 'medium'
                try:
                    assigned_to = int(assigned_to_raw) if assigned_to_raw else None
                except ValueError:
                    assigned_to = None
                try:
                    goal_id = int(goal_id_raw) if goal_id_raw else None
                except ValueError:
                    goal_id = None
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute(
                            'INSERT INTO comm_tasks (title, description, status, due_date, assigned_to, created_by, goal_id, priority, tags, reminder_note) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            (title, description or None, status, due_date, assigned_to, user_id, goal_id, priority, tags, reminder_note))
                        conn.commit()
                self.respond_redirect('/comm/board')
            else:
                self.render_comm_task_form(user_id, username)
        elif path == '/comm/tasks/edit':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            tid = query_params.get('id', [None])[0]
            try:
                tid_int = int(tid) if tid else None
            except ValueError:
                tid_int = None
            if not tid_int:
                self.respond_redirect('/comm/board')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                due_date = params.get('due_date', [''])[0].strip() or None
                assigned_to_raw = params.get('assigned_to', [''])[0].strip()
                goal_id_raw = params.get('goal_id', [''])[0].strip()
                status = params.get('status', ['backlog'])[0].strip()
                priority = params.get('priority', ['medium'])[0].strip()
                tags = params.get('tags', [''])[0].strip() or None
                reminder_note = params.get('reminder_note', [''])[0].strip() or None
                if status not in ('backlog', 'bezig', 'klaar'):
                    status = 'backlog'
                if priority not in ('hoog', 'medium', 'laag'):
                    priority = 'medium'
                try:
                    assigned_to = int(assigned_to_raw) if assigned_to_raw else None
                except ValueError:
                    assigned_to = None
                try:
                    goal_id = int(goal_id_raw) if goal_id_raw else None
                except ValueError:
                    goal_id = None
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('''UPDATE comm_tasks SET title=?, description=?, status=?, due_date=?,
                            assigned_to=?, goal_id=?, priority=?, tags=?, reminder_note=? WHERE id=?''',
                            (title, description or None, status, due_date, assigned_to, goal_id, priority, tags, reminder_note, tid_int))
                        conn.commit()
                self.respond_redirect('/comm/board')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM comm_tasks WHERE id = ?', (tid_int,))
                    task = cur.fetchone()
                    cur.execute("SELECT id, username FROM users WHERE is_comm=1 OR is_admin=1 OR id=1 ORDER BY username")
                    comm_members = cur.fetchall()
                    cur.execute("SELECT id, title FROM comm_goals WHERE status='actief' ORDER BY title")
                    active_goals = cur.fetchall()
                if not task:
                    self.respond_redirect('/comm/board')
                    return
                self.render_comm_task_edit(task, comm_members, active_goals, user_id, username)
        elif path == '/comm/tasks/comment':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                tid_raw = params.get('task_id', [''])[0].strip()
                content = params.get('content', [''])[0].strip()
                try:
                    tid_int = int(tid_raw) if tid_raw else None
                except ValueError:
                    tid_int = None
                if tid_int and content:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('INSERT INTO comm_task_comments (task_id, user_id, content) VALUES (?, ?, ?)',
                                    (tid_int, user_id, content))
                        conn.commit()
                self.respond_redirect('/comm/board')
        elif path == '/comm/tasks/archive-done':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute("UPDATE comm_tasks SET status = 'archief' WHERE status = 'klaar'")
                conn.commit()
            self.respond_redirect('/comm/board')
        elif path == '/comm/events-gov':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_events_gov(user_id, username)
        elif path == '/comm/events-gov/add':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                params = urllib.parse.parse_qs(self.rfile.read(length).decode('utf-8'))
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                event_context = params.get('event_context', [''])[0].strip()
                due_date = params.get('due_date', [''])[0].strip() or None
                priority = params.get('priority', ['medium'])[0].strip()
                if priority not in ('hoog', 'medium', 'laag'):
                    priority = 'medium'
                try:
                    assigned_to = int(params.get('assigned_to', [''])[0]) if params.get('assigned_to', [''])[0] else None
                except ValueError:
                    assigned_to = None
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        conn.execute('''INSERT INTO events_gov_tasks (title, description, event_context, assigned_to, due_date, priority, created_by)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                                     (title, description or None, event_context or None, assigned_to, due_date, priority, user_id))
                        conn.commit()
            self.respond_redirect('/comm/events-gov')
        elif path == '/comm/events-gov/status':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                params = urllib.parse.parse_qs(self.rfile.read(length).decode('utf-8'))
                try:
                    tid = int(params.get('id', [''])[0])
                    new_status = params.get('status', [''])[0].strip()
                    if new_status in ('open', 'in_check', 'klaar'):
                        with sqlite3.connect(DB_PATH, timeout=10) as conn:
                            conn.execute('UPDATE events_gov_tasks SET status=? WHERE id=?', (new_status, tid))
                            conn.commit()
                except (ValueError, IndexError):
                    pass
            self.respond_redirect('/comm/events-gov')
        elif path == '/comm/events-gov/delete':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                params = urllib.parse.parse_qs(self.rfile.read(length).decode('utf-8'))
                try:
                    tid = int(params.get('id', [''])[0])
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        conn.execute('DELETE FROM events_gov_tasks WHERE id=?', (tid,))
                        conn.commit()
                except (ValueError, IndexError):
                    pass
            self.respond_redirect('/comm/events-gov')
        elif path == '/comm/archived':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_comm_archived(user_id, username)
        elif path == '/comm/week':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_comm_week(user_id, username)
        elif path == '/comm/profile':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            profile_id_raw = query_params.get('id', [str(user_id)])[0]
            try:
                profile_id = int(profile_id_raw)
            except ValueError:
                profile_id = user_id
            if profile_id != user_id and not is_admin(user_id):
                self.respond_redirect('/comm/profile')
                return
            self.render_comm_profile(profile_id, user_id, username)
        elif path == '/comm/search':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            q = query_params.get('q', [''])[0].strip()
            filter_uid_raw = query_params.get('uid', [''])[0]
            filter_status = query_params.get('status', [''])[0].strip()
            filter_priority = query_params.get('priority', [''])[0].strip()
            try:
                filter_uid = int(filter_uid_raw) if filter_uid_raw else None
            except ValueError:
                filter_uid = None
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                conds = ['ct.status != "archief"']
                args = []
                if q:
                    conds.append('(ct.title LIKE ? OR ct.description LIKE ? OR ct.tags LIKE ?)')
                    like = f'%{q}%'
                    args.extend([like, like, like])
                if filter_uid:
                    conds.append('ct.assigned_to = ?')
                    args.append(filter_uid)
                if filter_status in ('backlog', 'bezig', 'klaar'):
                    conds.append('ct.status = ?')
                    args.append(filter_status)
                if filter_priority in ('hoog', 'medium', 'laag'):
                    conds.append('ct.priority = ?')
                    args.append(filter_priority)
                where = ' AND '.join(conds)
                cur.execute(f'''SELECT ct.*, u.username AS assigned_to_name, cg.title AS goal_title
                    FROM comm_tasks ct
                    LEFT JOIN users u ON ct.assigned_to = u.id
                    LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
                    WHERE {where}
                    ORDER BY CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                    COALESCE(ct.due_date,'9999-12-31') ASC''', args)
                results = cur.fetchall()
                cur.execute("SELECT id, username FROM users WHERE is_comm=1 OR is_admin=1 OR id=1 ORDER BY username")
                comm_members = cur.fetchall()
            self.render_comm_search(results, comm_members, q, filter_uid, filter_status, filter_priority, user_id, username)
        elif path == '/comm/goals/edit':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            gid = query_params.get('id', [None])[0]
            try:
                gid_int = int(gid) if gid else None
            except ValueError:
                gid_int = None
            if not gid_int:
                self.respond_redirect('/comm/goals')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                target_date = params.get('target_date', [''])[0].strip() or None
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE comm_goals SET title=?, description=?, target_date=? WHERE id=?',
                                    (title, description or None, target_date, gid_int))
                        conn.commit()
                self.respond_redirect('/comm/goals')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM comm_goals WHERE id = ?', (gid_int,))
                    goal = cur.fetchone()
                if not goal:
                    self.respond_redirect('/comm/goals')
                    return
                self.render_comm_goal_edit(goal, user_id, username)
        elif path == '/comm/tasks/move':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            tid = query_params.get('id', [None])[0]
            new_status = query_params.get('status', ['backlog'])[0].strip()
            if new_status not in ('backlog', 'bezig', 'klaar'):
                new_status = 'backlog'
            try:
                tid_int = int(tid) if tid else None
            except ValueError:
                tid_int = None
            if tid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('UPDATE comm_tasks SET status = ? WHERE id = ?', (new_status, tid_int))
                    conn.commit()
            self.respond_redirect('/comm/board')
        elif path == '/comm/tasks/delete':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            tid = query_params.get('id', [None])[0]
            try:
                tid_int = int(tid) if tid else None
            except ValueError:
                tid_int = None
            if tid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('DELETE FROM comm_tasks WHERE id = ?', (tid_int,))
                    conn.commit()
            self.respond_redirect('/comm/board')

        # ── Belangrijke Datums ───────────────────────────────────────────────
        elif path == '/comm/dates':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_comm_dates(user_id, username)
        elif path == '/comm/dates/add':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                date_val = params.get('date', [''])[0].strip()
                dtype = params.get('type', ['event'])[0].strip()
                if dtype not in ('event', 'deadline', 'mijlpaal'):
                    dtype = 'event'
                if title and date_val:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('INSERT INTO comm_dates (title, description, date, type, created_by) VALUES (?, ?, ?, ?, ?)',
                                    (title, description or None, date_val, dtype, user_id))
                        conn.commit()
            self.respond_redirect('/comm/dates')
        elif path == '/comm/dates/edit':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            did = query_params.get('id', [None])[0]
            try:
                did_int = int(did) if did else None
            except ValueError:
                did_int = None
            if not did_int:
                self.respond_redirect('/comm/dates')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                date_val = params.get('date', [''])[0].strip()
                dtype = params.get('type', ['event'])[0].strip()
                if dtype not in ('event', 'deadline', 'mijlpaal'):
                    dtype = 'event'
                if title and date_val:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE comm_dates SET title=?, description=?, date=?, type=? WHERE id=?',
                                    (title, description or None, date_val, dtype, did_int))
                        conn.commit()
                self.respond_redirect('/comm/dates')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM comm_dates WHERE id = ?', (did_int,))
                    date_row = cur.fetchone()
                if not date_row:
                    self.respond_redirect('/comm/dates')
                    return
                self.render_comm_date_edit(date_row, user_id, username)
        elif path == '/comm/dates/delete':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            did = query_params.get('id', [None])[0]
            try:
                did_int = int(did) if did else None
            except ValueError:
                did_int = None
            if did_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('DELETE FROM comm_dates WHERE id = ?', (did_int,))
                    conn.commit()
            self.respond_redirect('/comm/dates')
        elif path == '/comm/dates/to-task':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            did = query_params.get('id', [None])[0]
            try:
                did_int = int(did) if did else None
            except ValueError:
                did_int = None
            if did_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM comm_dates WHERE id = ?', (did_int,))
                    drow = cur.fetchone()
                    if drow:
                        cur.execute('INSERT INTO comm_tasks (title, description, status, due_date, created_by, priority) VALUES (?, ?, ?, ?, ?, ?)',
                                    (drow['title'], drow['description'], 'backlog', drow['date'], user_id, 'medium'))
                        conn.commit()
            self.respond_redirect('/comm/board')

        # ── Content Kalender ─────────────────────────────────────────────────
        elif path == '/comm/content':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_comm_content(user_id, username)
        elif path == '/comm/content/add':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                platform = params.get('platform', ['overig'])[0].strip()
                publish_date = params.get('publish_date', [''])[0].strip() or None
                status = params.get('status', ['idee'])[0].strip()
                tags = params.get('tags', [''])[0].strip() or None
                assigned_to_raw = params.get('assigned_to', [''])[0].strip()
                if status not in ('idee', 'gepland', 'klaar', 'gepubliceerd'):
                    status = 'idee'
                if platform not in ('instagram', 'linkedin', 'website', 'email', 'overig'):
                    platform = 'overig'
                try:
                    assigned_to = int(assigned_to_raw) if assigned_to_raw else None
                except ValueError:
                    assigned_to = None
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('INSERT INTO comm_content (title, description, platform, publish_date, status, assigned_to, created_by, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                    (title, description or None, platform, publish_date, status, assigned_to, user_id, tags))
                        conn.commit()
            self.respond_redirect('/comm/content')
        elif path == '/comm/content/edit':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            cid = query_params.get('id', [None])[0]
            try:
                cid_int = int(cid) if cid else None
            except ValueError:
                cid_int = None
            if not cid_int:
                self.respond_redirect('/comm/content')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                platform = params.get('platform', ['overig'])[0].strip()
                publish_date = params.get('publish_date', [''])[0].strip() or None
                status = params.get('status', ['idee'])[0].strip()
                tags = params.get('tags', [''])[0].strip() or None
                assigned_to_raw = params.get('assigned_to', [''])[0].strip()
                if status not in ('idee', 'gepland', 'klaar', 'gepubliceerd'):
                    status = 'idee'
                if platform not in ('instagram', 'linkedin', 'website', 'email', 'overig'):
                    platform = 'overig'
                try:
                    assigned_to = int(assigned_to_raw) if assigned_to_raw else None
                except ValueError:
                    assigned_to = None
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE comm_content SET title=?, description=?, platform=?, publish_date=?, status=?, assigned_to=?, tags=? WHERE id=?',
                                    (title, description or None, platform, publish_date, status, assigned_to, tags, cid_int))
                        conn.commit()
                self.respond_redirect('/comm/content')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM comm_content WHERE id = ?', (cid_int,))
                    citem = cur.fetchone()
                    cur.execute('SELECT id, username FROM users WHERE is_comm=1 OR is_admin=1 OR id=1 ORDER BY username')
                    comm_members = cur.fetchall()
                if not citem:
                    self.respond_redirect('/comm/content')
                    return
                self.render_comm_content_edit(citem, comm_members, user_id, username)
        elif path == '/comm/content/move':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            cid = query_params.get('id', [None])[0]
            new_status = query_params.get('status', ['idee'])[0].strip()
            if new_status not in ('idee', 'gepland', 'klaar', 'gepubliceerd'):
                new_status = 'idee'
            try:
                cid_int = int(cid) if cid else None
            except ValueError:
                cid_int = None
            if cid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('UPDATE comm_content SET status=? WHERE id=?', (new_status, cid_int))
                    conn.commit()
            self.respond_redirect('/comm/content')
        elif path == '/comm/content/delete':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            cid = query_params.get('id', [None])[0]
            try:
                cid_int = int(cid) if cid else None
            except ValueError:
                cid_int = None
            if cid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('DELETE FROM comm_content WHERE id=?', (cid_int,))
                    conn.commit()
            self.respond_redirect('/comm/content')
        elif path == '/comm/content/to-task':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            cid = query_params.get('id', [None])[0]
            try:
                cid_int = int(cid) if cid else None
            except ValueError:
                cid_int = None
            if cid_int:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM comm_content WHERE id=?', (cid_int,))
                    citem = cur.fetchone()
                    if citem:
                        task_title = f'[{citem["platform"].capitalize()}] {citem["title"]}'
                        cur.execute('INSERT INTO comm_tasks (title, description, status, due_date, assigned_to, created_by, priority, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                    (task_title, citem['description'], 'backlog', citem['publish_date'], citem['assigned_to'], user_id, 'medium', citem['tags']))
                        conn.commit()
            self.respond_redirect('/comm/board')

        elif path == '/comm/content/board-status':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            cid_raw = query_params.get('id', [''])[0]
            new_bs = query_params.get('status', [''])[0].strip()
            try:
                cid_int = int(cid_raw)
            except ValueError:
                self.respond_redirect('/comm/content')
                return
            allowed_bs = ('backlog', 'bezig', 'klaar', '')
            if new_bs not in allowed_bs:
                new_bs = ''
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
                cur = conn.cursor()
                cur.execute('UPDATE comm_content SET board_status=? WHERE id=?', (new_bs or None, cid_int))
                conn.commit()
            self.respond_redirect('/comm/content')

        # ── Overzicht ─────────────────────────────────────────────────────────
        elif path == '/comm/overview':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_comm_overview(user_id, username)

        # ── Profiel bewerken ─────────────────────────────────────────────────
        elif path == '/comm/profile/edit':
            if not logged_in or not is_comm_member(user_id):
                self.respond_redirect('/dashboard')
                return
            target_id_raw = query_params.get('id', [str(user_id)])[0]
            try:
                target_id = int(target_id_raw)
            except ValueError:
                target_id = user_id
            if target_id != user_id and not is_admin(user_id):
                self.respond_redirect(f'/comm/profile?id={user_id}')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                role_title = params.get('role_title', [''])[0].strip() or None
                bio = params.get('bio', [''])[0].strip() or None
                skills = params.get('skills', [''])[0].strip() or None
                avatar_color = params.get('avatar_color', ['#5C7A5A'])[0].strip()
                if not avatar_color.startswith('#'):
                    avatar_color = '#5C7A5A'
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT user_id FROM comm_profiles WHERE user_id=?', (target_id,))
                    if cur.fetchone():
                        cur.execute('UPDATE comm_profiles SET role_title=?, bio=?, skills=?, avatar_color=?, updated_at=CURRENT_TIMESTAMP WHERE user_id=?',
                            (role_title, bio, skills, avatar_color, target_id))
                    else:
                        cur.execute('INSERT INTO comm_profiles (user_id, role_title, bio, skills, avatar_color, updated_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)',
                            (target_id, role_title, bio, skills, avatar_color))
                    conn.commit()
                self.respond_redirect(f'/comm/profile?id={target_id}')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT id, username, email FROM users WHERE id=?', (target_id,))
                    target_user = cur.fetchone()
                    cur.execute('SELECT * FROM comm_profiles WHERE user_id=?', (target_id,))
                    profile = cur.fetchone()
                if not target_user:
                    self.respond_redirect('/comm/board')
                    return
                self.render_comm_profile_edit(target_user, profile, user_id, username)

        # ── Governance module ────────────────────────────────────────────────
        elif path == '/gov/board':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_gov_board(user_id, username)
        elif path == '/gov/overview':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_gov_overview(user_id, username)
        elif path == '/gov/person':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            person_id_raw = query_params.get('id', [None])[0]
            try:
                person_id = int(person_id_raw) if person_id_raw else None
            except ValueError:
                person_id = None
            if not person_id:
                self.respond_redirect('/gov/board')
                return
            self.render_gov_person(person_id, user_id, username)
        elif path == '/gov/persons/add':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                name = params.get('name', [''])[0].strip()
                tags = params.get('tags', [''])[0].strip()
                phase = params.get('phase', ['startpunt'])[0].strip()
                notes = params.get('notes', [''])[0].strip()
                project_type = params.get('project_type', [''])[0].strip()
                valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
                if phase not in valid_phases:
                    phase = 'startpunt'
                if name:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('INSERT INTO governance_persons (name, phase, tags, notes, created_by, project_type) VALUES (?, ?, ?, ?, ?, ?)',
                            (name, phase, tags or None, notes or None, user_id, project_type or None))
                        conn.commit()
            self.respond_redirect('/gov/board')
        elif path == '/gov/persons/edit':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            pid_raw = query_params.get('id', [None])[0]
            try:
                pid = int(pid_raw) if pid_raw else None
            except ValueError:
                pid = None
            if not pid:
                self.respond_redirect('/gov/board')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                name = params.get('name', [''])[0].strip()
                tags = params.get('tags', [''])[0].strip()
                phase = params.get('phase', ['startpunt'])[0].strip()
                notes = params.get('notes', [''])[0].strip()
                valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
                if phase not in valid_phases:
                    phase = 'startpunt'
                if name:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE governance_persons SET name=?, phase=?, tags=?, notes=? WHERE id=?',
                            (name, phase, tags or None, notes or None, pid))
                        conn.commit()
                self.respond_redirect('/gov/board')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM governance_persons WHERE id=?', (pid,))
                    person = cur.fetchone()
                if not person:
                    self.respond_redirect('/gov/board')
                    return
                valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
                phase_labels = {'startpunt':'Startpunt','empathize':'Empathize','define':'Define','ideate':'Ideate','prototype':'Prototype','test':'Test','uittreden':'Uittreden'}
                phase_opts = ''.join(f'<option value="{p}" {"selected" if person["phase"]==p else ""}>{phase_labels[p]}</option>' for p in valid_phases)
                body = html_header('Persoon bewerken', True, username, user_id)
                body += self._gov_nav('board', user_id)
                body += f'<h2 class="mt-4">&#9998; Persoon bewerken</h2>'
                body += f'''<div class="card" style="max-width:560px;">
                    <form method="POST" action="/gov/persons/edit?id={person["id"]}">
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Naam</label><br>
                            <input type="text" name="name" value="{html.escape(person["name"])}" class="form-control" required></div>
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Fase</label><br>
                            <select name="phase" class="form-control">{phase_opts}</select></div>
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Tags</label><br>
                            <input type="text" name="tags" value="{html.escape(person["tags"] or "")}" class="form-control" placeholder="komma-gescheiden"></div>
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Notities</label><br>
                            <textarea name="notes" class="form-control" rows="3">{html.escape(person["notes"] or "")}</textarea></div>
                        <button type="submit" class="btn btn-primary">Opslaan</button>
                        <a href="/gov/board" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
                    </form></div>'''
                body += html_footer()
                self._send_html(body)
        elif path == '/gov/persons/delete':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            pid_raw = query_params.get('id', [None])[0]
            try:
                pid = int(pid_raw) if pid_raw else None
            except ValueError:
                pid = None
            if pid:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('PRAGMA foreign_keys = ON')
                    cur.execute('DELETE FROM governance_persons WHERE id=?', (pid,))
                    conn.commit()
            self.respond_redirect('/gov/board')
        elif path == '/gov/persons/move':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            pid_raw = query_params.get('id', [None])[0]
            new_phase = query_params.get('phase', ['startpunt'])[0]
            valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
            if new_phase not in valid_phases:
                new_phase = 'startpunt'
            try:
                pid = int(pid_raw) if pid_raw else None
            except ValueError:
                pid = None
            if pid:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('UPDATE governance_persons SET phase=? WHERE id=?', (new_phase, pid))
                    conn.commit()
            self.respond_redirect('/gov/board')
        elif path == '/gov/progress/complete':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                pid_raw = params.get('person_id', [None])[0]
                iid_raw = params.get('item_id', [None])[0]
                note = params.get('note', [''])[0].strip() or None
                redirect_to = params.get('redirect', ['/gov/board'])[0]
                try:
                    pid = int(pid_raw) if pid_raw else None
                    iid = int(iid_raw) if iid_raw else None
                except ValueError:
                    pid = None
                    iid = None
                if pid and iid:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('SELECT id FROM governance_progress WHERE person_id=? AND item_id=?', (pid, iid))
                        if not cur.fetchone():
                            cur.execute('INSERT INTO governance_progress (person_id, item_id, completed_by, note) VALUES (?, ?, ?, ?)',
                                (pid, iid, user_id, note))
                            conn.commit()
                self.respond_redirect(redirect_to)
            else:
                self.respond_redirect('/gov/board')
        elif path == '/gov/items/quick-edit':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                iid_raw = params.get('item_id', [None])[0]
                pid_raw = params.get('person_id', [None])[0]
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip() or None
                norm = params.get('norm', [''])[0].strip() or None
                middelen = params.get('middelen', [''])[0].strip() or None
                try:
                    iid = int(iid_raw) if iid_raw else None
                    pid = int(pid_raw) if pid_raw else None
                except ValueError:
                    iid = None
                    pid = None
                if iid and title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE governance_card_items SET title=?, description=?, norm=?, middelen=? WHERE id=?',
                            (title, description, norm, middelen, iid))
                        conn.commit()
                self.respond_redirect(f'/gov/person?id={pid}' if pid else '/gov/board')
            else:
                self.respond_redirect('/gov/board')
        elif path == '/gov/progress/toggle':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            pid_raw = query_params.get('person_id', [None])[0]
            iid_raw = query_params.get('item_id', [None])[0]
            try:
                pid = int(pid_raw) if pid_raw else None
                iid = int(iid_raw) if iid_raw else None
            except ValueError:
                pid = None
                iid = None
            if pid and iid:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('SELECT id FROM governance_progress WHERE person_id=? AND item_id=?', (pid, iid))
                    existing = cur.fetchone()
                    if existing:
                        cur.execute('DELETE FROM governance_progress WHERE person_id=? AND item_id=?', (pid, iid))
                    else:
                        cur.execute('INSERT INTO governance_progress (person_id, item_id, completed_by) VALUES (?, ?, ?)', (pid, iid, user_id))
                    conn.commit()
            redirect_raw = query_params.get('redirect', [None])[0]
            redirect_to = redirect_raw if redirect_raw else f'/gov/person?id={pid}'
            self.respond_redirect(redirect_to)
        elif path == '/gov/profiles':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            self.render_gov_profiles(user_id, username)
        elif path == '/gov/profiles/consent':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            pid_raw = query_params.get('id', [None])[0]
            try:
                pid = int(pid_raw) if pid_raw else None
            except ValueError:
                pid = None
            if pid:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT consent_given FROM governance_persons WHERE id=?', (pid,))
                    row = cur.fetchone()
                    if row:
                        new_val = 0 if row['consent_given'] else 1
                        cur.execute('UPDATE governance_persons SET consent_given=? WHERE id=?', (new_val, pid))
                        conn.commit()
            self.respond_redirect('/gov/profiles')
        elif path == '/gov/notes/add':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                pid_raw = params.get('person_id', [None])[0]
                note_type = params.get('note_type', ['coaching'])[0].strip()
                content = params.get('content', [''])[0].strip()
                valid_types = ['coaching', 'intervisie', 'aandachtspunt']
                if note_type not in valid_types:
                    note_type = 'coaching'
                try:
                    pid = int(pid_raw) if pid_raw else None
                except ValueError:
                    pid = None
                if pid and content:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('INSERT INTO governance_notes (person_id, note_type, content, created_by) VALUES (?, ?, ?, ?)',
                            (pid, note_type, content, user_id))
                        conn.commit()
            self.respond_redirect('/gov/profiles')
        elif path == '/gov/notes/delete':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            nid_raw = query_params.get('id', [None])[0]
            try:
                nid = int(nid_raw) if nid_raw else None
            except ValueError:
                nid = None
            if nid:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('DELETE FROM governance_notes WHERE id=?', (nid,))
                    conn.commit()
            self.respond_redirect('/gov/profiles')
        elif path == '/gov/cards':
            if not logged_in or not is_gov_member(user_id):
                self.respond_redirect('/dashboard')
                return
            if not is_admin(user_id):
                self.respond_redirect('/gov/board')
                return
            self.render_gov_cards(user_id, username)
        elif path == '/gov/cards/add':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                phase = params.get('phase', ['startpunt'])[0].strip()
                description = params.get('description', [''])[0].strip()
                order_index = params.get('order_index', ['0'])[0].strip()
                project_type_val = params.get('project_type', [''])[0].strip().lower() or None
                valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
                if phase not in valid_phases:
                    phase = 'startpunt'
                try:
                    order_index = int(order_index)
                except ValueError:
                    order_index = 0
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('INSERT INTO governance_card_templates (phase, title, description, order_index, project_type) VALUES (?, ?, ?, ?, ?)',
                            (phase, title, description or None, order_index, project_type_val))
                        conn.commit()
            self.respond_redirect('/gov/cards')
        elif path == '/gov/cards/edit':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            cid_raw = query_params.get('id', [None])[0]
            try:
                cid = int(cid_raw) if cid_raw else None
            except ValueError:
                cid = None
            if not cid:
                self.respond_redirect('/gov/cards')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                title = params.get('title', [''])[0].strip()
                phase = params.get('phase', ['startpunt'])[0].strip()
                description = params.get('description', [''])[0].strip()
                order_index = params.get('order_index', ['0'])[0].strip()
                project_type_val = params.get('project_type', [''])[0].strip().lower() or None
                valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
                if phase not in valid_phases:
                    phase = 'startpunt'
                try:
                    order_index = int(order_index)
                except ValueError:
                    order_index = 0
                if title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('UPDATE governance_card_templates SET phase=?, title=?, description=?, order_index=?, project_type=? WHERE id=?',
                            (phase, title, description or None, order_index, project_type_val, cid))
                        conn.commit()
                self.respond_redirect('/gov/cards')
            else:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    conn.row_factory = sqlite3.Row
                    cur = conn.cursor()
                    cur.execute('SELECT * FROM governance_card_templates WHERE id=?', (cid,))
                    card = cur.fetchone()
                if not card:
                    self.respond_redirect('/gov/cards')
                    return
                valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
                phase_labels = {'startpunt':'Startpunt','empathize':'Empathize','define':'Define','ideate':'Ideate','prototype':'Prototype','test':'Test','uittreden':'Uittreden'}
                phase_opts = ''.join(f'<option value="{p}" {"selected" if card["phase"]==p else ""}>{phase_labels[p]}</option>' for p in valid_phases)
                body = html_header('Kaart bewerken', True, username, user_id)
                body += self._gov_nav('cards', user_id)
                body += f'<h2 class="mt-4">&#9998; Kaart bewerken</h2>'
                cur_pt = card['project_type'] or ''
                pt_opts_edit = '<option value="" ' + ('selected' if not cur_pt else '') + '>Alle typen</option>' + ''.join(f'<option value="{t}" {"selected" if cur_pt==t else ""}>{t.capitalize()}</option>' for t in ['communicatie','werkveld','evenementen','onderwijs'])
                body += f'''<div class="card" style="max-width:560px;">
                    <form method="POST" action="/gov/cards/edit?id={card["id"]}">
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Titel</label><br>
                            <input type="text" name="title" value="{html.escape(card["title"])}" class="form-control" required></div>
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Fase</label><br>
                            <select name="phase" class="form-control">{phase_opts}</select></div>
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Projecttype</label><br>
                            <select name="project_type" class="form-control">{pt_opts_edit}</select></div>
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Beschrijving</label><br>
                            <textarea name="description" class="form-control" rows="2">{html.escape(card["description"] or "")}</textarea></div>
                        <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Volgorde</label><br>
                            <input type="number" name="order_index" value="{card["order_index"]}" class="form-control"></div>
                        <button type="submit" class="btn btn-primary">Opslaan</button>
                        <a href="/gov/cards" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
                    </form></div>'''
                body += html_footer()
                self._send_html(body)
        elif path == '/gov/cards/delete':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            cid_raw = query_params.get('id', [None])[0]
            try:
                cid = int(cid_raw) if cid_raw else None
            except ValueError:
                cid = None
            if cid:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('PRAGMA foreign_keys = ON')
                    cur.execute('DELETE FROM governance_card_templates WHERE id=?', (cid,))
                    conn.commit()
            self.respond_redirect('/gov/cards')
        elif path == '/gov/items/add':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            if method == 'POST':
                length = int(self.headers.get('Content-Length', 0))
                data = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(data)
                card_id = params.get('card_id', [''])[0].strip()
                title = params.get('title', [''])[0].strip()
                description = params.get('description', [''])[0].strip()
                order_index = params.get('order_index', ['0'])[0].strip()
                try:
                    card_id = int(card_id)
                    order_index = int(order_index)
                except ValueError:
                    card_id = None
                    order_index = 0
                if card_id and title:
                    with sqlite3.connect(DB_PATH, timeout=10) as conn:
                        cur = conn.cursor()
                        cur.execute('INSERT INTO governance_card_items (card_id, title, description, order_index) VALUES (?, ?, ?, ?)',
                            (card_id, title, description or None, order_index))
                        conn.commit()
            self.respond_redirect('/gov/cards')
        elif path == '/gov/items/delete':
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            iid_raw = query_params.get('id', [None])[0]
            try:
                iid = int(iid_raw) if iid_raw else None
            except ValueError:
                iid = None
            if iid:
                with sqlite3.connect(DB_PATH, timeout=10) as conn:
                    cur = conn.cursor()
                    cur.execute('PRAGMA foreign_keys = ON')
                    cur.execute('DELETE FROM governance_card_items WHERE id=?', (iid,))
                    conn.commit()
            self.respond_redirect('/gov/cards')

        elif path == '/reports':
            # Display reports/dashboard for admin.  Only admin can view.
            if not logged_in or not is_admin(user_id):
                self.respond_redirect('/dashboard')
                return
            # Fetch statistics: customers by category, tasks by status, interactions by type
            with sqlite3.connect(DB_PATH, timeout=10) as conn:
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

    def _send_json(self, data: dict) -> None:
        import json
        body = json.dumps(data).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def render_conversations(self, user_id: int, username: str) -> None:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''
                SELECT other_id, u.username AS other_name,
                       MAX(m.created_at) AS last_at,
                       SUM(CASE WHEN m.recipient_id=? AND m.is_read=0 THEN 1 ELSE 0 END) AS unread,
                       (SELECT content FROM messages m2
                        WHERE (m2.sender_id=m.other_id AND m2.recipient_id=?)
                           OR (m2.sender_id=? AND m2.recipient_id=m.other_id)
                        ORDER BY m2.created_at DESC LIMIT 1) AS last_content
                FROM (
                    SELECT CASE WHEN sender_id=? THEN recipient_id ELSE sender_id END AS other_id,
                           id, created_at, recipient_id, is_read
                    FROM messages WHERE sender_id=? OR recipient_id=?
                ) m
                JOIN users u ON u.id = m.other_id
                GROUP BY other_id
                ORDER BY last_at DESC
            ''', (user_id, user_id, user_id, user_id, user_id, user_id))
            convs = cur.fetchall()
            cur.execute('SELECT id, username FROM users WHERE id != ? ORDER BY username', (user_id,))
            all_users = cur.fetchall()
        body = html_header('Berichten', True, username, user_id)
        body += '<h2 class="mt-4">&#128172; Berichten</h2>'
        user_opts = ''.join(f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in all_users)
        body += f'''<div style="margin-bottom:1rem;">
            <form method="GET" action="/messages/conversation" style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;">
                <select name="with" class="form-control" style="max-width:220px;" required>
                    <option value="">— Nieuw gesprek met... —</option>
                    {user_opts}
                </select>
                <button type="submit" class="btn btn-primary">&#43; Start gesprek</button>
            </form>
        </div>'''
        if convs:
            body += '<div class="card" style="padding:0;">'
            for c in convs:
                unread = c['unread'] or 0
                snippet = html.escape((c['last_content'] or '')[:80])
                badge = f'<span style="background:#5C7A5A;color:#fff;border-radius:50%;font-size:0.75rem;font-weight:bold;min-width:20px;height:20px;line-height:20px;text-align:center;display:inline-block;margin-left:0.4rem;">{unread}</span>' if unread else ''
                row_bg = 'background:#EDF3EC;' if unread else ''
                body += f'''<a href="/messages/conversation?with={c["other_id"]}" style="display:flex;align-items:center;padding:0.85rem 1.1rem;border-bottom:1px solid #eee;text-decoration:none;color:inherit;{row_bg}">
                    <div style="width:38px;height:38px;border-radius:50%;background:#5C7A5A;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:1rem;margin-right:0.85rem;flex-shrink:0;">{html.escape(c["other_name"][0].upper())}</div>
                    <div style="flex:1;min-width:0;">
                        <div style="font-weight:{"bold" if unread else "normal"};">{html.escape(c["other_name"])}{badge}</div>
                        <div style="font-size:0.85rem;color:#888;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{snippet}</div>
                    </div>
                    <div style="font-size:0.78rem;color:#aaa;flex-shrink:0;margin-left:0.5rem;">{(c["last_at"] or "")[:16]}</div>
                </a>'''
            body += '</div>'
        else:
            body += '<p style="color:#888;">Nog geen berichten. Start een gesprek hierboven.</p>'
        body += html_footer()
        self._send_html(body)

    def render_conversation(self, user_id: int, username: str, other_id: int) -> None:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT id, username FROM users WHERE id=?', (other_id,))
            other = cur.fetchone()
            if not other:
                self.respond_redirect('/messages')
                return
            cur.execute('''
                SELECT m.*, u.username AS sender_name,
                       r.content AS reply_content, ru.username AS reply_from
                FROM messages m
                JOIN users u ON m.sender_id = u.id
                LEFT JOIN messages r ON m.reply_to = r.id
                LEFT JOIN users ru ON r.sender_id = ru.id
                WHERE (m.sender_id=? AND m.recipient_id=?) OR (m.sender_id=? AND m.recipient_id=?)
                ORDER BY m.created_at ASC
            ''', (user_id, other_id, other_id, user_id))
            msgs = cur.fetchall()
        other_name = other['username']
        body = html_header(f'Gesprek met {other_name}', True, username, user_id)
        body += f'<div style="display:flex;align-items:center;gap:0.75rem;margin-top:1.5rem;margin-bottom:1rem;"><a href="/messages" style="color:#5C7A5A;">&#8592; Terug</a><h2 style="margin:0;">&#128172; {html.escape(other_name)}</h2></div>'
        body += '<div id="chat-box" style="display:flex;flex-direction:column;gap:0.5rem;margin-bottom:1.2rem;max-height:65vh;overflow-y:auto;padding:0.5rem;">'
        for m in msgs:
            is_me = m['sender_id'] == user_id
            align = 'flex-end' if is_me else 'flex-start'
            bg = '#5C7A5A' if is_me else '#f0f0f0'
            fg = '#fff' if is_me else '#333'
            reply_html = ''
            if m['reply_content']:
                border_col = 'rgba(255,255,255,0.5)' if is_me else '#5C7A5A'
                reply_html = f'<div style="font-size:0.78rem;border-left:3px solid {border_col};padding-left:0.4rem;margin-bottom:0.3rem;opacity:0.85;">{html.escape(m["reply_from"] or "")}: {html.escape((m["reply_content"] or "")[:60])}</div>'
            safe_sender = html.escape(m["sender_name"]).replace("'", "&#39;")
            safe_content = html.escape((m["content"] or "")[:60]).replace("'", "&#39;")
            body += f'''<div style="display:flex;flex-direction:column;align-items:{align};">
                <div style="background:{bg};color:{fg};border-radius:12px;padding:0.55rem 0.85rem;max-width:70%;word-break:break-word;">
                    {reply_html}{html.escape(m["content"])}
                </div>
                <div style="font-size:0.75rem;color:#aaa;margin-top:0.15rem;display:flex;gap:0.5rem;align-items:center;">
                    {(m["created_at"] or "")[:16]}
                    <a href="#" onclick="setReply({m["id"]},'{safe_sender}','{safe_content}');return false;" style="color:#5C7A5A;font-size:0.75rem;">&#8617; Reply</a>
                </div>
            </div>'''
        body += '</div>'
        body += f'''<div id="reply-preview" style="display:none;background:#EDF3EC;border-left:4px solid #5C7A5A;padding:0.4rem 0.8rem;border-radius:4px;margin-bottom:0.5rem;font-size:0.85rem;">
            <span id="reply-preview-text" style="flex:1;"></span>
            <button onclick="clearReply()" style="float:right;background:none;border:none;cursor:pointer;color:#5C7A5A;font-size:1rem;">&#10005;</button>
        </div>
        <form method="POST" action="/messages/conversation?with={other_id}" style="display:flex;gap:0.5rem;align-items:flex-end;">
            <input type="hidden" name="reply_to" id="reply-to-input" value="">
            <textarea name="content" class="form-control" rows="2" placeholder="Schrijf een bericht..." required style="flex:1;resize:none;" id="msg-input" onkeydown="if(event.key==='Enter'&&!event.shiftKey){{event.preventDefault();this.form.submit();}}"></textarea>
            <button type="submit" class="btn btn-primary">Verstuur</button>
        </form>
        <script>
        function setReply(id, from, text) {{
            document.getElementById('reply-to-input').value = id;
            document.getElementById('reply-preview-text').textContent = from + ': ' + text;
            document.getElementById('reply-preview').style.display = 'block';
            document.getElementById('msg-input').focus();
        }}
        function clearReply() {{
            document.getElementById('reply-to-input').value = '';
            document.getElementById('reply-preview').style.display = 'none';
        }}
        var cb = document.getElementById('chat-box');
        if (cb) cb.scrollTop = cb.scrollHeight;
        </script>'''
        body += html_footer()
        self._send_html(body)

    def render_dashboard(self, user_id: int, username: str) -> None:
        # Count customers, get recent notes and tasks due soon
        this_month = datetime.date.today().strftime('%Y-%m')
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            cur.execute("SELECT verbinding, COUNT(*) AS cnt FROM customers WHERE verbinding IS NOT NULL GROUP BY verbinding")
            verbinding_stats = {row['verbinding']: row['cnt'] for row in cur.fetchall()}
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
        def _stat(val, label, color='#5C7A5A'):
            return f'<div class="card" style="flex:1;min-width:130px;text-align:center;padding:0.75rem;"><div style="font-size:1.8rem;font-weight:bold;color:{color};">{val}</div><div style="font-size:0.85rem;color:#555;">{label}</div></div>'
        body += f'<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
        body += _stat(total_customers, 'Klanten')
        body += _stat(total_open_tasks, 'Open taken', '#f57f17')
        body += _stat(total_overdue, 'Verlopen taken', '#dc3545' if total_overdue else '#388e3c')
        body += _stat(interactions_this_month, 'Interacties deze maand', '#1565c0')
        body += '</div>'
        # Verbinding stats
        vb_items = [
            ('ambassadeur', '#2e7d32', '#e8f5e9'),
            ('betrokken', '#1565c0', '#e3f0ff'),
            ('niet betrokken', '#888', '#f5f5f5'),
        ]
        body += '<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
        for vb_key, vb_color, vb_bg in vb_items:
            cnt = verbinding_stats.get(vb_key, 0)
            body += f'<a href="/customers?verbinding={urllib.parse.quote(vb_key)}" style="flex:1;min-width:130px;text-decoration:none;"><div class="card" style="text-align:center;padding:0.75rem;background:{vb_bg};border-left:4px solid {vb_color};"><div style="font-size:1.8rem;font-weight:bold;color:{vb_color};">{cnt}</div><div style="font-size:0.85rem;color:#555;">{vb_key.capitalize()}</div></div></a>'
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
                <td><a href="/users/profile?id={us['id']}" style="color:#5C7A5A;">{html.escape(us['username'])}</a></td>
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
                cust_link = f"<a href='/customers/view?id={t['customer_id']}' style='color:#5C7A5A;font-weight:bold;'>{html.escape(t['customer_name'])}</a>"
                assigned_to = html.escape(t['assigned_to']) if t['assigned_to'] else ''
                resolve_btn = f"<a href='/tasks/resolve?id={t['task_id']}&from=dashboard' style='float:right;background:#198754;color:#fff;border-radius:4px;padding:0.15rem 0.55rem;font-size:0.8rem;text-decoration:none;'>&#10003; Resolve</a>"
                tasks_html += f"<div style='border-bottom:1px solid #eee; padding:0.5rem 0;'>{resolve_btn}{html.escape(t['title'])}{overdue_label}<br>{cust_link} &middot; <small style='color:#888;'>{assigned_to}</small> &middot; <small style='color:{date_color};'>&#128197; {date_str}</small></div>"
        else:
            tasks_html = '<p>Geen openstaande taken.</p>'
        body += f'''<div class="card">
            <div class="section-title">Openstaande taken (komende 14 dagen + verlopen) <a href="/tasks/archive" style="float:right;font-size:0.85rem;color:#5C7A5A;font-weight:normal;">&#128451; Archief voltooide taken</a></div>
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
    def render_user_list(self, users: List[sqlite3.Row], username: str, current_user_id: int = 1) -> None:
        """Render a list of users for admin."""
        body = html_header('Gebruikersbeheer', True, username, current_user_id)
        body += '<h2 class="mt-4">Gebruikers</h2>'
        body += '<div class="card">'
        body += '<div class="section-title">Huidige gebruikers</div>'
        if users:
            for user in users:
                is_admin_user = bool(user['id'] == 1 or user['is_admin'])
                is_comm_user = bool(user['is_comm'])
                is_gov_user = bool(user['is_governance'])
                is_protected = user['id'] == 1  # id=1 can never be demoted
                delete_btn = '' if is_protected else f'<a href="/users/delete?id={user["id"]}" class="btn btn-sm btn-danger" style="margin-left:0.5rem;" onclick="return confirm(\'Weet je zeker dat je {html.escape(user["username"])} wilt verwijderen?\');">Verwijder</a>'
                if not is_protected:
                    if is_admin_user:
                        toggle_btn = f'<a href="/users/toggle-admin?id={user["id"]}" class="btn btn-sm btn-secondary" style="margin-left:0.5rem;" onclick="return confirm(\'Admin-rechten verwijderen van {html.escape(user["username"])}?\');">Verwijder admin</a>'
                    else:
                        toggle_btn = f'<a href="/users/toggle-admin?id={user["id"]}" class="btn btn-sm" style="margin-left:0.5rem;background:#5C7A5A;color:#fff;" onclick="return confirm(\'{html.escape(user["username"])} admin maken?\');">Maak admin</a>'
                    if is_comm_user:
                        comm_btn = f'<a href="/users/toggle-comm?id={user["id"]}" class="btn btn-sm btn-secondary" style="margin-left:0.5rem;" onclick="return confirm(\'Comm-team verwijderen van {html.escape(user["username"])}?\');">&#128101; Comm uit</a>'
                    else:
                        comm_btn = f'<a href="/users/toggle-comm?id={user["id"]}" class="btn btn-sm" style="margin-left:0.5rem;background:#7b1fa2;color:#fff;" onclick="return confirm(\'{html.escape(user["username"])} aan comm-team toevoegen?\');">&#128101; Comm aan</a>'
                    if is_gov_user:
                        gov_btn = f'<a href="/users/toggle-governance?id={user["id"]}" class="btn btn-sm btn-secondary" style="margin-left:0.5rem;" onclick="return confirm(\'Governance verwijderen van {html.escape(user["username"])}?\');">&#9881; Gov uit</a>'
                    else:
                        gov_btn = f'<a href="/users/toggle-governance?id={user["id"]}" class="btn btn-sm" style="margin-left:0.5rem;background:#1565c0;color:#fff;" onclick="return confirm(\'{html.escape(user["username"])} aan governance toevoegen?\');">&#9881; Gov aan</a>'
                else:
                    toggle_btn = ''
                    comm_btn = ''
                    gov_btn = ''
                body += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0; display:flex; justify-content:space-between; align-items:center;">
                    <div>
                        <strong>{html.escape(user['username'])}</strong> ({html.escape(user['email'])})
                        {'<span style="font-size:0.75rem;background:#5C7A5A;color:#fff;border-radius:4px;padding:0.1rem 0.4rem;margin-left:0.5rem;">admin</span>' if is_admin_user else ''}
                        {'<span style="font-size:0.75rem;background:#7b1fa2;color:#fff;border-radius:4px;padding:0.1rem 0.4rem;margin-left:0.5rem;">&#128101; comm</span>' if is_comm_user else ''}
                        {'<span style="font-size:0.75rem;background:#1565c0;color:#fff;border-radius:4px;padding:0.1rem 0.4rem;margin-left:0.5rem;">&#9881; gov</span>' if is_gov_user else ''}
                        <div style="font-size:0.8rem; color:#666;">Aangemaakt op {(user['created_at'] or '')[:10]}</div>
                    </div>
                    <div>
                        <a href="/users/profile?id={user['id']}" class="btn btn-sm btn-secondary">Profiel</a>
                        {toggle_btn}
                        {comm_btn}
                        {gov_btn}
                        {delete_btn}
                    </div>
                </div>'''
        else:
            body += '<p>Er zijn nog geen gebruikers.</p>'
        body += '</div>'
        body += f'''<div class="card">
            <div class="section-title">Nieuwe gebruiker toevoegen</div>
            <form method="post" action="/users/add">
                {self._csrf_input(current_user_id)}
                <label>Gebruikersnaam<br><input type="text" name="username" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
                <label>E‑mail<br><input type="email" name="email" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
                <label>Wachtwoord<br><input type="password" name="password" required style="width:100%; padding:0.4rem; margin-bottom:0.3rem;"></label>
                <button type="submit" style="background-color:#5C7A5A; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Gebruiker toevoegen</button>
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
        <small style="color:#666;">Klant: <a href="/customers/view?id={task["customer_id"]}" style="color:#5C7A5A;">{html.escape(task["customer_name"])}</a></small></p>
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
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            # Recent conversations for messaging section (only show when viewing own profile)
            cur.execute('''
                SELECT other_id, u.username AS other_name,
                       MAX(m.created_at) AS last_at,
                       SUM(CASE WHEN m.recipient_id=? AND m.is_read=0 THEN 1 ELSE 0 END) AS unread,
                       (SELECT content FROM messages m2
                        WHERE (m2.sender_id=m.other_id AND m2.recipient_id=?)
                           OR (m2.sender_id=? AND m2.recipient_id=m.other_id)
                        ORDER BY m2.created_at DESC LIMIT 1) AS last_content
                FROM (
                    SELECT CASE WHEN sender_id=? THEN recipient_id ELSE sender_id END AS other_id,
                           id, created_at, recipient_id, is_read
                    FROM messages WHERE sender_id=? OR recipient_id=?
                ) m
                JOIN users u ON u.id = m.other_id
                GROUP BY other_id ORDER BY last_at DESC LIMIT 10
            ''', (pid, pid, pid, pid, pid, pid))
            msg_convs = cur.fetchall()
            cur.execute("SELECT COUNT(*) FROM messages WHERE recipient_id=? AND is_read=0", (pid,))
            msg_unread_total = cur.fetchone()[0]
        body = html_header(f'Profiel: {profile_user["username"]}', True, viewer_username, viewer_id)
        body += f'<h2 class="mt-4">&#128100; {html.escape(profile_user["username"])}</h2>'
        body += f'<p style="color:#666;">{html.escape(profile_user["email"])} &middot; Account aangemaakt op {profile_user["created_at"][:10]}</p>'
        # Stats row
        overdue = [t for t in open_tasks if t['due_date'] and t['due_date'] < datetime.date.today().isoformat()]
        body += f'''<div style="display:flex;gap:1rem;margin-bottom:1rem;flex-wrap:wrap;">
            <div class="card" style="flex:1;min-width:140px;text-align:center;">
                <div style="font-size:2rem;font-weight:bold;color:#5C7A5A;">{len(open_tasks)}</div>
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
                    <a href="/customers/view?id={t['customer_id']}" style="color:#5C7A5A;font-weight:bold;">{html.escape(t['customer_name'])}</a>
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
                    <td><a href="/customers/view?id={c['id']}" style="color:#5C7A5A;">{html.escape(c['name'])}</a></td>
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
                    &middot; <a href="/customers/view?id={i['customer_id']}" style="color:#5C7A5A;">{html.escape(i['customer_name'])}</a>{note_part}
                </div>'''
        else:
            body += '<p>Nog geen interacties geregistreerd.</p>'
        body += '</div>'
        # Notes by this user (collapsible)
        body += '<details style="margin-bottom:1rem;"><summary style="cursor:pointer;font-weight:bold;padding:0.6rem 1rem;background:#fff;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">&#128221; Toegevoegde notities (' + str(len(user_notes)) + ')</summary><div class="card" style="margin-top:0.25rem;">'
        if user_notes:
            for n in user_notes:
                content_val = n['content'] or ''
                snippet = (content_val[:120] + '…') if len(content_val) > 120 else content_val
                body += f'''<div style="border-bottom:1px solid #eee;padding:0.4rem 0;">
                    <small style="color:#888;">{n['created_at'][:10]}</small>
                    &middot; <a href="/customers/view?id={n['customer_id']}" style="color:#5C7A5A;">{html.escape(n['customer_name'])}</a>
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
                    <a href="/customers/view?id={c['id']}" style="color:#5C7A5A;font-weight:bold;">{html.escape(c['name'])}</a>
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
        # Berichten sectie (alleen op eigen profiel)
        if viewer_id == pid:
            unread_badge = f' <span style="background:#5C7A5A;color:#fff;border-radius:50%;font-size:0.75rem;font-weight:bold;min-width:20px;height:20px;line-height:20px;text-align:center;display:inline-block;margin-left:0.3rem;">{msg_unread_total}</span>' if msg_unread_total else ''
            body += f'<div class="card"><div class="section-title">&#128172; Berichten{unread_badge} <a href="/messages" style="float:right;font-size:0.85rem;font-weight:normal;color:#5C7A5A;">Alle gesprekken</a></div>'
            if msg_convs:
                for c in msg_convs:
                    unread = c['unread'] or 0
                    snippet = html.escape((c['last_content'] or '')[:60])
                    row_bg = 'background:#EDF3EC;' if unread else ''
                    badge = f'<span style="background:#5C7A5A;color:#fff;border-radius:50%;font-size:0.72rem;font-weight:bold;min-width:18px;height:18px;line-height:18px;text-align:center;display:inline-block;margin-left:0.3rem;">{unread}</span>' if unread else ''
                    body += f'''<a href="/messages/conversation?with={c["other_id"]}" style="display:flex;align-items:center;padding:0.6rem 0;border-bottom:1px solid #eee;text-decoration:none;color:inherit;{row_bg}">
                        <div style="width:32px;height:32px;border-radius:50%;background:#5C7A5A;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:0.9rem;margin-right:0.7rem;flex-shrink:0;">{html.escape(c["other_name"][0].upper())}</div>
                        <div style="flex:1;min-width:0;">
                            <span style="font-weight:{"bold" if unread else "normal"};">{html.escape(c["other_name"])}</span>{badge}
                            <div style="font-size:0.82rem;color:#888;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{snippet}</div>
                        </div>
                        <div style="font-size:0.75rem;color:#aaa;margin-left:0.5rem;">{(c["last_at"] or "")[:16]}</div>
                    </a>'''
            else:
                body += '<p style="color:#888;">Nog geen berichten.</p>'
            # Nieuw gesprek starten
            with sqlite3.connect(DB_PATH, timeout=10) as _conn:
                _conn.row_factory = sqlite3.Row
                _cur = _conn.cursor()
                _cur.execute('SELECT id, username FROM users WHERE id != ? ORDER BY username', (pid,))
                _all_users = _cur.fetchall()
            user_opts = ''.join(f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in _all_users)
            body += f'''<form method="GET" action="/messages/conversation" style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;margin-top:0.75rem;">
                <select name="with" class="form-control" style="max-width:200px;" required>
                    <option value="">— Nieuw gesprek... —</option>
                    {user_opts}
                </select>
                <button type="submit" class="btn btn-primary btn-sm">&#43; Start</button>
            </form>'''
            body += '</div>'
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
                body += f'<a href="/fields/delete?id={fid}" style="color:#5C7A5A; float:right;" onclick="return confirm(\'Weet je zeker dat je dit veld wilt verwijderen?\');">Verwijder</a>'
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
                            <div style="width:{width}%; background-color:#5C7A5A; height:100%;"></div>
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

    def render_customers(self, search: str, relation_filter: str = '', sort_col: str = 'name', sort_dir: str = 'asc', verbinding_filter: str = '') -> None:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            conditions = []
            args: list = []
            if search:
                like = f'%{search}%'
                conditions.append('(name LIKE ? OR email LIKE ? OR company LIKE ? OR tags LIKE ? OR role LIKE ? OR verbinding LIKE ?)')
                args.extend([like, like, like, like, like, like])
            if relation_filter:
                conditions.append('relation_type = ?')
                args.append(relation_filter)
            if verbinding_filter:
                conditions.append('verbinding = ?')
                args.append(verbinding_filter)
            where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
            safe_col = sort_col if sort_col in ('name','company','category','relation_type','created_at','role','verbinding') else 'name'
            safe_dir = 'ASC' if sort_dir == 'asc' else 'DESC'
            order_expr = safe_col if safe_col == 'created_at' else f'LOWER(COALESCE({safe_col},\'\'))'
            cur.execute(f'SELECT * FROM customers {where} ORDER BY {order_expr} {safe_dir}', args)
            customers = cur.fetchall()
            cur.execute('SELECT id, username FROM users ORDER BY username ASC')
            all_users_bulk = cur.fetchall()
            # Build lookup: customer_id → list of accountmanager usernames
            cur.execute('''SELECT cu.customer_id, u.username FROM customer_users cu
                           JOIN users u ON cu.user_id = u.id''')
            _am_map: dict = {}
            for _r in cur.fetchall():
                _am_map.setdefault(_r[0], []).append(_r[1])
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
            if verbinding_filter: parts.append(f'verbinding={urllib.parse.quote(verbinding_filter)}')
            if extra_params: parts.append(extra_params)
            return '/customers' + ('?' + '&'.join(parts) if parts else '')
        # Filter buttons Intern / Extern
        def _tab(label, val):
            active = relation_filter == val
            base_style = 'display:inline-block;padding:0.35rem 1.1rem;border-radius:20px;border:2px solid #5C7A5A;text-decoration:none;font-size:0.9rem;margin-right:0.4rem;'
            style = base_style + ('background:#5C7A5A;color:#fff;font-weight:bold;' if active else 'color:#5C7A5A;')
            extra = (f'&q={q_enc}' if search else '') + (f'&sort={sort_col}&dir={sort_dir}' if sort_col != 'name' or sort_dir != 'asc' else '') + (f'&verbinding={urllib.parse.quote(verbinding_filter)}' if verbinding_filter else '')
            href = f'/customers?relatie={val}' + extra
            return f'<a href="{href}" style="{style}">{label}</a>'
        alle_active = not relation_filter
        alle_style = 'display:inline-block;padding:0.35rem 1.1rem;border-radius:20px;border:2px solid #5C7A5A;text-decoration:none;font-size:0.9rem;margin-right:0.4rem;'
        alle_style += 'background:#5C7A5A;color:#fff;font-weight:bold;' if alle_active else 'color:#5C7A5A;'
        alle_href = '/customers' + (f'?q={q_enc}' if search else '')
        filter_btns = f'<a href="{alle_href}" style="{alle_style}">Alle</a>' + _tab('Extern', 'extern') + _tab('Intern', 'intern')
        # Verbinding filter buttons
        def _vtab(label, val, color):
            active = verbinding_filter == val
            base = 'display:inline-block;padding:0.25rem 0.9rem;border-radius:20px;text-decoration:none;font-size:0.85rem;margin-right:0.3rem;border:2px solid ' + color + ';'
            style = base + (f'background:{color};color:#fff;font-weight:bold;' if active else f'color:{color};')
            extra = (f'&q={q_enc}' if search else '') + (f'&relatie={relation_filter}' if relation_filter else '') + (f'&sort={sort_col}&dir={sort_dir}' if sort_col != 'name' or sort_dir != 'asc' else '')
            href = f'/customers?verbinding={urllib.parse.quote(val)}' + extra
            return f'<a href="{href}" style="{style}">{label}</a>'
        vb_reset_extra = (f'?q={q_enc}' if search else '') + (f'{"&" if search else "?"}relatie={relation_filter}' if relation_filter else '')
        vb_reset = f'<a href="/customers{vb_reset_extra}" style="display:inline-block;padding:0.25rem 0.9rem;border-radius:20px;text-decoration:none;font-size:0.85rem;margin-right:0.3rem;border:2px solid #aaa;{"background:#aaa;color:#fff;font-weight:bold;" if not verbinding_filter else "color:#aaa;"}">Alle</a>'
        verbinding_btns = vb_reset + _vtab('Ambassadeur', 'ambassadeur', '#2e7d32') + _vtab('Betrokken', 'betrokken', '#1565c0') + _vtab('Niet betrokken', 'niet betrokken', '#888')
        body += f'''
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.75rem;margin-top:1rem;margin-bottom:0.5rem;">
            <div>{filter_btns}</div>
            <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;">
                <form method="get" class="d-flex" role="search" style="margin:0;">
                    {'<input type="hidden" name="relatie" value="' + relation_filter + '">' if relation_filter else ''}
                    {'<input type="hidden" name="verbinding" value="' + verbinding_filter + '">' if verbinding_filter else ''}
                    {'<input type="hidden" name="sort" value="' + sort_col + '"><input type="hidden" name="dir" value="' + sort_dir + '">' if sort_col != 'name' or sort_dir != 'asc' else ''}
                    <input class="form-control me-2" type="search" name="q" placeholder="Zoeken" value="{q_enc}" style="min-width:180px;">
                    <button class="btn btn-outline-success" type="submit">Zoek</button>
                </form>
                <a href="/customers/add" class="btn btn-primary">+ Toevoegen</a>
            </div>
        </div>
        <div style="margin-bottom:0.75rem;">{verbinding_btns}</div>'''
        if is_admin(uid):
            admin_user_opts = '<option value="">-- Kies gebruiker --</option>' + ''.join(f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in all_users_bulk)
            body += f'''<div style="background:#f8f9fa;border:1px solid #dee2e6;border-radius:6px;padding:0.5rem 1rem;margin-bottom:0.5rem;display:flex;align-items:center;gap:0.5rem;flex-wrap:wrap;">
                <span style="font-size:0.85rem;color:#555;">&#128279; Koppel lege accountmanagers aan:</span>
                <form method="POST" action="/customers/bulk-link-empty" style="display:flex;gap:0.5rem;align-items:center;" onsubmit="return confirm('Alle relaties zonder accountmanager koppelen aan de gekozen gebruiker?');">
                    <select name="user_id" style="padding:0.25rem 0.5rem;border:1px solid #ced4da;border-radius:4px;font-size:0.85rem;" required>{admin_user_opts}</select>
                    <button type="submit" class="btn btn-sm btn-primary">Koppelen</button>
                </form>
            </div>'''
        # Bulk action bar
        user_opts_bulk = '<option value="">-- Kies gebruiker --</option>' + ''.join(f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in all_users_bulk)
        body += f'''<div id="bulk-bar" style="display:none;background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:0.6rem 1rem;margin-bottom:0.5rem;gap:0.75rem;align-items:center;flex-wrap:wrap;">
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
                    {_th('Type / Rol','role')}
                    {_th('Relatie','relation_type')}
                    {_th('Verbinding','verbinding')}
                    <th>Tags</th>
                    <th>E‑mail</th>
                    <th>Telefoon</th>
                    <th>Accountmanager</th>
                    {_th('Datum','created_at')}
                    <th class="text-end">Acties</th>
                </tr>
            </thead>
            <tbody>
        '''
        if customers:
            for cust in customers:
                tags_display = ', '.join([html.escape(tag.strip()) for tag in (cust['tags'] or '').split(',')]) if cust['tags'] else '-'
                rel = (cust['relation_type'] or 'extern') if 'relation_type' in cust.keys() else 'extern'
                if rel == 'intern':
                    role_v = cust['role'] if 'role' in cust.keys() else None
                    category_display = html.escape(role_v) if role_v else '<span style="color:#aaa;font-style:italic;">—</span>'
                else:
                    category_display = (cust['category'] or 'klant').capitalize() if 'category' in cust.keys() else 'Klant'
                rel_color = '#1565c0' if rel == 'intern' else '#555'
                rel_label = f'<span style="background:{"#e3f0ff" if rel == "intern" else "#f0f0f0"};color:{rel_color};border-radius:12px;padding:0.15rem 0.6rem;font-size:0.82rem;font-weight:bold;">{rel.capitalize()}</span>'
                am_names = _am_map.get(cust['id'], [])
                creator = ', '.join(html.escape(n) for n in am_names) if am_names else '-'
                vb_val = cust['verbinding'] if 'verbinding' in cust.keys() else None
                vb_colors = {'ambassadeur': ('#e8f5e9', '#2e7d32'), 'betrokken': ('#e3f0ff', '#1565c0'), 'niet betrokken': ('#f5f5f5', '#888')}
                if vb_val and vb_val in vb_colors:
                    vb_bg, vb_fg = vb_colors[vb_val]
                    vb_badge = f'<span style="background:{vb_bg};color:{vb_fg};border-radius:12px;padding:0.15rem 0.6rem;font-size:0.82rem;font-weight:bold;">{vb_val.capitalize()}</span>'
                else:
                    vb_badge = '<span style="color:#aaa;font-size:0.82rem;">—</span>'
                body += f'''<tr>
                    <td><input type="checkbox" name="selected_ids" value="{cust['id']}" class="row-cb" onchange="updateBulk()"></td>
                    <td><a href="/customers/view?id={cust['id']}">{html.escape(cust['name'])}</a></td>
                    <td>{html.escape(cust['company'] or '-')}</td>
                    <td>{category_display}</td>
                    <td>{rel_label}</td>
                    <td>{vb_badge}</td>
                    <td>{tags_display}</td>
                    <td>{html.escape(cust['email'])}</td>
                    <td>{html.escape(cust['phone'] or '-')}</td>
                    <td>{creator}</td>
                    <td style="color:#888;font-size:0.85rem;">{(cust['created_at'] or '')[:10]}</td>
                    <td class="text-end">
                        <a href="/customers/edit?id={cust['id']}" class="btn btn-sm btn-secondary">Bewerk</a>
                        <a href="/customers/delete?id={cust['id']}" class="btn btn-sm btn-danger" onclick="return confirm('Weet je zeker dat je deze klant wilt verwijderen?');">Verwijder</a>
                    </td>
                </tr>'''
        else:
            body += '<tr><td colspan="12" class="text-center">Geen klanten gevonden.</td></tr>'
        body += '</tbody></table></form>'
        body += '''<div id="bulk-overlay" style="display:none;position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.5);z-index:9999;align-items:center;justify-content:center;">
    <div style="background:#fff;border-radius:10px;padding:2rem 2.5rem;text-align:center;box-shadow:0 4px 20px rgba(0,0,0,0.3);min-width:280px;">
        <div style="font-size:2rem;margin-bottom:0.5rem;">&#9881;</div>
        <div style="font-size:1.1rem;font-weight:bold;margin-bottom:0.5rem;" id="overlay-title">Bezig...</div>
        <div style="color:#666;font-size:0.9rem;margin-bottom:1rem;" id="overlay-msg">Even geduld, de server verwerkt je actie...</div>
        <div style="background:#eee;border-radius:4px;height:8px;overflow:hidden;">
            <div id="overlay-bar" style="background:#5C7A5A;height:8px;width:0%;transition:width 0.4s linear;border-radius:4px;"></div>
        </div>
        <div style="margin-top:0.6rem;font-size:0.85rem;color:#888;" id="overlay-time"></div>
    </div>
</div>
<script>
function toggleAll(cb){document.querySelectorAll('.row-cb').forEach(c=>c.checked=cb.checked);updateBulk();}
function updateBulk(){
    var checked=document.querySelectorAll('.row-cb:checked').length;
    var bar=document.getElementById('bulk-bar');
    document.getElementById('bulk-count').textContent=checked+\' geselecteerd\';
    bar.style.display=checked>0?\'flex\':\'none\';
}
function bulkAction(action){
    var checked=document.querySelectorAll(\'.row-cb:checked\');
    if(!checked.length){alert(\'Selecteer eerst klanten.\');return;}
    document.getElementById(\'bulk-action-input\').value=action;
    document.getElementById(\'bulk-tag-hidden\').value=document.getElementById(\'bulk-tag-input\').value;
    document.getElementById(\'bulk-user-hidden\').value=document.getElementById(\'bulk-user-select\').value;
    var n=checked.length;
    var labels={intern:\'Instellen als Intern\',extern:\'Instellen als Extern\',add_tag:\'Tag toevoegen\',link_user:\'Gebruiker koppelen\'};
    document.getElementById(\'overlay-title\').textContent=(labels[action]||\'Bezig...\')+\' voor \'+n+\' klant\'+(n===1?\'\':\'en\');
    document.getElementById(\'overlay-time\').textContent=\'Pagina herlaadt automatisch zodra het klaar is.\';
    var overlay=document.getElementById(\'bulk-overlay\');
    overlay.style.display=\'flex\';
    var bar=document.getElementById(\'overlay-bar\');
    var start=Date.now();
    function tick(){var pct=Math.min(90,((Date.now()-start)/3000)*100);bar.style.width=pct+\'%\';if(pct<90)setTimeout(tick,100);}
    tick();
    document.getElementById(\'bulk-form\').submit();
}
</script>'''
        body += html_footer()
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def get_customer(self, customer_id: int) -> Optional[Dict[str, Any]]:
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
        with sqlite3.connect(DB_PATH, timeout=10) as conn_u:
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
        role_val = (customer.get('role') or '') if customer else ''
        verbinding_val = (customer.get('verbinding') or '') if customer else ''
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
                                <label class="form-label">Relatie</label><br>
                                <span class="user-pill">
                                    <input type="radio" name="relation_type" value="extern" id="rel_extern" {'checked' if relation_type != 'intern' else ''} onchange="toggleRolType()">
                                    <label for="rel_extern">Extern</label>
                                </span>
                                <span class="user-pill">
                                    <input type="radio" name="relation_type" value="intern" id="rel_intern" {'checked' if relation_type == 'intern' else ''} onchange="toggleRolType()">
                                    <label for="rel_intern">Intern</label>
                                </span>
                            </div>
                            <div class="mb-3" id="field-category" style="{'display:none' if relation_type == 'intern' else ''}">
                                <label for="category" class="form-label">Type</label>
                                <select class="form-select" id="category" name="category">
                                    <option value="klant" {'selected' if category == 'klant' else ''}>Klant</option>
                                    <option value="netwerk" {'selected' if category == 'netwerk' else ''}>Netwerk</option>
                                </select>
                            </div>
                            <div class="mb-3" id="field-rol" style="{'display:none' if relation_type != 'intern' else ''}">
                                <label for="role_select" class="form-label">Rol</label>
                                <select class="form-select" id="role_select" onchange="toggleRolCustom(this)">
                                    {''.join(f'<option value="{r}" {"selected" if role_val == r else ""}>{r}</option>' for r in ['Docent','Onderzoeker','Manager','Werknemer','Ondersteuner','Partnerdesk','Verbindingspersoon'])}
                                    <option value="anders" {'selected' if role_val and role_val not in ["Docent","Onderzoeker","Manager","Werknemer","Ondersteuner","Partnerdesk","Verbindingspersoon"] else ''}>Anders...</option>
                                </select>
                                <input type="text" class="form-control mt-2" id="role_custom" name="role" placeholder="Vul rol in..."
                                    value="{html.escape(role_val)}"
                                    style="{'display:none' if not role_val or role_val in ['Docent','Onderzoeker','Manager','Werknemer','Ondersteuner','Partnerdesk','Verbindingspersoon'] else ''}">
                            </div>
                            <div class="mb-3">
                                <label for="verbinding" class="form-label">Verbinding</label>
                                <select class="form-select" id="verbinding" name="verbinding">
                                    <option value="" {'selected' if not verbinding_val else ''}>— Kies —</option>
                                    <option value="ambassadeur" {'selected' if verbinding_val == 'ambassadeur' else ''}>Ambassadeur</option>
                                    <option value="betrokken" {'selected' if verbinding_val == 'betrokken' else ''}>Betrokken</option>
                                    <option value="niet betrokken" {'selected' if verbinding_val == 'niet betrokken' else ''}>Niet betrokken</option>
                                </select>
                            </div>
                            <script>
                            function toggleRolType() {{
                                var isIntern = document.getElementById('rel_intern').checked;
                                document.getElementById('field-category').style.display = isIntern ? 'none' : '';
                                document.getElementById('field-rol').style.display = isIntern ? '' : 'none';
                                if (isIntern) {{
                                    var sel = document.getElementById('role_select');
                                    toggleRolCustom(sel);
                                }}
                            }}
                            function toggleRolCustom(sel) {{
                                var custom = document.getElementById('role_custom');
                                if (sel.value === 'anders') {{
                                    custom.style.display = '';
                                    custom.required = true;
                                }} else {{
                                    custom.style.display = 'none';
                                    custom.required = false;
                                    custom.value = sel.value;
                                }}
                            }}
                            // Set role_custom value on form submit when a preset is selected
                            (function() {{
                                var form = document.querySelector('form[method="post"]');
                                if (form) {{
                                    form.addEventListener('submit', function() {{
                                        var isIntern = document.getElementById('rel_intern').checked;
                                        if (isIntern) {{
                                            var sel = document.getElementById('role_select');
                                            var custom = document.getElementById('role_custom');
                                            if (sel.value !== 'anders') {{
                                                custom.value = sel.value;
                                            }}
                                        }}
                                    }});
                                }}
                            }})();
                            </script>
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
                                    .user-pill label{{display:inline-block;padding:0.35rem 1rem;border-radius:20px;border:2px solid #5C7A5A;color:#5C7A5A;cursor:pointer;margin:0.25rem 0.25rem 0.25rem 0;font-size:0.9rem;transition:background 0.15s,color 0.15s}}
                                    .user-pill label:hover{{background:#EDF3EC}}
                                    .user-pill input[type=checkbox]:checked+label,.user-pill input[type=radio]:checked+label{{background:#5C7A5A;color:#fff;font-weight:bold}}
                                </style>
                                <div style="margin-top:0.3rem;">
                                    {users_checkboxes_html}
                                </div>
                                <small class="form-text text-muted">Klik op een naam om die accountmanager te koppelen. Gekoppelde managers ontvangen automatisch een herinnering (intern: 6 maanden, extern: 2 maanden).</small>
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
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
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
            <p><span class="icon">&#128221;</span>{'Rol: ' + html.escape(customer.get('role') or '-') if (customer.get('relation_type') or 'extern') == 'intern' else 'Type: ' + category_display} &middot; {(customer.get('relation_type') or 'extern').capitalize()}</p>
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
            <button type="submit" style="background-color:#5C7A5A; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Taak toevoegen</button>
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
                actions.append(f"<a href='/tasks/delete?id={task['task_id']}&customer_id={customer['id']}' style='color:#5C7A5A;' onclick=\"return confirm('Weet je zeker dat je deze taak wilt verwijderen?');\">Verwijder</a>")
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
            <button type="submit" style="background-color:#5C7A5A; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Opslaan</button>
        </form>'''
        if notes:
            for note in notes:
                author_part = f"door {html.escape(note['author'])}" if note['author'] else ''
                notes_section += f'''<div style="border-bottom:1px solid #eee; padding:0.5rem 0;">
                    {html.escape(note['content'])}
                    <div style="font-size:0.8rem; color:#666;">{note['created_at']} {author_part}</div>
                    <div style="font-size:0.8rem;"><a href='/notes/delete?id={note['note_id']}&customer_id={customer['id']}' style='color:#5C7A5A;' onclick="return confirm('Weet je zeker dat je deze notitie wilt verwijderen?');">Verwijder</a></div>
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
            <button type="submit" style="background-color:#5C7A5A; color:#fff; border:none; padding:0.5rem 1rem; border-radius:4px;">Interactie toevoegen</button>
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


    def _comm_nav(self, active: str, user_id: int) -> str:
        """Return navigation tabs for the comm dashboard."""
        tabs = [
            ('/comm/board', '&#9776; Board', 'board'),
            ('/comm/goals', '&#127945; Doelen', 'goals'),
            ('/comm/week', '&#128197; Week', 'week'),
            ('/comm/overview', '&#128203; Overzicht', 'overview'),
            ('/comm/events-gov', '&#127937; Events Gov', 'events-gov'),
            (f'/comm/profile?id={user_id}', '&#128100; Mijn profiel', 'profile'),
            ('/comm/search', '&#128269; Zoeken', 'search'),
            ('/comm/dates', '&#128197; Datums', 'dates'),
            ('/comm/content', '&#128240; Content', 'content'),
            ('/comm/archived', '&#128452; Archief', 'archived'),
        ]
        parts = []
        for href, label, key in tabs:
            if key == active:
                parts.append(f'<a href="{href}" style="background:#5C7A5A;color:#fff;padding:0.4rem 0.85rem;border-radius:4px;text-decoration:none;font-weight:bold;font-size:0.9rem;">{label}</a>')
            else:
                parts.append(f'<a href="{href}" style="background:#fff;color:#5C7A5A;border:2px solid #5C7A5A;padding:0.3rem 0.85rem;border-radius:4px;text-decoration:none;font-size:0.9rem;">{label}</a>')
        return '<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin-bottom:1rem;">' + ''.join(parts) + '</div>'

    def _priority_badge(self, priority: str) -> str:
        colors = {'hoog': ('#dc3545', '&#9650; Hoog'), 'medium': ('#f57f17', '&#9654; Medium'), 'laag': ('#388e3c', '&#9660; Laag')}
        color, label = colors.get(priority or 'medium', ('#f57f17', '&#9654; Medium'))
        return f'<span style="font-size:0.7rem;background:{color};color:#fff;border-radius:3px;padding:0.1rem 0.35rem;">{label}</span>'

    def _send_html(self, body: str) -> None:
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.end_headers()
        self.wfile.write(body.encode('utf-8'))

    def render_comm_board(self, user_id: int, username: str, user_filter: int = 0) -> None:
        """Render the communication team kanban board with stats."""
        today_iso = datetime.date.today().isoformat()
        week_end = (datetime.date.today() + datetime.timedelta(days=7)).isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            # All active comm tasks
            cur.execute('''
                SELECT ct.id, ct.title, ct.description, ct.status, ct.due_date,
                       ct.goal_id, cg.title AS goal_title, ct.priority, ct.tags,
                       ct.reminder_note, u.username AS assigned_to_name, ct.assigned_to,
                       cb.username AS created_by_name
                FROM comm_tasks ct
                LEFT JOIN users u ON ct.assigned_to = u.id
                LEFT JOIN users cb ON ct.created_by = cb.id
                LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
                WHERE ct.status != 'archief'
                ORDER BY COALESCE(ct.due_date, '9999-12-31') ASC,
                         CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                         ct.created_at DESC
            ''')
            all_tasks = cur.fetchall()
            cur.execute('''
                SELECT u.id, u.username,
                    SUM(CASE WHEN ct.status NOT IN ('klaar','archief') THEN 1 ELSE 0 END) AS open_tasks,
                    SUM(CASE WHEN ct.status = 'klaar' THEN 1 ELSE 0 END) AS done_tasks,
                    SUM(CASE WHEN ct.status NOT IN ('klaar','archief') AND ct.due_date < ? THEN 1 ELSE 0 END) AS overdue_tasks
                FROM users u
                LEFT JOIN comm_tasks ct ON ct.assigned_to = u.id
                WHERE u.is_comm = 1 OR u.is_admin = 1 OR u.id = 1
                GROUP BY u.id ORDER BY u.username ASC
            ''', (today_iso,))
            member_stats = cur.fetchall()
            cur.execute('SELECT id, username FROM users WHERE is_comm=1 OR is_admin=1 OR id=1 ORDER BY username')
            comm_members = cur.fetchall()
            cur.execute("SELECT id, title FROM comm_goals WHERE status='actief' ORDER BY title")
            active_goals = cur.fetchall()
            cur.execute('''SELECT cc.id, cc.title, cc.platform, cc.publish_date, cc.board_status,
                           u.username AS assigned_to_name
                           FROM comm_content cc LEFT JOIN users u ON cc.assigned_to = u.id
                           WHERE cc.board_status IS NOT NULL AND cc.board_status != ''
                           ORDER BY COALESCE(cc.publish_date,'9999-12-31') ASC''')
            board_content = cur.fetchall()

        def _task_matches_filter(t):
            if not user_filter:
                return True
            return t['assigned_to'] == user_filter

        backlog = [t for t in all_tasks if t['status'] == 'backlog' and _task_matches_filter(t)]
        bezig   = [t for t in all_tasks if t['status'] == 'bezig'   and _task_matches_filter(t)]
        klaar   = [t for t in all_tasks if t['status'] == 'klaar'   and _task_matches_filter(t)]
        board_content_backlog = [c for c in board_content if c['board_status'] == 'backlog' and (not user_filter or c.get('assigned_to') == user_filter)]
        board_content_bezig   = [c for c in board_content if c['board_status'] == 'bezig'   and (not user_filter or c.get('assigned_to') == user_filter)]
        board_content_klaar   = [c for c in board_content if c['board_status'] == 'klaar'   and (not user_filter or c.get('assigned_to') == user_filter)]

        body = html_header('Communicatie Board', True, username, user_id)
        body += '<h2 class="mt-4">&#128101; Communicatie Dashboard</h2>'
        body += self._comm_nav('board', user_id)

        # User filter pills
        body += '<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin-bottom:0.75rem;align-items:center;">'
        body += '<span style="font-size:0.82rem;color:#888;margin-right:0.2rem;">Filter:</span>'
        all_pill_style = 'background:#1565c0;color:#fff;' if not user_filter else 'background:#e0e0e0;color:#444;'
        body += f'<a href="/comm/board" style="text-decoration:none;border-radius:14px;padding:0.25rem 0.75rem;font-size:0.82rem;font-weight:bold;{all_pill_style}">Iedereen</a>'
        for m in comm_members:
            active = user_filter == m['id']
            pill_style = 'background:#5C7A5A;color:#fff;' if active else 'background:#EDF3EC;color:#3d5c3b;'
            href = '/comm/board' if active else f'/comm/board?user_filter={m["id"]}'
            body += f'<a href="{href}" style="text-decoration:none;border-radius:14px;padding:0.25rem 0.75rem;font-size:0.82rem;{pill_style}">&#128100; {html.escape(m["username"])}</a>'
        body += '</div>'

        # Reminder banner: overdue + due today + due within 48h
        deadline_48h = (datetime.date.today() + datetime.timedelta(hours=48)).isoformat()
        overdue_tasks  = [t for t in all_tasks if t['status'] not in ('klaar','archief') and t['due_date'] and t['due_date'] < today_iso and t['assigned_to'] == user_id]
        today_tasks    = [t for t in all_tasks if t['status'] not in ('klaar','archief') and t['due_date'] == today_iso and t['assigned_to'] == user_id]
        soon_tasks     = [t for t in all_tasks if t['status'] not in ('klaar','archief') and t['due_date'] and today_iso < t['due_date'] <= deadline_48h and t['assigned_to'] == user_id]
        if overdue_tasks:
            body += f'<div style="background:#ffebee;border-left:4px solid #dc3545;border-radius:4px;padding:0.65rem 1rem;margin-bottom:0.75rem;">&#9888; <strong>{len(overdue_tasks)} verlopen {"taak" if len(overdue_tasks)==1 else "taken"} van jou:</strong> ' + ', '.join(f'<em>{html.escape(t["title"])}</em>' for t in overdue_tasks[:5]) + ('...' if len(overdue_tasks) > 5 else '') + '</div>'
        if today_tasks:
            body += f'<div style="background:#fff8e1;border-left:4px solid #f57f17;border-radius:4px;padding:0.65rem 1rem;margin-bottom:0.75rem;">&#128197; <strong>{len(today_tasks)} {"taak" if len(today_tasks)==1 else "taken"} van jou vervalt vandaag:</strong> ' + ', '.join(f'<em>{html.escape(t["title"])}</em>' for t in today_tasks) + '</div>'
        if soon_tasks:
            body += f'<div style="background:#fff3e0;border-left:4px solid #ef6c00;border-radius:4px;padding:0.65rem 1rem;margin-bottom:0.75rem;">&#9201; <strong>Deadline binnen 48 uur:</strong> ' + ', '.join(f'<em>{html.escape(t["title"])}</em> <span style="font-size:0.8rem;color:#888;">({t["due_date"]})</span>' for t in soon_tasks) + '</div>'

        # Reminder notes banner (tasks with reminder_note assigned to me)
        reminders = [t for t in all_tasks if t['reminder_note'] and t['assigned_to'] == user_id and t['status'] not in ('klaar','archief')]
        if reminders:
            body += '<div style="background:#e8f5e9;border-left:4px solid #388e3c;border-radius:4px;padding:0.65rem 1rem;margin-bottom:0.75rem;">&#128276; <strong>Herinneringen:</strong><ul style="margin:0.3rem 0 0 1.2rem;padding:0;">'
            for r in reminders:
                body += f'<li><em>{html.escape(r["title"])}</em>: {html.escape(r["reminder_note"])}</li>'
            body += '</ul></div>'

        # Stats row
        def _stat(val, label, color='#5C7A5A'):
            return f'<div class="card" style="flex:1;min-width:100px;text-align:center;padding:0.6rem;"><div style="font-size:1.6rem;font-weight:bold;color:{color};">{val}</div><div style="font-size:0.8rem;color:#555;">{label}</div></div>'
        total_open    = len(backlog) + len(bezig)
        total_overdue = sum(1 for t in all_tasks if t['status'] not in ('klaar','archief') and t['due_date'] and t['due_date'] < today_iso)
        body += '<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
        body += _stat(total_open, 'Open taken', '#f57f17')
        body += _stat(total_overdue, 'Verlopen', '#dc3545' if total_overdue else '#388e3c')
        body += _stat(len(klaar), 'Afgerond', '#388e3c')
        body += _stat(len(active_goals), 'Actieve doelen', '#7b1fa2')
        body += '</div>'

        # Per-member stats (collapsible)
        body += '<details style="margin-bottom:0.75rem;"><summary style="cursor:pointer;font-weight:bold;padding:0.55rem 1rem;background:#fff;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">&#128200; Statistieken per teamlid</summary>'
        body += '<div class="card" style="margin-top:0.25rem;"><table><thead><tr><th>Teamlid</th><th>Open</th><th>Verlopen</th><th>Afgerond</th></tr></thead><tbody>'
        for m in member_stats:
            oc = '#dc3545' if (m['overdue_tasks'] or 0) > 0 else '#388e3c'
            body += f'<tr><td><a href="/comm/profile?id={m["id"]}" style="color:#5C7A5A;">{html.escape(m["username"])}</a></td><td>{m["open_tasks"] or 0}</td><td style="color:{oc};font-weight:bold;">{m["overdue_tasks"] or 0}</td><td style="color:#388e3c;">{m["done_tasks"] or 0}</td></tr>'
        body += '</tbody></table></div></details>'

        # Quick add form
        member_opts = '<option value="">Niet toegewezen</option>' + ''.join(
            f'<option value="{m["id"]}"{"selected" if m["id"]==user_id else ""}>{html.escape(m["username"])}</option>' for m in comm_members)
        goal_opts = '<option value="">Geen doel</option>' + ''.join(
            f'<option value="{g["id"]}">{html.escape(g["title"])}</option>' for g in active_goals)
        body += f'''<div class="card" style="margin-bottom:1rem;">
            <div class="section-title">&#43; Nieuwe taak</div>
            <form method="POST" action="/comm/tasks/add" style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
                <div style="flex:2;min-width:160px;"><label style="font-size:0.8rem;font-weight:bold;">Taak *</label><br>
                    <input type="text" name="title" required placeholder="Wat moet er gebeuren?" class="form-control"></div>
                <div style="flex:1;min-width:120px;"><label style="font-size:0.8rem;font-weight:bold;">Toegewezen aan</label><br>
                    <select name="assigned_to" class="form-control">{member_opts}</select></div>
                <div style="flex:1;min-width:120px;"><label style="font-size:0.8rem;font-weight:bold;">Doel</label><br>
                    <select name="goal_id" class="form-control">{goal_opts}</select></div>
                <div style="min-width:120px;"><label style="font-size:0.8rem;font-weight:bold;">Deadline</label><br>
                    <input type="date" name="due_date" class="form-control"></div>
                <div style="min-width:100px;"><label style="font-size:0.8rem;font-weight:bold;">Prioriteit</label><br>
                    <select name="priority" class="form-control">
                        <option value="laag">&#9660; Laag</option>
                        <option value="medium" selected>&#9654; Medium</option>
                        <option value="hoog">&#9650; Hoog</option>
                    </select></div>
                <div style="min-width:100px;"><label style="font-size:0.8rem;font-weight:bold;">Kolom</label><br>
                    <select name="status" class="form-control">
                        <option value="backlog">Backlog</option>
                        <option value="bezig">Bezig</option>
                    </select></div>
                <div><label style="font-size:0.8rem;font-weight:bold;">Tags</label><br>
                    <input type="text" name="tags" placeholder="bijv. social,pr" class="form-control" style="width:110px;"></div>
                <div style="align-self:flex-end;">
                    <button type="submit" class="btn btn-primary">Toevoegen</button></div>
            </form>
        </div>'''

        # Kanban board
        def _task_card(t):
            is_overdue = t['due_date'] and t['due_date'] < today_iso and t['status'] != 'klaar'
            is_today   = t['due_date'] == today_iso
            date_color = '#dc3545' if is_overdue else ('#f57f17' if is_today else '#555')
            border_left = 'border-left:3px solid #dc3545;' if is_overdue else ('border-left:3px solid #f57f17;' if is_today else '')
            date_html  = f'<div style="font-size:0.75rem;color:{date_color};">&#128197; {t["due_date"]}{"  &#9888;" if is_overdue else (" &#9889;" if is_today else "")}</div>' if t['due_date'] else ''
            assigned   = f'<span style="font-size:0.75rem;color:#888;">&#128100; {html.escape(t["assigned_to_name"])}</span>' if t['assigned_to_name'] else ''
            goal_badge = f'<span style="font-size:0.7rem;background:#ede7f6;color:#7b1fa2;border-radius:3px;padding:0.05rem 0.3rem;">&#127945; {html.escape(t["goal_title"])}</span>' if t['goal_title'] else ''
            tags_html  = ''.join(f'<span style="font-size:0.7rem;background:#e3f2fd;color:#1565c0;border-radius:3px;padding:0.05rem 0.3rem;">{html.escape(tag.strip())}</span>' for tag in (t['tags'] or '').split(',') if tag.strip())
            reminder   = f'<div style="font-size:0.75rem;color:#388e3c;margin-top:0.2rem;">&#128276; {html.escape(t["reminder_note"])}</div>' if t['reminder_note'] else ''
            prio_badge = self._priority_badge(t['priority'])
            desc       = f'<div style="font-size:0.78rem;color:#666;margin:0.2rem 0;">{html.escape(t["description"][:80])}{"…" if len(t["description"] or "")>80 else ""}</div>' if t['description'] else ''

            if t['status'] == 'backlog':
                move = f'<a href="/comm/tasks/move?id={t["id"]}&status=bezig" class="btn btn-sm" style="background:#1565c0;color:#fff;font-size:0.7rem;">→ Bezig</a>'
            elif t['status'] == 'bezig':
                move = (f'<a href="/comm/tasks/move?id={t["id"]}&status=backlog" class="btn btn-sm btn-secondary" style="font-size:0.7rem;">← Back</a> '
                        f'<a href="/comm/tasks/move?id={t["id"]}&status=klaar" class="btn btn-sm" style="background:#388e3c;color:#fff;font-size:0.7rem;">&#10003; Klaar</a>')
            else:
                move = (f'<a href="/comm/tasks/move?id={t["id"]}&status=bezig" class="btn btn-sm btn-secondary" style="font-size:0.7rem;">↩ Heropenen</a> '
                        f'<a href="/comm/tasks/archive-done" class="btn btn-sm btn-secondary" style="font-size:0.7rem;" onclick="return confirm(\'Alle afgeronde taken archiveren?\');">&#128452; Archiveer alle</a>')

            edit_btn = f'<a href="/comm/tasks/edit?id={t["id"]}" style="color:#1565c0;font-size:0.78rem;text-decoration:none;margin-right:0.4rem;">&#9998;</a>'
            del_btn  = f'<a href="/comm/tasks/delete?id={t["id"]}" style="color:#dc3545;font-size:0.78rem;text-decoration:none;" onclick="return confirm(\'Taak verwijderen?\');">&#10005;</a>'
            return f'''<div class="comm-card" draggable="true" data-task-id="{t['id']}" data-status="{t['status']}" style="background:#fff;border-radius:6px;padding:0.6rem 0.7rem;margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.1);cursor:grab;{border_left}">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div style="font-weight:bold;font-size:0.88rem;flex:1;">{html.escape(t["title"])}</div>
                    <div style="white-space:nowrap;margin-left:0.5rem;">{edit_btn}{del_btn}</div>
                </div>
                {desc}
                <div style="display:flex;gap:0.3rem;flex-wrap:wrap;margin:0.3rem 0;">{prio_badge}{goal_badge}{tags_html}{assigned}</div>
                {date_html}{reminder}
                <div style="margin-top:0.4rem;display:flex;gap:0.3rem;flex-wrap:wrap;">{move}</div>
            </div>'''

        def _content_board_card(c):
            platform_colors = {'instagram':'#e91e63','linkedin':'#0077b5','website':'#388e3c','email':'#f57f17','overig':'#7b1fa2'}
            col = platform_colors.get(c['platform'], '#7b1fa2')
            asgn = f'<span style="font-size:0.72rem;color:#888;">&#128100; {html.escape(c["assigned_to_name"])}</span>' if c['assigned_to_name'] else ''
            date_html = f'<span style="font-size:0.72rem;color:#888;">&#128197; {c["publish_date"]}</span>' if c['publish_date'] else ''
            bs_opts = ''.join(f'<option value="{v}"{"selected" if c["board_status"]==v else ""}>{l}</option>'
                for v, l in [('backlog','Backlog'),('bezig','Bezig'),('klaar','Klaar'),('','Verwijder uit board')])
            move_sel = f'''<form method="GET" action="/comm/content/board-status" style="display:inline;">
                <input type="hidden" name="id" value="{c["id"]}">
                <select name="status" style="font-size:0.68rem;padding:0.1rem;" onchange="this.form.submit()">{bs_opts}</select></form>'''
            return f'''<div style="background:#fff;border-radius:6px;padding:0.5rem 0.65rem;margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.08);border-left:3px solid {col};opacity:0.92;">
                <div style="font-size:0.82rem;font-weight:bold;">&#128240; {html.escape(c["title"])}</div>
                <div style="display:flex;gap:0.3rem;align-items:center;margin-top:0.25rem;flex-wrap:wrap;">{asgn}{date_html}{move_sel}</div>
            </div>'''

        cs = 'flex:1;min-width:240px;background:#f0f0f0;border-radius:8px;padding:0.75rem;transition:background 0.15s;'
        body += '<div style="display:flex;gap:1rem;flex-wrap:wrap;align-items:flex-start;">'
        backlog_total = len(backlog) + len(board_content_backlog)
        bezig_total   = len(bezig)   + len(board_content_bezig)
        klaar_total   = len(klaar)   + len(board_content_klaar)
        body += f'<div class="comm-column" data-status="backlog" style="{cs}"><div style="font-weight:bold;margin-bottom:0.75rem;">&#128203; Backlog <span style="background:#888;color:#fff;border-radius:10px;padding:0.1rem 0.5rem;font-size:0.78rem;">{backlog_total}</span></div>'
        body += (''.join(_task_card(t) for t in backlog) + ''.join(_content_board_card(c) for c in board_content_backlog)) or '<div class="comm-empty" style="color:#aaa;font-size:0.85rem;padding:1rem 0;text-align:center;">Sleep hier naartoe</div>'
        body += '</div>'
        body += f'<div class="comm-column" data-status="bezig" style="{cs}"><div style="font-weight:bold;margin-bottom:0.75rem;">&#9889; Bezig <span style="background:#1565c0;color:#fff;border-radius:10px;padding:0.1rem 0.5rem;font-size:0.78rem;">{bezig_total}</span></div>'
        body += (''.join(_task_card(t) for t in bezig) + ''.join(_content_board_card(c) for c in board_content_bezig)) or '<div class="comm-empty" style="color:#aaa;font-size:0.85rem;padding:1rem 0;text-align:center;">Sleep hier naartoe</div>'
        body += '</div>'
        body += f'<div class="comm-column" data-status="klaar" style="{cs}"><div style="font-weight:bold;margin-bottom:0.75rem;">&#10003; Klaar <span style="background:#388e3c;color:#fff;border-radius:10px;padding:0.1rem 0.5rem;font-size:0.78rem;">{klaar_total}</span></div>'
        klaar_html = ''.join(_task_card(t) for t in klaar) + ''.join(_content_board_card(c) for c in board_content_klaar)
        if klaar_html:
            body += klaar_html
            body += f'<div style="margin-top:0.5rem;"><a href="/comm/tasks/archive-done" class="btn btn-sm btn-secondary" onclick="return confirm(\'Alle afgeronde taken archiveren?\');">&#128452; Archiveer alle ({len(klaar)})</a></div>'
        else:
            body += '<div class="comm-empty" style="color:#aaa;font-size:0.85rem;padding:1rem 0;text-align:center;">Sleep hier naartoe</div>'
        body += '</div></div>'

        body += '''<script>
(function() {
    var dragging = null;

    function getInsertPoint(col, y) {
        var cards = Array.prototype.slice.call(col.querySelectorAll('.comm-card')).filter(function(c) { return c !== dragging; });
        var result = null;
        var closest = Infinity;
        cards.forEach(function(c) {
            var box = c.getBoundingClientRect();
            var mid = box.top + box.height / 2;
            var dist = y - mid;
            if (dist < 0 && -dist < closest) { closest = -dist; result = c; }
        });
        return result;
    }

    document.querySelectorAll('.comm-card').forEach(function(card) {
        card.addEventListener('dragstart', function(e) {
            dragging = card;
            setTimeout(function() { card.style.opacity = '0.4'; }, 0);
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/plain', card.dataset.taskId);
        });
        card.addEventListener('dragend', function() {
            card.style.opacity = '1';
            dragging = null;
            document.querySelectorAll('.comm-column').forEach(function(col) {
                col.style.background = '#f0f0f0';
                col.style.outline = '';
            });
        });
    });

    document.querySelectorAll('.comm-column').forEach(function(col) {
        col.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            col.style.background = '#dceeff';
            col.style.outline = '2px dashed #1565c0';
            if (!dragging) return;
            // Live preview: insert card at cursor position
            var emptyEl = col.querySelector('.comm-empty');
            if (emptyEl) emptyEl.style.display = 'none';
            var after = getInsertPoint(col, e.clientY);
            if (after) {
                col.insertBefore(dragging, after);
            } else {
                // Append before first non-card child after cards (e.g. archive button div)
                var nonCard = Array.prototype.slice.call(col.children).filter(function(el) {
                    return !el.classList.contains('comm-card') && !el.classList.contains('comm-empty');
                });
                var anchor = nonCard.find(function(el) { return el.tagName === 'DIV' && el.querySelector('a'); });
                if (anchor) { col.insertBefore(dragging, anchor); } else { col.appendChild(dragging); }
            }
        });
        col.addEventListener('dragleave', function(e) {
            if (!col.contains(e.relatedTarget)) {
                col.style.background = '#f0f0f0';
                col.style.outline = '';
                var emptyEl = col.querySelector('.comm-empty');
                if (emptyEl) emptyEl.style.display = '';
            }
        });
        col.addEventListener('drop', function(e) {
            e.preventDefault();
            col.style.background = '#f0f0f0';
            col.style.outline = '';
            var emptyEl = col.querySelector('.comm-empty');
            if (emptyEl) emptyEl.style.display = '';
            if (!dragging) return;
            var taskId = dragging.dataset.taskId;
            var newStatus = col.dataset.status;
            var oldStatus = dragging.dataset.status;
            dragging.dataset.status = newStatus;
            if (newStatus !== oldStatus) {
                fetch('/comm/tasks/move?id=' + taskId + '&status=' + newStatus)
                    .then(function(r) { if (!r.ok) window.location.reload(); })
                    .catch(function() { window.location.reload(); });
            }
        });
    });
})();
</script>'''

        body += html_footer()
        self._send_html(body)

    def render_comm_goals(self, user_id: int, username: str) -> None:
        """Render the goals page with deliverables and progress bars."""
        today_iso = datetime.date.today().isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''SELECT g.id, g.title, g.description, g.target_date, g.status,
                           cb.username AS created_by_name
                           FROM comm_goals g LEFT JOIN users cb ON g.created_by = cb.id
                           ORDER BY g.status ASC, COALESCE(g.target_date,'9999-12-31') ASC''')
            goals = cur.fetchall()
            cur.execute('''SELECT ct.id, ct.title, ct.status, ct.due_date, ct.goal_id, ct.priority,
                           u.username AS assigned_to_name
                           FROM comm_tasks ct LEFT JOIN users u ON ct.assigned_to = u.id
                           WHERE ct.goal_id IS NOT NULL
                           ORDER BY COALESCE(ct.due_date,'9999-12-31') ASC''')
            goal_tasks_raw = cur.fetchall()

        from collections import defaultdict
        tasks_by_goal: dict = defaultdict(list)
        for t in goal_tasks_raw:
            tasks_by_goal[t['goal_id']].append(t)

        body = html_header('Communicatie Doelen', True, username, user_id)
        body += '<h2 class="mt-4">&#127945; Doelen</h2>'
        body += self._comm_nav('goals', user_id)

        actief  = [g for g in goals if g['status'] == 'actief']
        behaald = [g for g in goals if g['status'] == 'behaald']
        body += f'<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;"><div class="card" style="flex:1;min-width:100px;text-align:center;padding:0.6rem;"><div style="font-size:1.6rem;font-weight:bold;color:#7b1fa2;">{len(actief)}</div><div style="font-size:0.8rem;color:#555;">Actief</div></div><div class="card" style="flex:1;min-width:100px;text-align:center;padding:0.6rem;"><div style="font-size:1.6rem;font-weight:bold;color:#388e3c;">{len(behaald)}</div><div style="font-size:0.8rem;color:#555;">Behaald</div></div></div>'

        body += '''<div class="card" style="margin-bottom:1rem;"><div class="section-title">&#43; Nieuw doel</div>
            <form method="POST" action="/comm/goals/add" style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
                <div style="flex:2;min-width:160px;"><label style="font-size:0.8rem;font-weight:bold;">Doel *</label><br>
                    <input type="text" name="title" required placeholder="Bijv. Q2 campagne" class="form-control"></div>
                <div style="flex:2;min-width:180px;"><label style="font-size:0.8rem;font-weight:bold;">Omschrijving</label><br>
                    <input type="text" name="description" placeholder="Toelichting" class="form-control"></div>
                <div style="min-width:130px;"><label style="font-size:0.8rem;font-weight:bold;">Streefdatum</label><br>
                    <input type="date" name="target_date" class="form-control"></div>
                <div><button type="submit" class="btn btn-primary" style="padding:0.45rem 1rem;">Toevoegen</button></div>
            </form></div>'''

        for g in goals:
            is_done  = g['status'] == 'behaald'
            overdue  = g['target_date'] and g['target_date'] < today_iso and not is_done
            dc       = '#dc3545' if overdue else ('#388e3c' if is_done else '#555')
            date_str = f' &#128197; <span style="color:{dc};">{g["target_date"]}{"  &#9888;" if overdue else ""}</span>' if g['target_date'] else ''
            sbadge   = '<span style="background:#e8f5e9;color:#388e3c;border-radius:4px;padding:0.1rem 0.4rem;font-size:0.78rem;">&#10003; Behaald</span>' if is_done else '<span style="background:#ede7f6;color:#7b1fa2;border-radius:4px;padding:0.1rem 0.4rem;font-size:0.78rem;">Actief</span>'
            desc     = f'<div style="font-size:0.85rem;color:#666;margin:0.25rem 0;">{html.escape(g["description"])}</div>' if g['description'] else ''
            act_btn  = (f'<a href="/comm/goals/reopen?id={g["id"]}" class="btn btn-sm btn-secondary">↩ Heropenen</a>' if is_done
                        else f'<a href="/comm/goals/complete?id={g["id"]}" class="btn btn-sm" style="background:#388e3c;color:#fff;" onclick="return confirm(\'Doel behaald markeren?\');">&#10003; Behaald</a>')
            edit_btn = f'<a href="/comm/goals/edit?id={g["id"]}" class="btn btn-sm btn-secondary" style="margin-left:0.4rem;">&#9998; Bewerk</a>'
            del_btn  = f'<a href="/comm/goals/delete?id={g["id"]}" class="btn btn-sm btn-danger" style="margin-left:0.4rem;" onclick="return confirm(\'Doel verwijderen?\');">Verwijder</a>'

            gtasks    = tasks_by_goal.get(g['id'], [])
            done_cnt  = sum(1 for t in gtasks if t['status'] == 'klaar')
            pct       = int(done_cnt / len(gtasks) * 100) if gtasks else 0
            prog_bar  = (f'<div style="margin:0.4rem 0;"><div style="font-size:0.75rem;color:#666;margin-bottom:0.2rem;">Deliverables: {done_cnt}/{len(gtasks)} ({pct}%)</div>'
                         f'<div style="background:#e0e0e0;border-radius:4px;height:7px;"><div style="background:#7b1fa2;border-radius:4px;height:7px;width:{pct}%;"></div></div></div>') if gtasks else ''

            body += f'''<div class="card" style="opacity:{'0.7' if is_done else '1'};">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;">
                    <div style="flex:1;"><strong>&#127945; {html.escape(g["title"])}</strong> {sbadge}{date_str}
                        {desc}{prog_bar}</div>
                    <div style="white-space:nowrap;">{act_btn}{edit_btn}{del_btn}</div>
                </div>'''
            if gtasks:
                body += '<div style="margin-top:0.6rem;border-top:1px solid #eee;padding-top:0.4rem;"><div style="font-size:0.78rem;font-weight:bold;color:#7b1fa2;margin-bottom:0.3rem;">Deliverables</div>'
                for t in gtasks:
                    td   = t['status'] == 'klaar'
                    to   = t['due_date'] and t['due_date'] < today_iso and not td
                    icon = '&#10003;' if td else ('&#9889;' if t['status'] == 'bezig' else '&#9675;')
                    ic   = '#388e3c' if td else ('#1565c0' if t['status'] == 'bezig' else '#888')
                    dat  = f' <small style="color:{"#dc3545" if to else "#888"};">&#128197; {t["due_date"]}</small>' if t['due_date'] else ''
                    asgn = f' <small style="color:#888;">&#128100; {html.escape(t["assigned_to_name"])}</small>' if t['assigned_to_name'] else ''
                    pb   = self._priority_badge(t['priority'])
                    body += f'<div style="padding:0.2rem 0;font-size:0.83rem;border-bottom:1px solid #f5f5f5;display:flex;gap:0.4rem;align-items:center;"><span style="color:{ic};">{icon}</span> <span>{html.escape(t["title"])}</span>{asgn}{dat} {pb}</div>'
                body += '</div>'
            elif not is_done:
                body += f'<div style="margin-top:0.4rem;font-size:0.78rem;color:#aaa;">Nog geen deliverables — voeg taken toe via het <a href="/comm/board" style="color:#7b1fa2;">board</a> en koppel ze aan dit doel.</div>'
            body += '</div>'

        if not goals:
            body += '<div class="card"><p style="color:#888;">Nog geen doelen. Voeg het eerste doel toe!</p></div>'
        body += html_footer()
        self._send_html(body)

    def render_events_gov(self, user_id: int, username: str) -> None:
        """Events Gov board: governance checks for events."""
        today_iso = datetime.date.today().isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''SELECT eg.*, u.username AS assigned_name, cb.username AS created_by_name
                           FROM events_gov_tasks eg
                           LEFT JOIN users u ON eg.assigned_to = u.id
                           LEFT JOIN users cb ON eg.created_by = cb.id
                           ORDER BY CASE eg.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                                    COALESCE(eg.due_date,'9999-12-31') ASC, eg.created_at DESC''')
            all_tasks = cur.fetchall()
            cur.execute('SELECT id, username FROM users ORDER BY username ASC')
            all_users = cur.fetchall()

        statuses = [('open','Open','#fff8e1','#f57f17'), ('in_check','In check','#e3f0ff','#1565c0'), ('klaar','Klaar','#e8f5e9','#388e3c')]
        by_status = {s[0]: [t for t in all_tasks if t['status'] == s[0]] for s in statuses}

        body = html_header('Events Gov', True, username, user_id)
        body += '<h2 class="mt-4">&#127937; Events Gov</h2>'
        body += self._comm_nav('events-gov', user_id)

        # Add form
        user_opts = '<option value="">-- Niemand --</option>' + ''.join(f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in all_users)
        body += f'''<details style="margin-bottom:1rem;">
            <summary style="cursor:pointer;font-weight:bold;padding:0.5rem 0.75rem;background:#fff;border-radius:6px;box-shadow:0 1px 3px rgba(0,0,0,0.1);">+ Governance check toevoegen</summary>
            <div class="card" style="margin-top:0.35rem;">
                <form method="POST" action="/comm/events-gov/add" style="display:flex;flex-direction:column;gap:0.5rem;">
                    <input type="text" name="title" placeholder="Wat moet gecheckt worden? *" class="form-control" required>
                    <input type="text" name="event_context" placeholder="Event of context (bijv. HAN Goes Green 2026)" class="form-control">
                    <textarea name="description" class="form-control" rows="2" placeholder="Toelichting / norm"></textarea>
                    <div style="display:flex;gap:0.5rem;flex-wrap:wrap;">
                        <select name="assigned_to" class="form-control" style="flex:1;">{user_opts}</select>
                        <select name="priority" class="form-control" style="flex:1;">
                            <option value="hoog">Hoog</option>
                            <option value="medium" selected>Medium</option>
                            <option value="laag">Laag</option>
                        </select>
                        <input type="date" name="due_date" class="form-control" style="flex:1;">
                    </div>
                    <button type="submit" class="btn btn-primary" style="align-self:flex-start;">Toevoegen</button>
                </form>
            </div>
        </details>'''

        # Kanban columns
        body += '<div style="display:flex;gap:1rem;align-items:flex-start;flex-wrap:wrap;">'
        for s_key, s_label, s_bg, s_color in statuses:
            tasks = by_status[s_key]
            body += f'<div style="flex:1;min-width:260px;background:{s_bg};border-radius:8px;padding:0.75rem;border-top:4px solid {s_color};">'
            body += f'<div style="font-weight:bold;color:{s_color};margin-bottom:0.6rem;">{s_label} ({len(tasks)})</div>'
            if not tasks:
                body += '<div style="color:#aaa;font-size:0.85rem;font-style:italic;">Leeg</div>'
            for t in tasks:
                is_overdue = t['due_date'] and t['due_date'] < today_iso and s_key != 'klaar'
                date_str = f'<span style="font-size:0.75rem;color:{"#dc3545" if is_overdue else "#888"};">&#128197; {t["due_date"]}</span>' if t['due_date'] else ''
                prio_colors = {'hoog': '#dc3545', 'medium': '#f57f17', 'laag': '#388e3c'}
                prio_dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{prio_colors.get(t["priority"],"#aaa")};margin-right:4px;"></span>'
                ctx = f'<div style="font-size:0.75rem;color:#7b1fa2;margin-bottom:0.2rem;">&#128197; {html.escape(t["event_context"])}</div>' if t['event_context'] else ''
                desc = f'<div style="font-size:0.78rem;color:#555;margin:0.2rem 0;">{html.escape(t["description"])}</div>' if t['description'] else ''
                assigned = f'<span style="font-size:0.75rem;color:#1565c0;">&#128100; {html.escape(t["assigned_name"])}</span>' if t['assigned_name'] else '<span style="font-size:0.75rem;color:#aaa;">Niemand</span>'
                # Status-buttons
                btn_opts = ''.join(
                    f'<button formaction="/comm/events-gov/status" name="status" value="{sk}" style="font-size:0.72rem;padding:0.1rem 0.4rem;border:1px solid {sc};background:#fff;color:{sc};border-radius:3px;cursor:pointer;">{sl}</button>'
                    for sk, sl, _, sc in statuses if sk != s_key
                )
                del_btn = f'<button formaction="/comm/events-gov/delete" style="font-size:0.72rem;padding:0.1rem 0.4rem;border:1px solid #dc3545;background:#fff;color:#dc3545;border-radius:3px;cursor:pointer;" onclick="return confirm(\'Verwijderen?\')">&#10005;</button>'
                body += f'''<div style="background:#fff;border-radius:6px;padding:0.6rem;margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.08);">
                    <div style="font-weight:bold;font-size:0.88rem;margin-bottom:0.2rem;">{prio_dot}{html.escape(t["title"])}</div>
                    {ctx}{desc}
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-top:0.4rem;flex-wrap:wrap;gap:0.2rem;">
                        {assigned} {date_str}
                    </div>
                    <form method="POST" style="display:flex;gap:0.25rem;margin-top:0.4rem;flex-wrap:wrap;">
                        <input type="hidden" name="id" value="{t["id"]}">
                        {btn_opts} {del_btn}
                    </form>
                </div>'''
            body += '</div>'
        body += '</div>'
        body += html_footer()
        self._send_html(body)

    def render_comm_week(self, user_id: int, username: str) -> None:
        """Render a week overview: tasks due in the next 7 days."""
        today      = datetime.date.today()
        today_iso  = today.isoformat()
        week_end   = (today + datetime.timedelta(days=7)).isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority, ct.tags,
                           u.username AS assigned_to_name, cg.title AS goal_title
                           FROM comm_tasks ct
                           LEFT JOIN users u ON ct.assigned_to = u.id
                           LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
                           WHERE ct.status NOT IN ('klaar','archief') AND ct.due_date IS NOT NULL AND ct.due_date <= ?
                           ORDER BY ct.due_date ASC,
                           CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END''', (week_end,))
            week_tasks = cur.fetchall()
            cur.execute('''SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority,
                           u.username AS assigned_to_name, cg.title AS goal_title
                           FROM comm_tasks ct LEFT JOIN users u ON ct.assigned_to = u.id
                           LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
                           WHERE ct.status NOT IN ('klaar','archief') AND (ct.due_date IS NULL OR ct.due_date > ?)
                           AND ct.priority = 'hoog'
                           ORDER BY COALESCE(ct.due_date,'9999-12-31') ASC''', (week_end,))
            hoog_other = cur.fetchall()

        body = html_header('Week Overzicht', True, username, user_id)
        body += f'<h2 class="mt-4">&#128197; Week overzicht — {today.strftime("%d %b %Y")}</h2>'
        body += self._comm_nav('week', user_id)

        overdue = [t for t in week_tasks if t['due_date'] < today_iso]
        vandaag = [t for t in week_tasks if t['due_date'] == today_iso]
        morgen  = [t for t in week_tasks if t['due_date'] == (today + datetime.timedelta(days=1)).isoformat()]
        rest    = [t for t in week_tasks if t['due_date'] > (today + datetime.timedelta(days=1)).isoformat()]

        def _week_row(t, highlight=''):
            pb   = self._priority_badge(t['priority'])
            asgn = f'<span style="color:#888;font-size:0.78rem;">&#128100; {html.escape(t["assigned_to_name"])}</span>' if t['assigned_to_name'] else ''
            goal = f'<span style="font-size:0.72rem;background:#ede7f6;color:#7b1fa2;border-radius:3px;padding:0.05rem 0.3rem;">&#127945; {html.escape(t["goal_title"])}</span>' if t['goal_title'] else ''
            return f'<div style="padding:0.45rem 0;border-bottom:1px solid #eee;display:flex;gap:0.5rem;align-items:center;{highlight}"><span style="min-width:70px;font-size:0.78rem;color:#888;">{t["due_date"] or ""}</span> <span style="flex:1;font-weight:bold;font-size:0.88rem;">{html.escape(t["title"])}</span> {pb} {goal} {asgn}</div>'

        def _section(title, tasks, bg='#fff', border='#ddd'):
            if not tasks:
                return ''
            s = f'<div class="card" style="margin-bottom:0.75rem;border-left:4px solid {border};"><div style="font-weight:bold;margin-bottom:0.5rem;">{title} ({len(tasks)})</div>'
            s += ''.join(_week_row(t) for t in tasks)
            return s + '</div>'

        body += _section('&#9888; Verlopen', overdue, border='#dc3545')
        body += _section('&#128197; Vandaag', vandaag, border='#f57f17')
        body += _section('&#9728; Morgen', morgen, border='#1565c0')
        body += _section('&#128336; Deze week', rest, border='#388e3c')
        if hoog_other:
            body += _section('&#9650; Hoge prioriteit (later)', hoog_other, border='#5C7A5A')
        if not week_tasks and not hoog_other:
            body += '<div class="card"><p style="color:#888;">Geen taken deze week. Goed bezig! &#127881;</p></div>'
        body += html_footer()
        self._send_html(body)

    def render_comm_profile(self, profile_id: int, viewer_id: int, viewer_username: str) -> None:
        """Render personal comm profile: my tasks, my goals, my stats."""
        today_iso = datetime.date.today().isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT id, username, email, created_at FROM users WHERE id = ?', (profile_id,))
            profile_user = cur.fetchone()
            if not profile_user:
                self.respond_redirect('/comm/board')
                return
            cur.execute('''SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority, ct.tags,
                           ct.reminder_note, cg.title AS goal_title
                           FROM comm_tasks ct LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
                           WHERE ct.assigned_to = ? AND ct.status NOT IN ('klaar','archief')
                           ORDER BY CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                           COALESCE(ct.due_date,'9999-12-31') ASC''', (profile_id,))
            my_open = cur.fetchall()
            cur.execute('''SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority
                           FROM comm_tasks ct
                           WHERE ct.assigned_to = ? AND ct.status = 'klaar'
                           ORDER BY ct.created_at DESC LIMIT 10''', (profile_id,))
            my_done = cur.fetchall()
            cur.execute('''SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority,
                           cg.title AS goal_title, ct.reminder_note
                           FROM comm_tasks ct LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
                           WHERE ct.created_by = ? AND (ct.assigned_to IS NULL OR ct.assigned_to != ?)
                           AND ct.status NOT IN ('klaar','archief')
                           ORDER BY COALESCE(ct.due_date,'9999-12-31') ASC''', (profile_id, profile_id))
            created_by_me = cur.fetchall()
            cur.execute('''SELECT g.id, g.title, g.target_date, g.status, g.description
                           FROM comm_goals g WHERE g.created_by = ?
                           ORDER BY g.status ASC, COALESCE(g.target_date,'9999-12-31') ASC''', (profile_id,))
            my_goals = cur.fetchall()
            overdue_cnt = sum(1 for t in my_open if t['due_date'] and t['due_date'] < today_iso)
            cur.execute('SELECT * FROM comm_profiles WHERE user_id=?', (profile_id,))
            ext_profile = cur.fetchone()
            cur.execute('''SELECT eg.id, eg.title, eg.status, eg.due_date, eg.priority, eg.event_context
                           FROM events_gov_tasks eg
                           WHERE eg.assigned_to = ? AND eg.status != 'klaar'
                           ORDER BY CASE eg.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                                    COALESCE(eg.due_date,'9999-12-31') ASC''', (profile_id,))
            my_gov_tasks = cur.fetchall()

        avatar_color = (ext_profile['avatar_color'] if ext_profile and ext_profile['avatar_color'] else '#5C7A5A')
        role_title   = (ext_profile['role_title'] if ext_profile and ext_profile['role_title'] else '')
        bio          = (ext_profile['bio'] if ext_profile and ext_profile['bio'] else '')
        skills_raw   = (ext_profile['skills'] if ext_profile and ext_profile['skills'] else '')
        can_edit     = (profile_id == viewer_id or is_admin(viewer_id))

        body = html_header(f'Profiel: {html.escape(profile_user["username"])}', True, viewer_username, viewer_id)
        body += f'<h2 class="mt-4">&#128100; {html.escape(profile_user["username"])}</h2>'
        body += self._comm_nav('profile', viewer_id)

        # Profile card with extended info
        edit_link = f'<a href="/comm/profile/edit?id={profile_id}" class="btn btn-sm btn-secondary" style="margin-top:0.5rem;">&#9998; Profiel bewerken</a>' if can_edit else ''
        skills_html = ''
        if skills_raw:
            skills_html = '<div style="margin-top:0.5rem;display:flex;gap:0.3rem;flex-wrap:wrap;">' + ''.join(
                f'<span style="background:#EDF3EC;color:#5C7A5A;border-radius:12px;padding:0.15rem 0.6rem;font-size:0.78rem;">{html.escape(s.strip())}</span>'
                for s in skills_raw.split(',') if s.strip()) + '</div>'
        bio_html = f'<div style="font-size:0.85rem;color:#555;margin-top:0.35rem;font-style:italic;">{html.escape(bio)}</div>' if bio else ''
        role_html = f'<div style="font-size:0.88rem;color:#5C7A5A;font-weight:bold;">{html.escape(role_title)}</div>' if role_title else ''
        body += f'''<div class="card" style="display:flex;gap:1rem;align-items:flex-start;flex-wrap:wrap;margin-bottom:1rem;">
            <div style="width:60px;height:60px;border-radius:50%;background:{avatar_color};color:#fff;display:flex;align-items:center;justify-content:center;font-size:1.7rem;font-weight:bold;flex-shrink:0;">{html.escape(profile_user["username"][0].upper())}</div>
            <div style="flex:1;">
                <div style="font-size:1.1rem;font-weight:bold;">{html.escape(profile_user["username"])}</div>
                {role_html}
                <div style="color:#888;font-size:0.82rem;">{html.escape(profile_user["email"])} &middot; Lid sinds {profile_user["created_at"][:10]}</div>
                {bio_html}{skills_html}{edit_link}
            </div>
        </div>'''

        # 48-uurs deadline berekening (voor stats én banners)
        deadline_48h = (datetime.date.today() + datetime.timedelta(hours=48)).isoformat()
        soon_tasks = [t for t in my_open if t['due_date'] and today_iso < t['due_date'] <= deadline_48h]
        today_due  = [t for t in my_open if t['due_date'] == today_iso]

        # Stats row
        def _stat(v, l, c='#5C7A5A'):
            return f'<div class="card" style="flex:1;min-width:90px;text-align:center;padding:0.6rem;"><div style="font-size:1.5rem;font-weight:bold;color:{c};">{v}</div><div style="font-size:0.78rem;color:#555;">{l}</div></div>'
        soon_cnt = len(soon_tasks) + len(today_due)
        body += '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:1rem;">'
        body += _stat(len(my_open), 'Open taken', '#f57f17')
        body += _stat(overdue_cnt, 'Verlopen', '#dc3545' if overdue_cnt else '#388e3c')
        body += _stat(soon_cnt, 'Bijna deadline', '#ef6c00' if soon_cnt else '#388e3c')
        body += _stat(len(my_done), 'Afgerond', '#388e3c')
        body += _stat(len(my_goals), 'Doelen', '#7b1fa2')
        body += '</div>'

        def _task_row(t):
            pb  = self._priority_badge(t['priority'])
            ov  = t['due_date'] and t['due_date'] < today_iso and t['status'] not in ('klaar','archief')
            dc  = '#dc3545' if ov else '#555'
            dat = f'<small style="color:{dc};">&#128197; {t["due_date"]}</small>' if t['due_date'] else ''
            gl  = f'<span style="font-size:0.7rem;background:#ede7f6;color:#7b1fa2;border-radius:3px;padding:0.05rem 0.3rem;">&#127945; {html.escape(t["goal_title"] or "")}</span>' if t['goal_title'] else ''
            return f'<div style="padding:0.35rem 0;border-bottom:1px solid #eee;display:flex;gap:0.4rem;align-items:center;"><span style="flex:1;font-size:0.86rem;">{html.escape(t["title"])}</span>{pb}{gl}{dat}</div>'
        if today_due:
            body += f'<div style="background:#fff8e1;border-left:4px solid #f57f17;border-radius:4px;padding:0.6rem 0.9rem;margin-bottom:0.75rem;">&#128197; <strong>Vervalt vandaag:</strong> ' + ', '.join(f'<em>{html.escape(t["title"])}</em>' for t in today_due) + '</div>'
        if soon_tasks:
            body += f'<div style="background:#fff3e0;border-left:4px solid #ef6c00;border-radius:4px;padding:0.6rem 0.9rem;margin-bottom:0.75rem;">&#9201; <strong>Deadline binnen 48 uur:</strong> ' + ', '.join(f'<em>{html.escape(t["title"])}</em> <span style="font-size:0.8rem;color:#888;">({t["due_date"]})</span>' for t in soon_tasks) + '</div>'

        # Handmatige herinneringsnotities
        reminders_with_note = [t for t in my_open if t['reminder_note']]
        if reminders_with_note:
            body += '<div class="card" style="border-left:4px solid #388e3c;"><div class="section-title">&#128276; Herinneringen</div>'
            for t in reminders_with_note:
                body += f'<div style="padding:0.35rem 0;border-bottom:1px solid #eee;font-size:0.85rem;"><strong>{html.escape(t["title"])}</strong>: <span style="color:#388e3c;">{html.escape(t["reminder_note"])}</span></div>'
            body += '</div>'

        # Open tasks
        body += '<div class="card"><div class="section-title">Open taken</div>'
        body += (''.join(_task_row(t) for t in my_open) if my_open else '<p style="color:#888;font-size:0.85rem;">Geen open taken.</p>') + '</div>'

        # Created by me (assigned to others)
        if created_by_me:
            body += '<div class="card"><div class="section-title">Door mij aangemaakt (toegewezen aan anderen)</div>'
            body += ''.join(_task_row(t) for t in created_by_me) + '</div>'

        # My goals
        body += '<div class="card"><div class="section-title">Mijn doelen</div>'
        if my_goals:
            for g in my_goals:
                done = g['status'] == 'behaald'
                ov   = g['target_date'] and g['target_date'] < today_iso and not done
                dc   = '#dc3545' if ov else ('#388e3c' if done else '#555')
                dat  = f' <small style="color:{dc};">&#128197; {g["target_date"]}</small>' if g['target_date'] else ''
                sb   = '<span style="color:#388e3c;font-size:0.8rem;">&#10003;</span>' if done else '<span style="color:#7b1fa2;font-size:0.8rem;">&#9679;</span>'
                body += f'<div style="padding:0.35rem 0;border-bottom:1px solid #eee;font-size:0.86rem;">{sb} <strong>{html.escape(g["title"])}</strong>{dat}</div>'
        else:
            body += '<p style="color:#888;font-size:0.85rem;">Geen doelen gevonden.</p>'
        body += '</div>'

        # Recent done
        if my_done:
            body += '<div class="card"><div class="section-title">Recent afgerond &#10003;</div>'
            for t in my_done[:5]:
                body += f'<div style="padding:0.3rem 0;border-bottom:1px solid #eee;font-size:0.83rem;color:#388e3c;">&#10003; {html.escape(t["title"])}</div>'
            body += '</div>'

        # Events Gov checks
        if my_gov_tasks:
            body += '<div class="card" style="border-left:4px solid #7b1fa2;"><div class="section-title">&#127937; Events Gov — mijn checks</div>'
            s_labels = {'open': ('Open','#f57f17'), 'in_check': ('In check','#1565c0'), 'klaar': ('Klaar','#388e3c')}
            for t in my_gov_tasks:
                sl, sc = s_labels.get(t['status'], ('?','#888'))
                badge = f'<span style="font-size:0.72rem;background:{sc};color:#fff;border-radius:3px;padding:0.1rem 0.3rem;">{sl}</span>'
                ctx = f' <span style="font-size:0.75rem;color:#7b1fa2;">{html.escape(t["event_context"])}</span>' if t['event_context'] else ''
                date_str = f' <small style="color:#888;">&#128197; {t["due_date"]}</small>' if t['due_date'] else ''
                body += f'<div style="padding:0.35rem 0;border-bottom:1px solid #eee;font-size:0.85rem;display:flex;align-items:center;gap:0.4rem;flex-wrap:wrap;">{badge} <strong>{html.escape(t["title"])}</strong>{ctx}{date_str}</div>'
            body += f'<div style="margin-top:0.5rem;"><a href="/comm/events-gov" style="font-size:0.82rem;color:#7b1fa2;">→ Naar Events Gov board</a></div></div>'

        body += html_footer()
        self._send_html(body)

    def render_comm_overview(self, user_id: int, username: str) -> None:
        """Render a combined overview of upcoming tasks, dates and content items."""
        today = datetime.date.today()
        today_iso = today.isoformat()
        horizon = (today + datetime.timedelta(days=30)).isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''SELECT ct.id, ct.title, ct.due_date, ct.priority, ct.status,
                           u.username AS assigned_to_name
                           FROM comm_tasks ct LEFT JOIN users u ON ct.assigned_to = u.id
                           WHERE ct.status NOT IN ('klaar','archief') AND ct.due_date IS NOT NULL
                           AND ct.due_date <= ? ORDER BY ct.due_date ASC''', (horizon,))
            upcoming_tasks = cur.fetchall()
            cur.execute('''SELECT id, title, date AS event_date, type AS event_type FROM comm_dates
                           WHERE date >= ? AND date <= ?
                           ORDER BY date ASC''', (today_iso, horizon))
            upcoming_dates = cur.fetchall()
            cur.execute('''SELECT cc.id, cc.title, cc.platform, cc.publish_date, cc.status,
                           u.username AS assigned_to_name
                           FROM comm_content cc LEFT JOIN users u ON cc.assigned_to = u.id
                           WHERE cc.publish_date IS NOT NULL AND cc.publish_date >= ?
                           AND cc.publish_date <= ? AND cc.status != 'gepubliceerd'
                           ORDER BY cc.publish_date ASC''', (today_iso, horizon))
            upcoming_content = cur.fetchall()

        body = html_header('Overzicht', True, username, user_id)
        body += '<h2 class="mt-4">&#128203; Overzicht — komende 30 dagen</h2>'
        body += self._comm_nav('overview', user_id)

        # Build combined timeline
        items = []
        for t in upcoming_tasks:
            overdue = t['due_date'] < today_iso
            items.append((t['due_date'], 'task', t, overdue))
        for d in upcoming_dates:
            items.append((d['event_date'], 'date', d, False))
        for c in upcoming_content:
            overdue = c['publish_date'] < today_iso
            items.append((c['publish_date'], 'content', c, overdue))
        items.sort(key=lambda x: x[0])

        if not items:
            body += '<div class="card"><p style="color:#888;">Geen aankomende items in de komende 30 dagen. &#127881;</p></div>'
        else:
            body += '<div class="card" style="padding:0;">'
            prev_date = None
            for date_str, kind, item, overdue in items:
                if date_str != prev_date:
                    days_diff = (datetime.date.fromisoformat(date_str) - today).days
                    if days_diff < 0:
                        day_label = f'&#9888; {abs(days_diff)} dag{"" if abs(days_diff)==1 else "en"} geleden'
                        hdr_color = '#dc3545'
                    elif days_diff == 0:
                        day_label = 'Vandaag'
                        hdr_color = '#f57f17'
                    elif days_diff == 1:
                        day_label = 'Morgen'
                        hdr_color = '#1565c0'
                    else:
                        day_label = f'Over {days_diff} dagen'
                        hdr_color = '#555'
                    d_obj = datetime.date.fromisoformat(date_str)
                    body += f'<div style="background:#f8f9fa;padding:0.4rem 0.9rem;font-size:0.78rem;font-weight:bold;color:{hdr_color};border-bottom:1px solid #eee;">&#128197; {d_obj.strftime("%d %b %Y")} — {day_label}</div>'
                    prev_date = date_str
                if kind == 'task':
                    pb = self._priority_badge(item['priority'])
                    asgn = f'<span style="font-size:0.72rem;color:#888;">&#128100; {html.escape(item["assigned_to_name"])}</span>' if item['assigned_to_name'] else ''
                    ov_style = 'color:#dc3545;' if overdue else ''
                    body += f'<div style="padding:0.45rem 0.9rem;border-bottom:1px solid #f0f0f0;display:flex;gap:0.4rem;align-items:center;"><span style="font-size:0.75rem;background:#fff3e0;color:#f57f17;border-radius:3px;padding:0.05rem 0.3rem;">&#128203; Taak</span> <a href="/comm/board" style="flex:1;font-size:0.86rem;font-weight:bold;{ov_style}color:inherit;text-decoration:none;">{html.escape(item["title"])}</a> {pb} {asgn}</div>'
                elif kind == 'date':
                    type_icons = {'deadline':'&#9888;','milestone':'&#127937;','event':'&#127881;'}
                    icon = type_icons.get(item['event_type'], '&#128197;')
                    body += f'<div style="padding:0.45rem 0.9rem;border-bottom:1px solid #f0f0f0;display:flex;gap:0.4rem;align-items:center;"><span style="font-size:0.75rem;background:#EDF3EC;color:#5C7A5A;border-radius:3px;padding:0.05rem 0.3rem;">{icon} Datum</span> <a href="/comm/dates" style="flex:1;font-size:0.86rem;font-weight:bold;color:inherit;text-decoration:none;">{html.escape(item["title"])}</a></div>'
                elif kind == 'content':
                    platform_icons = {'instagram':'&#128247;','linkedin':'&#128188;','website':'&#127760;','email':'&#128140;','overig':'&#128204;'}
                    icon = platform_icons.get(item['platform'], '&#128204;')
                    asgn = f'<span style="font-size:0.72rem;color:#888;">&#128100; {html.escape(item["assigned_to_name"])}</span>' if item['assigned_to_name'] else ''
                    ov_style = 'color:#dc3545;' if overdue else ''
                    body += f'<div style="padding:0.45rem 0.9rem;border-bottom:1px solid #f0f0f0;display:flex;gap:0.4rem;align-items:center;"><span style="font-size:0.75rem;background:#e3f2fd;color:#1565c0;border-radius:3px;padding:0.05rem 0.3rem;">{icon} Content</span> <a href="/comm/content" style="flex:1;font-size:0.86rem;font-weight:bold;{ov_style}color:inherit;text-decoration:none;">{html.escape(item["title"])}</a> {asgn}</div>'
            body += '</div>'

        # Quick stats
        n_overdue = sum(1 for d, k, i, ov in items if ov)
        n_today   = sum(1 for d, k, i, ov in items if d == today_iso)
        body += f'<div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-top:0.75rem;">'
        body += f'<div class="card" style="flex:1;min-width:90px;text-align:center;padding:0.6rem;"><div style="font-size:1.4rem;font-weight:bold;color:#dc3545;">{n_overdue}</div><div style="font-size:0.78rem;color:#555;">Verlopen</div></div>'
        body += f'<div class="card" style="flex:1;min-width:90px;text-align:center;padding:0.6rem;"><div style="font-size:1.4rem;font-weight:bold;color:#f57f17;">{n_today}</div><div style="font-size:0.78rem;color:#555;">Vandaag</div></div>'
        body += f'<div class="card" style="flex:1;min-width:90px;text-align:center;padding:0.6rem;"><div style="font-size:1.4rem;font-weight:bold;color:#388e3c;">{len(items)}</div><div style="font-size:0.78rem;color:#555;">Totaal (30d)</div></div>'
        body += '</div>'
        body += html_footer()
        self._send_html(body)

    def render_comm_archived(self, user_id: int, username: str) -> None:
        """Render archived (completed) comm tasks."""
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''SELECT ct.id, ct.title, ct.due_date, ct.priority, ct.tags,
                           u.username AS assigned_to_name, cg.title AS goal_title, ct.created_at
                           FROM comm_tasks ct
                           LEFT JOIN users u ON ct.assigned_to = u.id
                           LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
                           WHERE ct.status = 'archief'
                           ORDER BY ct.created_at DESC''')
            archived = cur.fetchall()

        body = html_header('Archief', True, username, user_id)
        body += '<h2 class="mt-4">&#128452; Archief — Afgeronde taken</h2>'
        body += self._comm_nav('archived', user_id)
        body += f'<div class="card"><div class="section-title">{len(archived)} gearchiveerde taken</div>'
        if archived:
            body += '<table><thead><tr><th>Taak</th><th>Toegewezen aan</th><th>Doel</th><th>Prioriteit</th><th>Tags</th></tr></thead><tbody>'
            for t in archived:
                pb   = self._priority_badge(t['priority'])
                asgn = html.escape(t['assigned_to_name'] or '-')
                goal = html.escape(t['goal_title'] or '-')
                tags = html.escape(t['tags'] or '-')
                body += f'<tr><td>{html.escape(t["title"])}</td><td>{asgn}</td><td>{goal}</td><td>{pb}</td><td>{tags}</td></tr>'
            body += '</tbody></table>'
        else:
            body += '<p style="color:#888;">Nog niets gearchiveerd.</p>'
        body += '</div>'
        body += html_footer()
        self._send_html(body)

    def render_comm_search(self, results, comm_members, q, filter_uid, filter_status, filter_priority, user_id, username) -> None:
        """Render comm task search results."""
        today_iso = datetime.date.today().isoformat()
        body = html_header('Zoeken', True, username, user_id)
        body += '<h2 class="mt-4">&#128269; Taken zoeken</h2>'
        body += self._comm_nav('search', user_id)

        member_opts = '<option value="">Alle teamleden</option>' + ''.join(
            f'<option value="{m["id"]}"{"selected" if filter_uid==m["id"] else ""}>{html.escape(m["username"])}</option>' for m in comm_members)
        stat_opts = ''.join(f'<option value="{v}"{"selected" if filter_status==v else ""}>{l}</option>'
                            for v, l in [('','Alle statussen'),('backlog','Backlog'),('bezig','Bezig'),('klaar','Klaar')])
        prio_opts = ''.join(f'<option value="{v}"{"selected" if filter_priority==v else ""}>{l}</option>'
                            for v, l in [('','Alle prioriteiten'),('hoog','Hoog'),('medium','Medium'),('laag','Laag')])

        body += f'''<div class="card" style="margin-bottom:1rem;">
            <form method="GET" action="/comm/search" style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
                <div style="flex:2;min-width:160px;"><label style="font-size:0.8rem;font-weight:bold;">Zoekterm</label><br>
                    <input type="search" name="q" value="{html.escape(q)}" placeholder="Zoek in titel, omschrijving, tags..." class="form-control"></div>
                <div><label style="font-size:0.8rem;font-weight:bold;">Teamlid</label><br>
                    <select name="uid" class="form-control">{member_opts}</select></div>
                <div><label style="font-size:0.8rem;font-weight:bold;">Status</label><br>
                    <select name="status" class="form-control">{stat_opts}</select></div>
                <div><label style="font-size:0.8rem;font-weight:bold;">Prioriteit</label><br>
                    <select name="priority" class="form-control">{prio_opts}</select></div>
                <div><button type="submit" class="btn btn-primary">Zoeken</button>
                    <a href="/comm/search" style="color:#5C7A5A;font-size:0.85rem;padding:0.4rem 0.5rem;">Wis</a></div>
            </form></div>'''

        body += f'<div class="card"><div class="section-title">Resultaten ({len(results)})</div>'
        if results:
            body += '<table><thead><tr><th>Taak</th><th>Toegewezen aan</th><th>Doel</th><th>Prioriteit</th><th>Status</th><th>Deadline</th></tr></thead><tbody>'
            for t in results:
                ov   = t['due_date'] and t['due_date'] < today_iso and t['status'] not in ('klaar','archief')
                dc   = '#dc3545' if ov else '#555'
                pb   = self._priority_badge(t['priority'])
                sb   = {'backlog':'&#128203; Backlog','bezig':'&#9889; Bezig','klaar':'&#10003; Klaar'}.get(t['status'], t['status'])
                body += f'<tr><td><strong>{html.escape(t["title"])}</strong>'
                if t['tags']:
                    body += f'<br><small style="color:#1565c0;">{html.escape(t["tags"])}</small>'
                body += f'</td><td>{html.escape(t["assigned_to_name"] or "-")}</td><td>{html.escape(t["goal_title"] or "-")}</td><td>{pb}</td><td>{sb}</td><td style="color:{dc};">{t["due_date"] or "-"}</td></tr>'
            body += '</tbody></table>'
        else:
            body += '<p style="color:#888;">Geen resultaten.</p>'
        body += '</div>'
        body += html_footer()
        self._send_html(body)

    def render_comm_task_edit(self, task, comm_members, active_goals, user_id: int, username: str) -> None:
        """Render edit form for a comm task."""
        member_opts = '<option value="">Niet toegewezen</option>' + ''.join(
            f'<option value="{m["id"]}"{"selected" if task["assigned_to"]==m["id"] else ""}>{html.escape(m["username"])}</option>' for m in comm_members)
        goal_opts = '<option value="">Geen doel</option>' + ''.join(
            f'<option value="{g["id"]}"{"selected" if task["goal_id"]==g["id"] else ""}>{html.escape(g["title"])}</option>' for g in active_goals)
        def _sel(name, options, current):
            return ''.join(f'<option value="{v}"{"selected" if current==v else ""}>{l}</option>' for v, l in options)
        stat_opts = _sel('status', [('backlog','Backlog'),('bezig','Bezig'),('klaar','Klaar')], task['status'])
        prio_opts = _sel('priority', [('hoog','&#9650; Hoog'),('medium','&#9654; Medium'),('laag','&#9660; Laag')], task['priority'] or 'medium')

        body = html_header('Taak bewerken', True, username, user_id)
        body += '<h2 class="mt-4">&#9998; Taak bewerken</h2>'
        body += f'''<div class="card" style="max-width:640px;">
            <form method="POST" action="/comm/tasks/edit?id={task["id"]}">
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Taak *</label><br>
                    <input type="text" name="title" value="{html.escape(task["title"])}" required class="form-control"></div>
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Omschrijving</label><br>
                    <textarea name="description" class="form-control" rows="3">{html.escape(task["description"] or "")}</textarea></div>
                <div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.6rem;">
                    <div style="flex:1;min-width:120px;"><label style="font-weight:bold;">Toegewezen aan</label><br>
                        <select name="assigned_to" class="form-control">{member_opts}</select></div>
                    <div style="flex:1;min-width:120px;"><label style="font-weight:bold;">Doel</label><br>
                        <select name="goal_id" class="form-control">{goal_opts}</select></div>
                    <div style="flex:1;min-width:110px;"><label style="font-weight:bold;">Status</label><br>
                        <select name="status" class="form-control">{stat_opts}</select></div>
                    <div style="flex:1;min-width:110px;"><label style="font-weight:bold;">Prioriteit</label><br>
                        <select name="priority" class="form-control">{prio_opts}</select></div>
                </div>
                <div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.6rem;">
                    <div style="flex:1;min-width:130px;"><label style="font-weight:bold;">Deadline</label><br>
                        <input type="date" name="due_date" value="{task["due_date"] or ""}" class="form-control"></div>
                    <div style="flex:2;min-width:150px;"><label style="font-weight:bold;">Tags (komma-gescheiden)</label><br>
                        <input type="text" name="tags" value="{html.escape(task["tags"] or "")}" placeholder="bijv. social,pr,intern" class="form-control"></div>
                </div>
                <div style="margin-bottom:0.8rem;"><label style="font-weight:bold;">Herinnering / notitie</label><br>
                    <input type="text" name="reminder_note" value="{html.escape(task["reminder_note"] or "")}" placeholder="Bijv. Wacht op goedkeuring van Jan" class="form-control"></div>
                <button type="submit" class="btn btn-primary">Opslaan</button>
                <a href="/comm/board" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
            </form></div>'''
        body += html_footer()
        self._send_html(body)

    def render_comm_goal_edit(self, goal, user_id: int, username: str) -> None:
        """Render edit form for a comm goal."""
        body = html_header('Doel bewerken', True, username, user_id)
        body += '<h2 class="mt-4">&#9998; Doel bewerken</h2>'
        body += f'''<div class="card" style="max-width:540px;">
            <form method="POST" action="/comm/goals/edit?id={goal["id"]}">
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Doel *</label><br>
                    <input type="text" name="title" value="{html.escape(goal["title"])}" required class="form-control"></div>
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Omschrijving</label><br>
                    <textarea name="description" class="form-control" rows="3">{html.escape(goal["description"] or "")}</textarea></div>
                <div style="margin-bottom:0.8rem;"><label style="font-weight:bold;">Streefdatum</label><br>
                    <input type="date" name="target_date" value="{goal["target_date"] or ""}" class="form-control"></div>
                <button type="submit" class="btn btn-primary">Opslaan</button>
                <a href="/comm/goals" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
            </form></div>'''
        body += html_footer()
        self._send_html(body)

    def render_comm_task_form(self, user_id: int, username: str) -> None:
        self.respond_redirect('/comm/board')

    def render_comm_goal_form(self, user_id: int, username: str) -> None:
        self.respond_redirect('/comm/goals')

    def render_comm_dates(self, user_id: int, username: str) -> None:
        """Render the important dates page grouped by month."""
        today_iso = datetime.date.today().isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''SELECT d.id, d.title, d.description, d.date, d.type,
                           u.username AS created_by_name
                           FROM comm_dates d LEFT JOIN users u ON d.created_by = u.id
                           ORDER BY d.date ASC''')
            all_dates = cur.fetchall()

        body = html_header('Belangrijke Datums', True, username, user_id)
        body += '<h2 class="mt-4">&#128197; Belangrijke Datums</h2>'
        body += self._comm_nav('dates', user_id)

        # Stats
        past    = sum(1 for d in all_dates if d['date'] < today_iso)
        upcoming = sum(1 for d in all_dates if d['date'] >= today_iso)
        this_week_end = (datetime.date.today() + datetime.timedelta(days=7)).isoformat()
        this_week = sum(1 for d in all_dates if today_iso <= d['date'] <= this_week_end)
        body += f'<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
        body += f'<div class="card" style="flex:1;min-width:100px;text-align:center;padding:0.6rem;"><div style="font-size:1.5rem;font-weight:bold;color:#f57f17;">{this_week}</div><div style="font-size:0.78rem;color:#555;">Deze week</div></div>'
        body += f'<div class="card" style="flex:1;min-width:100px;text-align:center;padding:0.6rem;"><div style="font-size:1.5rem;font-weight:bold;color:#5C7A5A;">{upcoming}</div><div style="font-size:0.78rem;color:#555;">Aankomend</div></div>'
        body += f'<div class="card" style="flex:1;min-width:100px;text-align:center;padding:0.6rem;"><div style="font-size:1.5rem;font-weight:bold;color:#aaa;">{past}</div><div style="font-size:0.78rem;color:#555;">Geweest</div></div>'
        body += '</div>'

        # Add form
        body += '''<div class="card" style="margin-bottom:1rem;"><div class="section-title">&#43; Datum toevoegen</div>
            <form method="POST" action="/comm/dates/add" style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
                <div style="flex:2;min-width:160px;"><label style="font-size:0.8rem;font-weight:bold;">Titel *</label><br>
                    <input type="text" name="title" required placeholder="Bijv. Campagne lancering" class="form-control"></div>
                <div style="min-width:130px;"><label style="font-size:0.8rem;font-weight:bold;">Datum *</label><br>
                    <input type="date" name="date" required class="form-control"></div>
                <div style="min-width:120px;"><label style="font-size:0.8rem;font-weight:bold;">Type</label><br>
                    <select name="type" class="form-control">
                        <option value="event">&#127881; Event</option>
                        <option value="deadline">&#9888; Deadline</option>
                        <option value="mijlpaal">&#127937; Mijlpaal</option>
                    </select></div>
                <div style="flex:2;min-width:160px;"><label style="font-size:0.8rem;font-weight:bold;">Omschrijving</label><br>
                    <input type="text" name="description" placeholder="Optioneel" class="form-control"></div>
                <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
            </form></div>'''

        # Type icons and colors
        type_cfg = {
            'event':    ('&#127881;', '#1565c0', '#e3f2fd'),
            'deadline': ('&#9888;',   '#dc3545', '#ffebee'),
            'mijlpaal': ('&#127937;', '#7b1fa2', '#ede7f6'),
        }

        # Group by month
        from collections import OrderedDict
        months: dict = OrderedDict()
        for d in all_dates:
            m = d['date'][:7]  # YYYY-MM
            months.setdefault(m, []).append(d)

        if not all_dates:
            body += '<div class="card"><p style="color:#888;">Nog geen datums. Voeg de eerste toe!</p></div>'

        for month_key, dates in months.items():
            try:
                month_dt = datetime.datetime.strptime(month_key, '%Y-%m')
                month_label = month_dt.strftime('%B %Y').capitalize()
            except Exception:
                month_label = month_key
            is_past_month = month_key < today_iso[:7]
            body += f'<div class="card" style="margin-bottom:0.75rem;opacity:{"0.65" if is_past_month else "1"};"><div style="font-weight:bold;font-size:1rem;margin-bottom:0.5rem;color:{"#aaa" if is_past_month else "#333"};">{"🗓" if not is_past_month else "⏮"} {month_label}</div>'
            for d in dates:
                icon, color, bg = type_cfg.get(d['type'], ('&#128197;', '#555', '#f5f5f5'))
                is_past  = d['date'] < today_iso
                is_today = d['date'] == today_iso
                days_delta = (datetime.date.fromisoformat(d['date']) - datetime.date.today()).days
                if is_today:
                    delta_str = '<span style="color:#f57f17;font-weight:bold;">Vandaag!</span>'
                elif is_past:
                    delta_str = f'<span style="color:#aaa;">{abs(days_delta)} dagen geleden</span>'
                elif days_delta <= 7:
                    delta_str = f'<span style="color:#f57f17;font-weight:bold;">Over {days_delta} dag{"en" if days_delta!=1 else ""}</span>'
                else:
                    delta_str = f'<span style="color:#555;">Over {days_delta} dagen</span>'

                desc = f'<div style="font-size:0.82rem;color:#666;">{html.escape(d["description"])}</div>' if d['description'] else ''
                to_task_btn = f'<a href="/comm/dates/to-task?id={d["id"]}" class="btn btn-sm" style="background:#1565c0;color:#fff;font-size:0.7rem;margin-left:0.3rem;" onclick="return confirm(\'Taak aanmaken van deze datum?\');">→ Taak</a>'
                edit_btn    = f'<a href="/comm/dates/edit?id={d["id"]}" class="btn btn-sm btn-secondary" style="font-size:0.7rem;margin-left:0.3rem;">&#9998;</a>'
                del_btn     = f'<a href="/comm/dates/delete?id={d["id"]}" class="btn btn-sm btn-danger" style="font-size:0.7rem;margin-left:0.3rem;" onclick="return confirm(\'Verwijderen?\');">&#10005;</a>'
                body += f'''<div style="display:flex;align-items:center;gap:0.75rem;padding:0.45rem 0;border-bottom:1px solid #eee;flex-wrap:wrap;">
                    <div style="min-width:90px;font-size:0.82rem;font-weight:bold;color:{color};">{d["date"]}</div>
                    <span style="background:{bg};color:{color};border-radius:4px;padding:0.1rem 0.4rem;font-size:0.75rem;">{icon} {d["type"].capitalize()}</span>
                    <div style="flex:1;"><strong style="font-size:0.9rem;">{html.escape(d["title"])}</strong> {delta_str}{desc}</div>
                    <div style="white-space:nowrap;">{to_task_btn}{edit_btn}{del_btn}</div>
                </div>'''
            body += '</div>'

        body += html_footer()
        self._send_html(body)

    def render_comm_date_edit(self, date_row, user_id: int, username: str) -> None:
        """Render edit form for an important date."""
        type_opts = ''.join(f'<option value="{v}"{"selected" if date_row["type"]==v else ""}>{l}</option>'
                            for v, l in [('event','&#127881; Event'),('deadline','&#9888; Deadline'),('mijlpaal','&#127937; Mijlpaal')])
        body = html_header('Datum bewerken', True, username, user_id)
        body += '<h2 class="mt-4">&#9998; Datum bewerken</h2>'
        body += f'''<div class="card" style="max-width:520px;">
            <form method="POST" action="/comm/dates/edit?id={date_row["id"]}">
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Titel *</label><br>
                    <input type="text" name="title" value="{html.escape(date_row["title"])}" required class="form-control"></div>
                <div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.6rem;">
                    <div style="flex:1;min-width:130px;"><label style="font-weight:bold;">Datum *</label><br>
                        <input type="date" name="date" value="{date_row["date"]}" required class="form-control"></div>
                    <div style="flex:1;min-width:120px;"><label style="font-weight:bold;">Type</label><br>
                        <select name="type" class="form-control">{type_opts}</select></div>
                </div>
                <div style="margin-bottom:0.8rem;"><label style="font-weight:bold;">Omschrijving</label><br>
                    <input type="text" name="description" value="{html.escape(date_row["description"] or "")}" class="form-control"></div>
                <button type="submit" class="btn btn-primary">Opslaan</button>
                <a href="/comm/dates" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
            </form></div>'''
        body += html_footer()
        self._send_html(body)

    def render_comm_content(self, user_id: int, username: str) -> None:
        """Render content calendar with status columns."""
        today_iso = datetime.date.today().isoformat()
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('''SELECT cc.id, cc.title, cc.description, cc.platform, cc.publish_date,
                           cc.status, cc.tags, u.username AS assigned_to_name, cc.assigned_to,
                           cc.board_status
                           FROM comm_content cc LEFT JOIN users u ON cc.assigned_to = u.id
                           ORDER BY COALESCE(cc.publish_date,'9999-12-31') ASC, cc.created_at DESC''')
            all_items = cur.fetchall()
            cur.execute('SELECT id, username FROM users WHERE is_comm=1 OR is_admin=1 OR id=1 ORDER BY username')
            comm_members = cur.fetchall()

        platform_cfg = {
            'instagram': ('&#128247;', '#e91e63', '#EDF3EC'),
            'linkedin':  ('&#128188;', '#0077b5', '#e3f2fd'),
            'website':   ('&#127760;', '#388e3c', '#e8f5e9'),
            'email':     ('&#128140;', '#f57f17', '#fff8e1'),
            'overig':    ('&#128204;', '#7b1fa2', '#ede7f6'),
        }

        idee        = [i for i in all_items if i['status'] == 'idee']
        gepland     = [i for i in all_items if i['status'] == 'gepland']
        klaar       = [i for i in all_items if i['status'] == 'klaar']
        gepubliceerd = [i for i in all_items if i['status'] == 'gepubliceerd']

        body = html_header('Content Kalender', True, username, user_id)
        body += '<h2 class="mt-4">&#128240; Content Kalender</h2>'
        body += self._comm_nav('content', user_id)

        # Stats
        body += f'<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
        for cnt, label, color in [(len(idee),'Ideeën','#888'),(len(gepland),'Gepland','#1565c0'),(len(klaar),'Klaar','#f57f17'),(len(gepubliceerd),'Gepubliceerd','#388e3c')]:
            body += f'<div class="card" style="flex:1;min-width:90px;text-align:center;padding:0.6rem;"><div style="font-size:1.5rem;font-weight:bold;color:{color};">{cnt}</div><div style="font-size:0.78rem;color:#555;">{label}</div></div>'
        body += '</div>'

        # Quick add form
        member_opts = '<option value="">Niet toegewezen</option>' + ''.join(
            f'<option value="{m["id"]}"{"selected" if m["id"]==user_id else ""}>{html.escape(m["username"])}</option>' for m in comm_members)
        body += f'''<div class="card" style="margin-bottom:1rem;"><div class="section-title">&#43; Nieuw content item</div>
            <form method="POST" action="/comm/content/add" style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
                <div style="flex:2;min-width:160px;"><label style="font-size:0.8rem;font-weight:bold;">Titel *</label><br>
                    <input type="text" name="title" required placeholder="Bijv. Zomercampagne post" class="form-control"></div>
                <div style="min-width:110px;"><label style="font-size:0.8rem;font-weight:bold;">Platform</label><br>
                    <select name="platform" class="form-control">
                        <option value="instagram">&#128247; Instagram</option>
                        <option value="linkedin">&#128188; LinkedIn</option>
                        <option value="website">&#127760; Website</option>
                        <option value="email">&#128140; Email</option>
                        <option value="overig">&#128204; Overig</option>
                    </select></div>
                <div style="min-width:110px;"><label style="font-size:0.8rem;font-weight:bold;">Status</label><br>
                    <select name="status" class="form-control">
                        <option value="idee">Idee</option>
                        <option value="gepland">Gepland</option>
                        <option value="klaar">Klaar</option>
                    </select></div>
                <div style="min-width:130px;"><label style="font-size:0.8rem;font-weight:bold;">Publicatiedatum</label><br>
                    <input type="date" name="publish_date" class="form-control"></div>
                <div style="min-width:120px;"><label style="font-size:0.8rem;font-weight:bold;">Toegewezen aan</label><br>
                    <select name="assigned_to" class="form-control">{member_opts}</select></div>
                <div style="min-width:100px;"><label style="font-size:0.8rem;font-weight:bold;">Tags</label><br>
                    <input type="text" name="tags" placeholder="bijv. zomer" class="form-control"></div>
                <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
            </form></div>'''

        def _content_card(item):
            icon, color, bg = platform_cfg.get(item['platform'], ('&#128204;', '#7b1fa2', '#ede7f6'))
            is_overdue = item['publish_date'] and item['publish_date'] < today_iso and item['status'] != 'gepubliceerd'
            date_color = '#dc3545' if is_overdue else '#555'
            date_html  = f'<div style="font-size:0.75rem;color:{date_color};">&#128197; {item["publish_date"]}{"  &#9888;" if is_overdue else ""}</div>' if item['publish_date'] else ''
            assigned   = f'<div style="font-size:0.75rem;color:#888;">&#128100; {html.escape(item["assigned_to_name"])}</div>' if item['assigned_to_name'] else ''
            tags_html  = ''.join(f'<span style="font-size:0.7rem;background:#e3f2fd;color:#1565c0;border-radius:3px;padding:0.05rem 0.3rem;">{html.escape(t.strip())}</span>' for t in (item['tags'] or '').split(',') if t.strip())

            # Status move buttons
            all_statuses = [('idee','Idee'),('gepland','Gepland'),('klaar','Klaar'),('gepubliceerd','&#10003; Gepubliceerd')]
            move_btns = ' '.join(
                f'<a href="/comm/content/move?id={item["id"]}&status={s}" class="btn btn-sm btn-secondary" style="font-size:0.68rem;">{l}</a>'
                for s, l in all_statuses if s != item['status'])

            edit_btn = f'<a href="/comm/content/edit?id={item["id"]}" style="color:#1565c0;font-size:0.78rem;text-decoration:none;margin-right:0.4rem;">&#9998;</a>'
            del_btn  = f'<a href="/comm/content/delete?id={item["id"]}" style="color:#dc3545;font-size:0.78rem;text-decoration:none;" onclick="return confirm(\'Verwijderen?\');">&#10005;</a>'
            task_btn = f'<a href="/comm/content/to-task?id={item["id"]}" class="btn btn-sm" style="background:#1565c0;color:#fff;font-size:0.68rem;margin-top:0.3rem;" onclick="return confirm(\'Als taak toevoegen aan board?\');">→ Taak</a>'

            bs = item['board_status'] or ''
            bs_options = ''.join(
                f'<option value="{v}"{"selected" if bs==v else ""}>{l}</option>'
                for v, l in [('','— Niet in board'),('backlog','Board: Backlog'),('bezig','Board: Bezig'),('klaar','Board: Klaar')])
            board_sel = f'''<form method="GET" action="/comm/content/board-status" style="display:inline;">
                <input type="hidden" name="id" value="{item["id"]}">
                <select name="status" class="form-control" style="display:inline;width:auto;font-size:0.72rem;padding:0.1rem 0.25rem;" onchange="this.form.submit()">
                {bs_options}</select></form>'''
            bs_badge = f' <span style="font-size:0.68rem;background:#1565c0;color:#fff;border-radius:3px;padding:0.05rem 0.3rem;">&#9776; {bs.capitalize()}</span>' if bs else ''

            return f'''<div style="background:#fff;border-radius:6px;padding:0.6rem 0.7rem;margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.1);border-left:3px solid {color};">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                    <div style="font-size:0.88rem;font-weight:bold;flex:1;">{html.escape(item["title"])}{bs_badge}</div>
                    <div>{edit_btn}{del_btn}</div>
                </div>
                <div style="margin:0.2rem 0;"><span style="background:{bg};color:{color};border-radius:3px;padding:0.05rem 0.35rem;font-size:0.73rem;">{icon} {item["platform"].capitalize()}</span> {tags_html}</div>
                {assigned}{date_html}
                <div style="margin-top:0.4rem;display:flex;gap:0.3rem;flex-wrap:wrap;align-items:center;">{move_btns} {task_btn} {board_sel}</div>
            </div>'''

        cs = 'flex:1;min-width:220px;background:#f0f0f0;border-radius:8px;padding:0.75rem;'
        body += '<div style="display:flex;gap:1rem;flex-wrap:wrap;align-items:flex-start;">'
        for col_items, col_label, col_badge_color, col_status in [
            (idee,         '&#128161; Idee',            '#888',    'idee'),
            (gepland,      '&#128197; Gepland',          '#1565c0', 'gepland'),
            (klaar,        '&#9989; Klaar',              '#f57f17', 'klaar'),
            (gepubliceerd, '&#10003; Gepubliceerd',      '#388e3c', 'gepubliceerd'),
        ]:
            body += f'<div style="{cs}"><div style="font-weight:bold;margin-bottom:0.75rem;">{col_label} <span style="background:{col_badge_color};color:#fff;border-radius:10px;padding:0.1rem 0.5rem;font-size:0.78rem;">{len(col_items)}</span></div>'
            body += (''.join(_content_card(i) for i in col_items) or '<div style="color:#aaa;font-size:0.85rem;">Leeg</div>') + '</div>'
        body += '</div>'
        body += html_footer()
        self._send_html(body)

    def render_comm_content_edit(self, item, comm_members, user_id: int, username: str) -> None:
        """Render edit form for a content calendar item."""
        def _sel(current, options):
            return ''.join(f'<option value="{v}"{"selected" if current==v else ""}>{l}</option>' for v, l in options)
        plat_opts = _sel(item['platform'], [('instagram','&#128247; Instagram'),('linkedin','&#128188; LinkedIn'),('website','&#127760; Website'),('email','&#128140; Email'),('overig','&#128204; Overig')])
        stat_opts = _sel(item['status'], [('idee','Idee'),('gepland','Gepland'),('klaar','Klaar'),('gepubliceerd','Gepubliceerd')])
        memb_opts = '<option value="">Niet toegewezen</option>' + ''.join(
            f'<option value="{m["id"]}"{"selected" if item["assigned_to"]==m["id"] else ""}>{html.escape(m["username"])}</option>' for m in comm_members)
        body = html_header('Content bewerken', True, username, user_id)
        body += '<h2 class="mt-4">&#9998; Content item bewerken</h2>'
        body += f'''<div class="card" style="max-width:600px;">
            <form method="POST" action="/comm/content/edit?id={item["id"]}">
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Titel *</label><br>
                    <input type="text" name="title" value="{html.escape(item["title"])}" required class="form-control"></div>
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Omschrijving</label><br>
                    <textarea name="description" class="form-control" rows="3">{html.escape(item["description"] or "")}</textarea></div>
                <div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.6rem;">
                    <div style="flex:1;min-width:110px;"><label style="font-weight:bold;">Platform</label><br>
                        <select name="platform" class="form-control">{plat_opts}</select></div>
                    <div style="flex:1;min-width:110px;"><label style="font-weight:bold;">Status</label><br>
                        <select name="status" class="form-control">{stat_opts}</select></div>
                    <div style="flex:1;min-width:130px;"><label style="font-weight:bold;">Publicatiedatum</label><br>
                        <input type="date" name="publish_date" value="{item["publish_date"] or ""}" class="form-control"></div>
                    <div style="flex:1;min-width:130px;"><label style="font-weight:bold;">Toegewezen aan</label><br>
                        <select name="assigned_to" class="form-control">{memb_opts}</select></div>
                </div>
                <div style="margin-bottom:0.8rem;"><label style="font-weight:bold;">Tags</label><br>
                    <input type="text" name="tags" value="{html.escape(item["tags"] or "")}" placeholder="bijv. zomer,campagne" class="form-control"></div>
                <button type="submit" class="btn btn-primary">Opslaan</button>
                <a href="/comm/content" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
            </form></div>'''
        body += html_footer()
        self._send_html(body)

    def render_comm_profile_edit(self, target_user, profile, user_id: int, username: str) -> None:
        """Render profile edit form with skills, bio, role title, avatar color."""
        p = profile  # may be None
        role_title   = html.escape(p['role_title']   if p and p['role_title']   else '')
        bio          = html.escape(p['bio']           if p and p['bio']          else '')
        skills       = html.escape(p['skills']        if p and p['skills']       else '')
        avatar_color = (p['avatar_color'] if p and p['avatar_color'] else '#5C7A5A')

        color_options = ['#5C7A5A','#7b1fa2','#1565c0','#00695c','#e65100','#37474f','#558b2f','#ad1457']
        color_btns = ''.join(
            f'<label style="cursor:pointer;"><input type="radio" name="avatar_color" value="{c}" {"checked" if avatar_color==c else ""} style="display:none;"><span style="display:inline-block;width:30px;height:30px;border-radius:50%;background:{c};border:3px solid {"#333" if avatar_color==c else "transparent"};margin:2px;"></span></label>'
            for c in color_options)

        body = html_header('Profiel bewerken', True, username, user_id)
        body += f'<h2 class="mt-4">&#9998; Profiel bewerken — {html.escape(target_user["username"])}</h2>'
        body += f'''<div class="card" style="max-width:560px;">
            <form method="POST" action="/comm/profile/edit?id={target_user["id"]}">
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Functietitel</label><br>
                    <input type="text" name="role_title" value="{role_title}" placeholder="Bijv. Social Media Manager" class="form-control"></div>
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Bio / Over mij</label><br>
                    <textarea name="bio" class="form-control" rows="3" placeholder="Korte omschrijving...">{bio}</textarea></div>
                <div style="margin-bottom:0.6rem;"><label style="font-weight:bold;">Skills & vaardigheden</label><br>
                    <input type="text" name="skills" value="{skills}" placeholder="Bijv. Copywriting, Canva, SEO, Video" class="form-control">
                    <div style="font-size:0.75rem;color:#888;margin-top:0.2rem;">Komma-gescheiden. Worden getoond als tags op je profiel.</div></div>
                <div style="margin-bottom:0.8rem;"><label style="font-weight:bold;">Profielkleur</label><br>
                    <div style="margin-top:0.3rem;">{color_btns}</div></div>
                <button type="submit" class="btn btn-primary">Opslaan</button>
                <a href="/comm/profile?id={target_user["id"]}" class="btn btn-secondary" style="margin-left:0.5rem;">Annuleren</a>
            </form></div>'''
        body += html_footer()
        self._send_html(body)


    # ── Governance render helpers ─────────────────────────────────────────

    def _csrf_token(self, user_id: int) -> str:
        """Return CSRF token for this user, generating one if needed."""
        if user_id not in csrf_tokens:
            csrf_tokens[user_id] = secrets.token_hex(32)
        return csrf_tokens[user_id]

    def _csrf_input(self, user_id: int) -> str:
        return f'<input type="hidden" name="csrf_token" value="{self._csrf_token(user_id)}">'

    def _csrf_ok(self, params: dict, user_id: int) -> bool:
        """Return True if the CSRF token in the POST params is valid."""
        submitted = params.get('csrf_token', [''])[0]
        expected = csrf_tokens.get(user_id, '')
        return bool(expected and submitted == expected)

    def _render_password_form(self, user_id: int) -> str:
        return f'''<div class="card" style="max-width:420px;">
            <form method="POST" action="/account/password">
                {self._csrf_input(user_id)}
                <div style="margin-bottom:0.75rem;">
                    <label style="font-weight:bold;display:block;margin-bottom:0.25rem;">Huidig wachtwoord</label>
                    <input type="password" name="current_password" class="form-control" required autocomplete="current-password">
                </div>
                <div style="margin-bottom:0.75rem;">
                    <label style="font-weight:bold;display:block;margin-bottom:0.25rem;">Nieuw wachtwoord</label>
                    <input type="password" name="new_password" class="form-control" required autocomplete="new-password" minlength="6">
                </div>
                <div style="margin-bottom:1rem;">
                    <label style="font-weight:bold;display:block;margin-bottom:0.25rem;">Bevestig nieuw wachtwoord</label>
                    <input type="password" name="confirm_password" class="form-control" required autocomplete="new-password" minlength="6">
                </div>
                <button type="submit" class="btn btn-primary">Wachtwoord wijzigen</button>
            </form>
        </div>'''

    def _gov_nav(self, active: str, user_id: int) -> str:
        """Return navigation tabs for the governance dashboard."""
        tabs = [
            ('/gov/board', '&#9776; Board', 'board'),
            ('/gov/overview', '&#128200; Overzicht', 'overview'),
            ('/gov/profiles', '&#128101; Personen', 'profiles'),
        ]
        if is_admin(user_id):
            tabs.append(('/gov/cards', '&#9881; Kaartbeheer', 'cards'))
        parts = []
        for href, label, key in tabs:
            if key == active:
                parts.append(f'<a href="{href}" style="background:#1565c0;color:#fff;padding:0.4rem 0.85rem;border-radius:4px;text-decoration:none;font-weight:bold;font-size:0.9rem;">{label}</a>')
            else:
                parts.append(f'<a href="{href}" style="background:#fff;color:#1565c0;border:2px solid #1565c0;padding:0.3rem 0.85rem;border-radius:4px;text-decoration:none;font-size:0.9rem;">{label}</a>')
        return '<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin-bottom:1rem;">' + ''.join(parts) + '</div>'

    def _gov_phase_color(self, phase: str) -> str:
        colors = {
            'startpunt': '#888',
            'empathize': '#1565c0',
            'define': '#7b1fa2',
            'ideate': '#f57f17',
            'prototype': '#ef6c00',
            'test': '#388e3c',
            'uittreden': '#5C7A5A',
        }
        return colors.get(phase, '#888')

    def _gov_phase_label(self, phase: str) -> str:
        labels = {
            'startpunt': 'Startpunt',
            'empathize': 'Empathize',
            'define': 'Define',
            'ideate': 'Ideate',
            'prototype': 'Prototype',
            'test': 'Test',
            'uittreden': 'Uittreden',
        }
        return labels.get(phase, phase.capitalize())

    def _gov_tag_pills(self, tags: str) -> str:
        if not tags:
            return ''
        tag_colors = ['#1565c0','#7b1fa2','#388e3c','#ef6c00','#5C7A5A','#37474f','#00695c']
        parts = []
        for i, t in enumerate(tags.split(',')):
            t = t.strip()
            if t:
                color = tag_colors[i % len(tag_colors)]
                parts.append(f'<span style="font-size:0.7rem;background:{color};color:#fff;border-radius:10px;padding:0.1rem 0.45rem;margin-right:0.2rem;">{html.escape(t)}</span>')
        return ''.join(parts)

    def render_gov_board(self, user_id: int, username: str) -> None:
        """Render the governance kanban board."""
        valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM governance_persons ORDER BY name ASC')
            all_persons = cur.fetchall()
            cur.execute('SELECT * FROM governance_card_templates ORDER BY order_index ASC, id ASC')
            all_cards = cur.fetchall()
            cur.execute('SELECT * FROM governance_card_items ORDER BY order_index ASC, id ASC')
            all_items = cur.fetchall()
            # All progress entries
            cur.execute('SELECT person_id, item_id FROM governance_progress')
            completed_by_person = {}
            for row in cur.fetchall():
                completed_by_person.setdefault(row['person_id'], set()).add(row['item_id'])

        # Build lookup structures
        items_by_card = {}
        for item in all_items:
            items_by_card.setdefault(item['card_id'], []).append(item)
        cards_by_phase = {}
        for card in all_cards:
            cards_by_phase.setdefault(card['phase'], []).append(card)

        def relevant_cards_for_person(person):
            pt = (person['project_type'] or '').lower()
            result = []
            for ph in valid_phases:
                for card in cards_by_phase.get(ph, []):
                    if pt and card['project_type'] and card['project_type'] != pt:
                        continue
                    result.append(card)
            return result

        # Compute per-person progress using only their relevant items
        progress_map = {}
        relevant_totals = {}
        for person in all_persons:
            rel_cards = relevant_cards_for_person(person)
            rel_ids = set()
            for card in rel_cards:
                for item in items_by_card.get(card['id'], []):
                    rel_ids.add(item['id'])
            done_ids = completed_by_person.get(person['id'], set())
            relevant_totals[person['id']] = len(rel_ids)
            progress_map[person['id']] = len(done_ids & rel_ids)

        # Group by phase
        phase_map = {p: [] for p in valid_phases}
        for person in all_persons:
            ph = person['phase'] if person['phase'] in valid_phases else 'startpunt'
            phase_map[ph].append(person)

        body = html_header('Governance Board', True, username, user_id)
        body += '<h2 class="mt-4">&#9881; Governance Dashboard</h2>'
        body += self._gov_nav('board', user_id)

        # Stats row
        def _stat(val, label, color='#1565c0'):
            return f'<div class="card" style="flex:1;min-width:100px;text-align:center;padding:0.6rem;"><div style="font-size:1.6rem;font-weight:bold;color:{color};">{val}</div><div style="font-size:0.8rem;color:#555;">{label}</div></div>'

        body += '<div style="display:flex;gap:0.75rem;flex-wrap:wrap;margin-bottom:0.75rem;">'
        body += _stat(len(all_persons), 'Totaal personen', '#1565c0')
        for ph in valid_phases:
            cnt = len(phase_map[ph])
            if cnt:
                body += _stat(cnt, self._gov_phase_label(ph), self._gov_phase_color(ph))
        body += '</div>'

        # Quick add form
        phase_opts_add = ''.join(f'<option value="{p}">{self._gov_phase_label(p)}</option>' for p in valid_phases)
        body += f'''<div class="card" style="margin-bottom:0.75rem;">
            <div class="section-title">&#43; Persoon toevoegen</div>
            <form method="POST" action="/gov/persons/add" style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
                <div><label style="font-size:0.85rem;">Naam</label><br><input type="text" name="name" class="form-control" required style="min-width:160px;"></div>
                <div><label style="font-size:0.85rem;">Tags</label><br><input type="text" name="tags" class="form-control" placeholder="komma-gescheiden" style="min-width:140px;"></div>
                <div><label style="font-size:0.85rem;">Fase</label><br><select name="phase" class="form-control">{phase_opts_add}</select></div>
                <div><label style="font-size:0.85rem;">Projecttype</label><br>
<select name="project_type" class="form-control">
<option value="communicatie">Communicatie</option>
<option value="werkveld">Werkveld</option>
<option value="evenementen">Evenementen</option>
<option value="onderwijs">Onderwijs</option>
</select></div>
                <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
            </form></div>'''

        # Kanban columns — full-width scroll container
        body += '<div style="overflow-x:auto;margin:0 -1rem;padding:0 1rem 1rem 1rem;-webkit-overflow-scrolling:touch;">'
        body += '<div style="display:flex;gap:1rem;width:max-content;padding-bottom:0.5rem;" id="gov-board">'
        for ph in valid_phases:
            color = self._gov_phase_color(ph)
            label = self._gov_phase_label(ph)
            persons = phase_map[ph]
            body += f'''<div class="gov-column" data-phase="{ph}" style="width:240px;flex:0 0 240px;background:#f5f7fa;border-radius:8px;padding:0.75rem;border-top:4px solid {color};" ondragover="event.preventDefault();" ondrop="govDrop(event, '{ph}')">
                <div style="font-weight:bold;color:{color};font-size:0.95rem;margin-bottom:0.65rem;">{label} <span style="background:{color};color:#fff;border-radius:10px;padding:0.05rem 0.5rem;font-size:0.75rem;margin-left:0.3rem;">{len(persons)}</span></div>'''
            for person in persons:
                done = progress_map.get(person['id'], 0)
                rel_total = relevant_totals.get(person['id'], 0)
                pct = round(done / rel_total * 100) if rel_total else 0
                tag_html = self._gov_tag_pills(person['tags'] or '')
                pt = person['project_type'] or ''
                pt_colors = {'communicatie': '#5C7A5A', 'werkveld': '#388e3c', 'evenementen': '#7b1fa2', 'onderwijs': '#1565c0'}
                pt_html = f'<span style="font-size:0.68rem;background:{pt_colors.get(pt,"#888")};color:#fff;border-radius:3px;padding:0.05rem 0.3rem;margin-right:0.2rem;">{pt.capitalize()}</span>' if pt else ''
                # Build inline checklist for this person
                rel_cards = relevant_cards_for_person(person)
                completed_ids_p = completed_by_person.get(person['id'], set())
                inline_html = ''
                for ph2 in valid_phases:
                    phase_rel = [c for c in rel_cards if c['phase'] == ph2]
                    if not phase_rel:
                        continue
                    ph2_color = self._gov_phase_color(ph2)
                    ph2_label = self._gov_phase_label(ph2)
                    inline_html += f'<div style="font-size:0.78rem;font-weight:bold;color:{ph2_color};margin:0.4rem 0 0.2rem 0;">{ph2_label}</div>'
                    for card in phase_rel:
                        items = items_by_card.get(card['id'], [])
                        if not items:
                            continue
                        inline_html += f'<div style="font-size:0.75rem;color:#555;margin-bottom:0.1rem;font-style:italic;">{html.escape(card["title"])}</div>'
                        for item in items:
                            checked = item['id'] in completed_ids_p
                            chk_color = ph2_color
                            box = f'<span style="display:inline-block;width:14px;height:14px;border:2px solid {chk_color};border-radius:2px;background:{"" + chk_color if checked else "#fff"};text-align:center;line-height:10px;font-size:10px;color:#fff;flex-shrink:0;">{"&#10003;" if checked else ""}</span>'
                            strike = 'text-decoration:line-through;color:#aaa;' if checked else ''
                            inline_html += f'<div style="display:flex;align-items:flex-start;gap:0.3rem;margin-bottom:0.2rem;"><a href="/gov/progress/toggle?person_id={person["id"]}&item_id={item["id"]}&redirect=/gov/board" style="text-decoration:none;flex-shrink:0;">{box}</a><span style="font-size:0.78rem;{strike}">{html.escape(item["title"])}</span></div>'
                body += f'''<div class="gov-card" draggable="true" data-person-id="{person['id']}" data-phase="{ph}"
                    style="background:#fff;border-radius:6px;padding:0.5rem 0.6rem;margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.1);cursor:grab;">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                        <a href="/gov/person?id={person['id']}" style="font-weight:bold;color:#1565c0;text-decoration:none;font-size:0.9rem;">{html.escape(person['name'])}</a>
                        <button onclick="govToggle(event,{person['id']})" style="background:none;border:none;cursor:pointer;font-size:0.75rem;color:#1565c0;padding:0;line-height:1;" title="Afvinken">&#9654;</button>
                    </div>
                    <div style="margin-top:0.2rem;">{pt_html}{tag_html}</div>
                    <div style="margin-top:0.35rem;">
                        <div style="height:5px;background:#e0e0e0;border-radius:3px;overflow:hidden;">
                            <div style="height:100%;width:{pct}%;background:{color};border-radius:3px;"></div>
                        </div>
                        <div style="font-size:0.7rem;color:#888;margin-top:0.1rem;">{done}/{rel_total} items ({pct}%)</div>
                    </div>
                    <div style="margin-top:0.3rem;display:flex;gap:0.3rem;">
                        <a href="/gov/persons/edit?id={person['id']}" style="font-size:0.75rem;color:#555;text-decoration:none;">&#9998;</a>
                        <a href="/gov/persons/delete?id={person['id']}" style="font-size:0.75rem;color:#dc3545;text-decoration:none;" onclick="return confirm('Persoon verwijderen?');">&#128465;</a>
                    </div>
                    <div id="gov-inline-{person['id']}" style="display:none;border-top:1px solid #e0e0e0;margin-top:0.4rem;padding-top:0.4rem;max-height:300px;overflow-y:auto;">
                        {inline_html}
                    </div>
                </div>'''
            body += '</div>'
        body += '</div></div>'

        # Drag & drop + inline toggle JS
        body += '''<script>
        document.querySelectorAll('.gov-card').forEach(function(card) {
            card.addEventListener('dragstart', function(e) {
                e.dataTransfer.setData('personId', card.dataset.personId);
                e.dataTransfer.setData('fromPhase', card.dataset.phase);
            });
        });
        function govDrop(e, newPhase) {
            e.preventDefault();
            var personId = e.dataTransfer.getData('personId');
            if (!personId) return;
            fetch('/gov/persons/move?id=' + personId + '&phase=' + newPhase)
                .then(function() { location.reload(); });
        }
        function govToggle(e, personId) {
            e.stopPropagation();
            e.preventDefault();
            var div = document.getElementById('gov-inline-' + personId);
            var btn = e.currentTarget;
            if (div.style.display === 'none') {
                div.style.display = 'block';
                btn.innerHTML = '&#9660;';
            } else {
                div.style.display = 'none';
                btn.innerHTML = '&#9654;';
            }
        }
        </script>'''
        body += html_footer()
        self._send_html(body)

    def render_gov_person(self, person_id: int, user_id: int, username: str) -> None:
        """Render the detail page for a governance person with progress checkboxes."""
        valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM governance_persons WHERE id=?', (person_id,))
            person = cur.fetchone()
            if not person:
                self.respond_redirect('/gov/board')
                return
            # All card templates ordered by phase then order_index
            cur.execute('SELECT * FROM governance_card_templates ORDER BY order_index ASC, id ASC')
            all_cards = cur.fetchall()
            # All items
            cur.execute('SELECT * FROM governance_card_items ORDER BY order_index ASC, id ASC')
            all_items = cur.fetchall()
            # Completed items + notes for this person
            cur.execute('SELECT item_id, note FROM governance_progress WHERE person_id=?', (person_id,))
            completed_info = {row['item_id']: (row['note'] or '') for row in cur.fetchall()}
            completed_ids = set(completed_info.keys())

        project_type = (person['project_type'] or '').lower()

        # Group items by card
        items_by_card = {}
        for item in all_items:
            items_by_card.setdefault(item['card_id'], []).append(item)

        # Group cards by phase — filter startpunt by project_type, rest are shared
        cards_by_phase = {}
        for card in all_cards:
            cards_by_phase.setdefault(card['phase'], []).append(card)

        def relevant_cards_for_phase(ph, phase_cards):
            if project_type:
                return [c for c in phase_cards if not c['project_type'] or c['project_type'] == project_type]
            return phase_cards

        # Compute totals using only relevant items for this person
        relevant_item_ids = set()
        for ph in valid_phases:
            for card in relevant_cards_for_phase(ph, cards_by_phase.get(ph, [])):
                for item in items_by_card.get(card['id'], []):
                    relevant_item_ids.add(item['id'])
        total_items = len(relevant_item_ids)
        total_done = len(completed_ids & relevant_item_ids)
        overall_pct = round(total_done / total_items * 100) if total_items else 0

        body = html_header(f'Gov: {person["name"]}', True, username, user_id)
        body += self._gov_nav('profile', user_id)

        phase_color = self._gov_phase_color(person['phase'])
        phase_label = self._gov_phase_label(person['phase'])
        tag_html = self._gov_tag_pills(person['tags'] or '')

        body += f'<h2 class="mt-4">&#128100; {html.escape(person["name"])}</h2>'
        body += f'''<div class="card" style="margin-bottom:0.75rem;">
            <div style="display:flex;align-items:center;gap:1rem;flex-wrap:wrap;">
                <span style="background:{phase_color};color:#fff;border-radius:12px;padding:0.2rem 0.7rem;font-size:0.9rem;">{phase_label}</span>
                <div>{tag_html}</div>
                <a href="/gov/persons/edit?id={person_id}" class="btn btn-sm" style="background:#1565c0;color:#fff;">&#9998; Bewerken</a>
            </div>'''
        if person['notes']:
            body += f'<div style="margin-top:0.5rem;color:#555;font-size:0.9rem;">{html.escape(person["notes"])}</div>'
        # Overall progress
        body += f'''<div style="margin-top:0.75rem;">
            <div style="font-size:0.85rem;color:#555;margin-bottom:0.25rem;">Totale voortgang: {total_done}/{total_items} items ({overall_pct}%)</div>
            <div style="height:8px;background:#e0e0e0;border-radius:4px;overflow:hidden;">
                <div style="height:100%;width:{overall_pct}%;background:#1565c0;border-radius:4px;"></div>
            </div>
        </div></div>'''

        # Cards grouped by phase
        for ph in valid_phases:
            relevant_cards = relevant_cards_for_phase(ph, cards_by_phase.get(ph, []))
            if not relevant_cards:
                continue
            ph_color = self._gov_phase_color(ph)
            ph_label = self._gov_phase_label(ph)
            body += f'<h3 style="color:{ph_color};margin-top:1rem;margin-bottom:0.5rem;">&#9654; {ph_label}</h3>'
            for card in relevant_cards:
                items = items_by_card.get(card['id'], [])
                card_done = sum(1 for i in items if i['id'] in completed_ids)
                card_total = len(items)
                card_pct = round(card_done / card_total * 100) if card_total else 0
                body += f'''<div class="card" style="margin-bottom:0.5rem;">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.35rem;">
                        <strong>{html.escape(card["title"])}</strong>
                        <span style="font-size:0.8rem;color:#888;">{card_done}/{card_total}</span>
                    </div>'''
                if card['description']:
                    body += f'<div style="font-size:0.8rem;color:#666;margin-bottom:0.4rem;">{html.escape(card["description"])}</div>'
                body += f'''<div style="height:5px;background:#e0e0e0;border-radius:3px;overflow:hidden;margin-bottom:0.5rem;">
                        <div style="height:100%;width:{card_pct}%;background:{ph_color};border-radius:3px;"></div>
                    </div>'''
                for item in items:
                    checked = item['id'] in completed_ids
                    item_note = completed_info.get(item['id'], '')
                    norm_html = ''
                    if item['norm'] or item['middelen']:
                        norm_content = ''
                        if item['norm']:
                            norm_content += f'<div style="margin-bottom:0.3rem;"><strong>Norm:</strong> {html.escape(item["norm"])}</div>'
                        if item['middelen']:
                            norm_content += f'<div><strong>Middelen:</strong> {html.escape(item["middelen"])}</div>'
                        norm_html = f'<details style="display:inline-block;margin-left:0.3rem;"><summary style="cursor:pointer;font-size:0.72rem;color:#1565c0;list-style:none;">&#8505; info</summary><div style="background:#e3f2fd;border-radius:4px;padding:0.4rem 0.6rem;margin-top:0.3rem;font-size:0.78rem;color:#333;max-width:480px;">{norm_content}</div></details>'
                    if checked:
                        chk_html = f'<a href="/gov/progress/toggle?person_id={person_id}&item_id={item["id"]}" style="text-decoration:none;flex-shrink:0;"><span style="display:inline-block;width:18px;height:18px;border:2px solid {ph_color};border-radius:3px;background:{ph_color};text-align:center;line-height:14px;font-size:13px;color:#fff;">&#10003;</span></a>'
                    else:
                        chk_html = f'<span onclick="govNoteToggle({item["id"]})" style="flex-shrink:0;cursor:pointer;display:inline-block;width:18px;height:18px;border:2px solid {ph_color};border-radius:3px;background:#fff;"></span>'
                    title_style = 'text-decoration:line-through;color:#aaa;' if checked else ''
                    note_display = f'<div style="font-size:0.75rem;color:#1565c0;background:#e3f2fd;border-radius:3px;padding:0.15rem 0.4rem;margin-top:0.15rem;display:inline-block;">&#128196; {html.escape(item_note)}</div>' if item_note else ''
                    edit_form = f'''<div id="gedit-{item["id"]}" style="display:none;background:#f8f9fa;border-radius:4px;padding:0.5rem;margin-top:0.3rem;">
                        <form method="POST" action="/gov/items/quick-edit">
                        <input type="hidden" name="item_id" value="{item["id"]}"><input type="hidden" name="person_id" value="{person_id}">
                        <div style="margin-bottom:0.25rem;"><input type="text" name="title" value="{html.escape(item["title"])}" class="form-control" style="font-size:0.85rem;" required placeholder="Titel"></div>
                        <div style="margin-bottom:0.25rem;"><input type="text" name="description" value="{html.escape(item["description"] or "")}" class="form-control" style="font-size:0.85rem;" placeholder="Beschrijving"></div>
                        <div style="margin-bottom:0.25rem;"><textarea name="norm" class="form-control" style="font-size:0.85rem;" rows="2" placeholder="Norm">{html.escape(item["norm"] or "")}</textarea></div>
                        <div style="margin-bottom:0.25rem;"><textarea name="middelen" class="form-control" style="font-size:0.85rem;" rows="2" placeholder="Middelen">{html.escape(item["middelen"] or "")}</textarea></div>
                        <button type="submit" class="btn btn-sm btn-primary" style="font-size:0.8rem;">Opslaan</button>
                        <button type="button" onclick="govEditToggle({item["id"]})" style="font-size:0.8rem;background:none;border:none;cursor:pointer;color:#555;margin-left:0.4rem;">Annuleren</button>
                        </form></div>'''
                    note_form = ''
                    if not checked:
                        note_form = f'''<div id="gnote-{item["id"]}" style="display:none;background:#fff8e1;border-radius:4px;padding:0.5rem;margin-top:0.25rem;">
                            <form method="POST" action="/gov/progress/complete">
                            <input type="hidden" name="person_id" value="{person_id}">
                            <input type="hidden" name="item_id" value="{item["id"]}">
                            <input type="hidden" name="redirect" value="/gov/person?id={person_id}">
                            <textarea name="note" class="form-control" rows="2" placeholder="Notitie bij afronding (optioneel)..." style="font-size:0.85rem;margin-bottom:0.25rem;"></textarea>
                            <button type="submit" class="btn btn-sm" style="font-size:0.8rem;background:#388e3c;color:#fff;">&#10003; Afronden</button>
                            <button type="button" onclick="govNoteToggle({item["id"]})" style="font-size:0.8rem;background:none;border:none;cursor:pointer;color:#555;margin-left:0.4rem;">Annuleren</button>
                            </form></div>'''
                    body += f'''<div style="margin-bottom:0.45rem;">
                        <div style="display:flex;align-items:flex-start;gap:0.4rem;">
                            {chk_html}
                            <div style="flex:1;">
                                <span style="font-size:0.9rem;{title_style}">{html.escape(item["title"])}</span>{norm_html}
                                <button onclick="govEditToggle({item["id"]})" style="background:none;border:none;cursor:pointer;font-size:0.72rem;color:#bbb;padding:0;margin-left:0.3rem;" title="Bewerken">&#9998;</button>
                                {f'<div style="font-size:0.75rem;color:#888;">{html.escape(item["description"])}</div>' if item["description"] else ""}
                                {note_display}
                                {edit_form}
                                {note_form}
                            </div>
                        </div>
                    </div>'''
                body += '</div>'

        body += '''<script>
        function govEditToggle(id) {
            var d = document.getElementById('gedit-' + id);
            d.style.display = d.style.display === 'none' ? 'block' : 'none';
        }
        function govNoteToggle(id) {
            var d = document.getElementById('gnote-' + id);
            d.style.display = d.style.display === 'none' ? 'block' : 'none';
        }
        </script>'''
        body += html_footer()
        self._send_html(body)

    def render_gov_profiles(self, user_id: int, username: str) -> None:
        """Render the governance persons profiles page with notes and consent."""
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM governance_persons ORDER BY name ASC')
            all_persons = cur.fetchall()
            cur.execute('SELECT gn.*, u.username AS author FROM governance_notes gn LEFT JOIN users u ON gn.created_by = u.id ORDER BY gn.created_at DESC')
            all_notes = cur.fetchall()

        notes_by_person = {}
        for note in all_notes:
            notes_by_person.setdefault(note['person_id'], []).append(note)

        type_labels = {'coaching': '&#128172; Coaching', 'intervisie': '&#128101; Intervisie', 'aandachtspunt': '&#127919; Aandachtspunt'}
        type_colors = {'coaching': '#1565c0', 'intervisie': '#7b1fa2', 'aandachtspunt': '#ef6c00'}
        pt_colors = {'communicatie': '#5C7A5A', 'werkveld': '#388e3c', 'evenementen': '#7b1fa2', 'onderwijs': '#1565c0'}

        body = html_header('Governance Personen', True, username, user_id)
        body += '<h2 class="mt-4">&#128101; Personen &amp; Profiel</h2>'
        body += self._gov_nav('profiles', user_id)

        if not all_persons:
            body += '<div class="card"><p>Nog geen personen toegevoegd.</p></div>'
        else:
            for person in all_persons:
                pid = person['id']
                phase_color = self._gov_phase_color(person['phase'])
                phase_label = self._gov_phase_label(person['phase'])
                pt = person['project_type'] or ''
                pt_badge = f'<span style="font-size:0.75rem;background:{pt_colors.get(pt,"#888")};color:#fff;border-radius:3px;padding:0.1rem 0.4rem;margin-left:0.4rem;">{pt.capitalize()}</span>' if pt else ''
                consent = person['consent_given']
                consent_icon = '&#9989;' if consent else '&#9744;'
                consent_color = '#388e3c' if consent else '#888'
                consent_label = 'Akkoord gegeven' if consent else 'Nog geen akkoord'

                person_notes = notes_by_person.get(pid, [])
                aandachtspunten = [n for n in person_notes if n['note_type'] == 'aandachtspunt']
                other_notes = [n for n in person_notes if n['note_type'] != 'aandachtspunt']

                body += f'''<div class="card" style="margin-bottom:1rem;border-left:4px solid {phase_color};">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:0.5rem;">
                        <div>
                            <a href="/gov/person?id={pid}" style="font-size:1.1rem;font-weight:bold;color:#1565c0;text-decoration:none;">{html.escape(person['name'])}</a>
                            {pt_badge}
                            <span style="font-size:0.8rem;background:{phase_color};color:#fff;border-radius:10px;padding:0.1rem 0.5rem;margin-left:0.4rem;">{phase_label}</span>
                        </div>
                        <a href="/gov/profiles/consent?id={pid}" style="font-size:0.8rem;color:{consent_color};text-decoration:none;border:1px solid {consent_color};border-radius:4px;padding:0.2rem 0.5rem;" title="Klik om akkoord te wisselen">{consent_icon} {consent_label}</a>
                    </div>'''

                if person['notes']:
                    body += f'<div style="font-size:0.85rem;color:#555;margin-top:0.4rem;font-style:italic;">{html.escape(person["notes"])}</div>'

                # Aandachtspunten (prominent)
                if aandachtspunten:
                    body += '<div style="margin-top:0.6rem;">'
                    body += '<div style="font-size:0.85rem;font-weight:bold;color:#ef6c00;margin-bottom:0.3rem;">&#127919; Persoonlijke aandachtspunten</div>'
                    for note in aandachtspunten:
                        body += f'''<div style="background:#fff8e1;border-left:3px solid #ef6c00;padding:0.4rem 0.6rem;margin-bottom:0.3rem;border-radius:0 4px 4px 0;font-size:0.88rem;display:flex;justify-content:space-between;align-items:flex-start;">
                            <div>
                                <span>{html.escape(note["content"])}</span>
                                <div style="font-size:0.72rem;color:#aaa;margin-top:0.15rem;">{(note["created_at"] or "")[:10]} — {html.escape(note["author"] or "?")}</div>
                            </div>
                            <a href="/gov/notes/delete?id={note['id']}" style="color:#dc3545;font-size:0.75rem;text-decoration:none;flex-shrink:0;margin-left:0.5rem;" onclick="return confirm('Verwijderen?');">&#128465;</a>
                        </div>'''
                    body += '</div>'

                # Coaching + intervisie notities (collapsible)
                if other_notes:
                    body += f'<details style="margin-top:0.5rem;"><summary style="cursor:pointer;font-size:0.85rem;color:#555;">&#128196; {len(other_notes)} notitie(s) — klik om te tonen</summary>'
                    body += '<div style="margin-top:0.4rem;">'
                    for note in other_notes:
                        ncolor = type_colors.get(note['note_type'], '#888')
                        nlabel = type_labels.get(note['note_type'], note['note_type'])
                        body += f'''<div style="border-left:3px solid {ncolor};padding:0.4rem 0.6rem;margin-bottom:0.35rem;border-radius:0 4px 4px 0;font-size:0.88rem;display:flex;justify-content:space-between;align-items:flex-start;">
                            <div>
                                <span style="font-size:0.72rem;background:{ncolor};color:#fff;border-radius:3px;padding:0.05rem 0.3rem;margin-right:0.3rem;">{nlabel}</span>
                                <span>{html.escape(note["content"])}</span>
                                <div style="font-size:0.72rem;color:#aaa;margin-top:0.15rem;">{(note["created_at"] or "")[:10]} — {html.escape(note["author"] or "?")}</div>
                            </div>
                            <a href="/gov/notes/delete?id={note['id']}" style="color:#dc3545;font-size:0.75rem;text-decoration:none;flex-shrink:0;margin-left:0.5rem;" onclick="return confirm('Verwijderen?');">&#128465;</a>
                        </div>'''
                    body += '</div></details>'

                # Add note form
                body += f'''<details style="margin-top:0.6rem;"><summary style="cursor:pointer;font-size:0.85rem;color:#1565c0;">&#43; Notitie / aandachtspunt toevoegen</summary>
                    <div style="margin-top:0.4rem;background:#f8f9fa;border-radius:4px;padding:0.6rem;">
                        <form method="POST" action="/gov/notes/add">
                            <input type="hidden" name="person_id" value="{pid}">
                            <div style="display:flex;gap:0.4rem;flex-wrap:wrap;align-items:flex-end;">
                                <div>
                                    <label style="font-size:0.8rem;display:block;margin-bottom:0.15rem;">Type</label>
                                    <select name="note_type" class="form-control" style="font-size:0.85rem;">
                                        <option value="coaching">&#128172; Coaching</option>
                                        <option value="intervisie">&#128101; Intervisie</option>
                                        <option value="aandachtspunt">&#127919; Aandachtspunt</option>
                                    </select>
                                </div>
                                <div style="flex:1;min-width:220px;">
                                    <label style="font-size:0.8rem;display:block;margin-bottom:0.15rem;">Notitie</label>
                                    <textarea name="content" class="form-control" rows="2" required style="font-size:0.85rem;" placeholder="Notitie..."></textarea>
                                </div>
                                <div><button type="submit" class="btn btn-primary" style="font-size:0.85rem;">Opslaan</button></div>
                            </div>
                        </form>
                    </div></details>'''

                body += '</div>'

        body += html_footer()
        self._send_html(body)

    def render_gov_overview(self, user_id: int, username: str) -> None:
        """Render the governance overview page."""
        valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM governance_persons ORDER BY name ASC')
            all_persons = cur.fetchall()
            cur.execute('SELECT COUNT(*) FROM governance_card_items')
            total_items = cur.fetchone()[0]
            cur.execute('SELECT person_id, COUNT(*) AS cnt FROM governance_progress GROUP BY person_id')
            progress_map = {row['person_id']: row['cnt'] for row in cur.fetchall()}

        phase_counts = {p: 0 for p in valid_phases}
        for person in all_persons:
            ph = person['phase'] if person['phase'] in valid_phases else 'startpunt'
            phase_counts[ph] += 1

        total_persons = len(all_persons)
        avg_pct = 0
        if all_persons and total_items:
            total_pct_sum = sum(progress_map.get(p['id'], 0) for p in all_persons)
            avg_pct = round(total_pct_sum / (total_persons * total_items) * 100) if total_persons else 0

        body = html_header('Governance Overzicht', True, username, user_id)
        body += '<h2 class="mt-4">&#128200; Governance Overzicht</h2>'
        body += self._gov_nav('overview', user_id)

        # Phase distribution
        body += '<div class="card" style="margin-bottom:0.75rem;"><div class="section-title">Faseverdeling</div>'
        body += '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.5rem;">'
        for ph in valid_phases:
            cnt = phase_counts[ph]
            color = self._gov_phase_color(ph)
            label = self._gov_phase_label(ph)
            pct = round(cnt / total_persons * 100) if total_persons else 0
            body += f'''<div style="flex:1;min-width:90px;text-align:center;">
                <div style="font-size:1.2rem;font-weight:bold;color:{color};">{cnt}</div>
                <div style="font-size:0.75rem;color:#555;">{label}</div>
                <div style="height:6px;background:#e0e0e0;border-radius:3px;margin-top:0.2rem;overflow:hidden;">
                    <div style="height:100%;width:{pct}%;background:{color};"></div>
                </div>
            </div>'''
        body += '</div>'
        body += f'<div style="font-size:0.85rem;color:#555;">Gemiddelde afronding: <strong>{avg_pct}%</strong></div>'
        body += '</div>'

        # Persons table
        body += '<div class="card"><div class="section-title">Alle personen</div><table><thead><tr><th>Naam</th><th>Fase</th><th>Voortgang</th><th>% Klaar</th><th>Tags</th></tr></thead><tbody>'
        for person in all_persons:
            ph = person['phase'] if person['phase'] in valid_phases else 'startpunt'
            color = self._gov_phase_color(ph)
            label = self._gov_phase_label(ph)
            done = progress_map.get(person['id'], 0)
            pct = round(done / total_items * 100) if total_items else 0
            tag_html = self._gov_tag_pills(person['tags'] or '')
            body += f'''<tr>
                <td><a href="/gov/person?id={person['id']}" style="color:#1565c0;">{html.escape(person['name'])}</a></td>
                <td><span style="background:{color};color:#fff;border-radius:10px;padding:0.1rem 0.5rem;font-size:0.8rem;">{label}</span></td>
                <td style="min-width:120px;">
                    <div style="height:8px;background:#e0e0e0;border-radius:4px;overflow:hidden;">
                        <div style="height:100%;width:{pct}%;background:{color};border-radius:4px;"></div>
                    </div>
                </td>
                <td><strong>{pct}%</strong></td>
                <td>{tag_html}</td>
            </tr>'''
        body += '</tbody></table></div>'
        body += html_footer()
        self._send_html(body)

    def render_gov_cards(self, user_id: int, username: str) -> None:
        """Render the governance card template management page (admin only)."""
        valid_phases = ['startpunt','empathize','define','ideate','prototype','test','uittreden']
        with sqlite3.connect(DB_PATH, timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM governance_card_templates ORDER BY order_index ASC, id ASC')
            all_cards = cur.fetchall()
            cur.execute('SELECT * FROM governance_card_items ORDER BY order_index ASC, id ASC')
            all_items = cur.fetchall()

        items_by_card = {}
        for item in all_items:
            items_by_card.setdefault(item['card_id'], []).append(item)

        cards_by_phase = {}
        for card in all_cards:
            cards_by_phase.setdefault(card['phase'], []).append(card)

        body = html_header('Governance Kaartbeheer', True, username, user_id)
        body += '<h2 class="mt-4">&#9881; Kaartbeheer</h2>'
        body += self._gov_nav('cards', user_id)

        # Add card form
        phase_opts_add = ''.join(f'<option value="{p}">{self._gov_phase_label(p)}</option>' for p in valid_phases)
        pt_opts_add = '<option value="">Alle typen</option>' + ''.join(f'<option value="{t}">{t.capitalize()}</option>' for t in ['communicatie','werkveld','evenementen','onderwijs'])
        body += f'''<div class="card" style="margin-bottom:0.75rem;">
            <div class="section-title">Kaart toevoegen</div>
            <form method="POST" action="/gov/cards/add" style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
                <div><label style="font-size:0.85rem;">Titel</label><br><input type="text" name="title" class="form-control" required style="min-width:180px;"></div>
                <div><label style="font-size:0.85rem;">Fase</label><br><select name="phase" class="form-control">{phase_opts_add}</select></div>
                <div><label style="font-size:0.85rem;">Projecttype</label><br><select name="project_type" class="form-control">{pt_opts_add}</select></div>
                <div><label style="font-size:0.85rem;">Beschrijving</label><br><input type="text" name="description" class="form-control" style="min-width:200px;"></div>
                <div><label style="font-size:0.85rem;">Volgorde</label><br><input type="number" name="order_index" value="0" class="form-control" style="width:70px;"></div>
                <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
            </form></div>'''

        # Cards grouped by phase
        for ph in valid_phases:
            cards = cards_by_phase.get(ph, [])
            if not cards:
                continue
            ph_color = self._gov_phase_color(ph)
            ph_label = self._gov_phase_label(ph)
            body += f'<h3 style="color:{ph_color};margin-top:1rem;">{ph_label}</h3>'
            for card in cards:
                items = items_by_card.get(card['id'], [])
                pt_colors = {'communicatie': '#5C7A5A', 'werkveld': '#388e3c', 'evenementen': '#7b1fa2', 'onderwijs': '#1565c0'}
                card_pt = card['project_type'] or ''
                pt_badge = f'<span style="font-size:0.72rem;background:{pt_colors.get(card_pt,"#888")};color:#fff;border-radius:3px;padding:0.05rem 0.35rem;margin-left:0.4rem;">{card_pt.capitalize()}</span>' if card_pt else '<span style="font-size:0.72rem;background:#888;color:#fff;border-radius:3px;padding:0.05rem 0.35rem;margin-left:0.4rem;">Alle typen</span>'
                body += f'''<div class="card" style="border-left:4px solid {ph_color};margin-bottom:0.5rem;">
                    <div style="display:flex;justify-content:space-between;align-items:center;">
                        <div><strong>{html.escape(card["title"])}</strong>{pt_badge}</div>
                        <div>
                            <a href="/gov/cards/edit?id={card['id']}" class="btn btn-sm btn-secondary">&#9998; Bewerken</a>
                            <a href="/gov/cards/delete?id={card['id']}" class="btn btn-sm btn-danger" style="margin-left:0.3rem;" onclick="return confirm('Kaart verwijderen?');">&#128465;</a>
                        </div>
                    </div>'''
                if card['description']:
                    body += f'<div style="font-size:0.85rem;color:#666;margin-top:0.2rem;">{html.escape(card["description"])}</div>'
                body += f'<div style="font-size:0.75rem;color:#888;margin-top:0.1rem;">Volgorde: {card["order_index"]}</div>'

                # Items list
                if items:
                    body += '<ul style="margin:0.5rem 0 0.3rem 1.2rem;padding:0;">'
                    for item in items:
                        body += f'''<li style="margin-bottom:0.2rem;font-size:0.9rem;">
                            {html.escape(item["title"])}
                            {f'<span style="font-size:0.8rem;color:#888;"> — {html.escape(item["description"])}</span>' if item["description"] else ""}
                            <a href="/gov/items/delete?id={item['id']}" style="color:#dc3545;margin-left:0.4rem;font-size:0.75rem;" onclick="return confirm('Item verwijderen?');">&#128465;</a>
                        </li>'''
                    body += '</ul>'

                # Add item form
                body += f'''<form method="POST" action="/gov/items/add" style="display:flex;gap:0.4rem;flex-wrap:wrap;align-items:flex-end;margin-top:0.5rem;">
                    <input type="hidden" name="card_id" value="{card['id']}">
                    <div><input type="text" name="title" class="form-control" placeholder="Item titel" required style="min-width:160px;"></div>
                    <div><input type="text" name="description" class="form-control" placeholder="Beschrijving (optioneel)" style="min-width:160px;"></div>
                    <div><input type="number" name="order_index" value="0" class="form-control" style="width:65px;" placeholder="Volgorde"></div>
                    <div><button type="submit" class="btn btn-sm" style="background:#1565c0;color:#fff;">&#43; Item</button></div>
                </form>'''
                body += '</div>'

        body += html_footer()
        self._send_html(body)


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





