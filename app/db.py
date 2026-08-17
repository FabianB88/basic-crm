"""Database access: connections, schema, migrations, indexes.

Two things here matter more than they look:

1. ``connect()`` turns on foreign keys for *every* connection. The PRAGMA is
   per-connection and defaults to OFF, so before this the ON DELETE CASCADE
   clauses in the schema never fired and deleting a customer left orphaned
   tasks, interactions and links behind.

2. ``init_db()`` runs in a fixed order: tables, then added columns, then
   indexes, then data migrations, then seed data. The previous version ran two
   data migrations *before* the tables they touched existed, so a fresh
   database died with "no such table: tasks" and the app could not start
   anywhere the committed crm.db was absent.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

from . import config
from .seed_governance import GOVERNANCE_PHASES

_BUSY_TIMEOUT_MS = 10_000

# The connection currently open on this thread, if any. Threads never share it.
_local = threading.local()


# ── Connections ───────────────────────────────────────────────────────────
@contextmanager
def connect(readonly: bool = False) -> Iterator[sqlite3.Connection]:
    """Yield a configured connection. Commits on success, rolls back on error.

    Re-entrant: a nested connect() joins the transaction already open on this
    thread instead of opening a second one. Without that, any helper that opens
    its own connection while a write transaction is in flight — log_action was
    the common case — hits "database is locked", because SQLite will not let a
    second writer queue behind the first on the same thread.

    Only the outermost block commits and closes.
    """
    existing = getattr(_local, 'conn', None)
    if existing is not None:
        yield existing
        return

    conn = sqlite3.connect(config.DB_PATH, timeout=_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    _local.conn = conn
    try:
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute(f'PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}')
        yield conn
        if not readonly:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _local.conn = None
        conn.close()


def query_all(sql: str, args: tuple = ()) -> List[sqlite3.Row]:
    with connect(readonly=True) as conn:
        return conn.execute(sql, args).fetchall()


def query_one(sql: str, args: tuple = ()) -> Optional[sqlite3.Row]:
    with connect(readonly=True) as conn:
        return conn.execute(sql, args).fetchone()


def query_value(sql: str, args: tuple = (), default: Any = None) -> Any:
    row = query_one(sql, args)
    return row[0] if row is not None else default


def execute(sql: str, args: tuple = ()) -> int:
    """Run a statement and return lastrowid."""
    with connect() as conn:
        cur = conn.execute(sql, args)
        return cur.lastrowid


# ── Schema helpers ────────────────────────────────────────────────────────
def _columns(cur: sqlite3.Cursor, table: str) -> set:
    try:
        return {r[1] for r in cur.execute(f'PRAGMA table_info({table})')}
    except sqlite3.OperationalError:
        return set()


def _ensure_column(cur: sqlite3.Cursor, table: str, column: str, ddl: str) -> None:
    """Add a column when an older database predates it. Idempotent."""
    if column not in _columns(cur, table):
        cur.execute(f'ALTER TABLE {table} ADD COLUMN {ddl}')


def _migration_done(cur: sqlite3.Cursor, name: str) -> bool:
    return cur.execute('SELECT 1 FROM _migrations WHERE name = ?', (name,)).fetchone() is not None


def _mark_migration(cur: sqlite3.Cursor, name: str) -> None:
    cur.execute('INSERT OR IGNORE INTO _migrations (name) VALUES (?)', (name,))


# ── Table definitions ─────────────────────────────────────────────────────
# Full current shape, so a fresh database is correct in one step and does not
# depend on the ALTER statements further down.
_TABLES = [
    ('_migrations', '''
        CREATE TABLE IF NOT EXISTS _migrations (
            name TEXT PRIMARY KEY
        )'''),
    ('users', '''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            is_comm INTEGER DEFAULT 0,
            is_governance INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )'''),
    # Sessions live in the database so a restart no longer logs everyone out,
    # and so expired sessions can actually be reaped.
    ('sessions', '''
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            csrf_token TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('customers', '''
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
            category TEXT DEFAULT 'klant',
            relation_type TEXT DEFAULT 'extern',
            role TEXT,
            verbinding TEXT,
            created_by INTEGER,
            custom_fields TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
        )'''),
    ('notes', '''
        CREATE TABLE IF NOT EXISTS notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            customer_id INTEGER NOT NULL,
            user_id INTEGER,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
        )'''),
    ('tasks', '''
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
        )'''),
    ('interactions', '''
        CREATE TABLE IF NOT EXISTS interactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            interaction_type TEXT NOT NULL,
            note TEXT,
            contact_date DATE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            customer_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('customer_users', '''
        CREATE TABLE IF NOT EXISTS customer_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            linked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            reminder_paused_until DATE,
            UNIQUE(customer_id, user_id),
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('customer_fields', '''
        CREATE TABLE IF NOT EXISTS customer_fields (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            label TEXT NOT NULL
        )'''),
    ('documents', '''
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
        )'''),
    ('audit_logs', '''
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT NOT NULL,
            table_name TEXT NOT NULL,
            row_id INTEGER,
            details TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
        )'''),
    # The internal messaging feature shipped without this table: commit 507cf65
    # added it, ccf7308 reverted it, c897f89 re-added the UI but not the schema.
    # Every /messages route and the whole /users/profile page raised
    # "no such table: messages", and the 5-second poller raised it on every page.
    ('messages', '''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER NOT NULL,
            recipient_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            reply_to INTEGER,
            is_read INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (sender_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (recipient_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (reply_to) REFERENCES messages(id) ON DELETE SET NULL
        )'''),
    ('comm_goals', '''
        CREATE TABLE IF NOT EXISTS comm_goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            target_date DATE,
            status TEXT NOT NULL DEFAULT 'actief',
            created_by INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('comm_tasks', '''
        CREATE TABLE IF NOT EXISTS comm_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            status TEXT NOT NULL DEFAULT 'backlog',
            due_date DATE,
            assigned_to INTEGER,
            created_by INTEGER NOT NULL,
            goal_id INTEGER,
            priority TEXT DEFAULT 'medium',
            tags TEXT,
            reminder_note TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (assigned_to) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (goal_id) REFERENCES comm_goals(id) ON DELETE SET NULL
        )'''),
    ('comm_task_comments', '''
        CREATE TABLE IF NOT EXISTS comm_task_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (task_id) REFERENCES comm_tasks(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('comm_dates', '''
        CREATE TABLE IF NOT EXISTS comm_dates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            date DATE NOT NULL,
            type TEXT NOT NULL DEFAULT 'event',
            created_by INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('comm_content', '''
        CREATE TABLE IF NOT EXISTS comm_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            platform TEXT DEFAULT 'overig',
            publish_date DATE,
            status TEXT NOT NULL DEFAULT 'idee',
            board_status TEXT,
            assigned_to INTEGER,
            created_by INTEGER NOT NULL,
            tags TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (assigned_to) REFERENCES users(id) ON DELETE SET NULL,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('comm_profiles', '''
        CREATE TABLE IF NOT EXISTS comm_profiles (
            user_id INTEGER PRIMARY KEY,
            role_title TEXT,
            bio TEXT,
            skills TEXT,
            avatar_color TEXT DEFAULT '#5C7A5A',
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('events_gov_tasks', '''
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
        )'''),
    ('governance_persons', '''
        CREATE TABLE IF NOT EXISTS governance_persons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phase TEXT NOT NULL DEFAULT 'startpunt',
            project_type TEXT DEFAULT '',
            tags TEXT,
            notes TEXT,
            consent_given INTEGER DEFAULT 0,
            created_by INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE CASCADE
        )'''),
    ('governance_card_templates', '''
        CREATE TABLE IF NOT EXISTS governance_card_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phase TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            project_type TEXT DEFAULT NULL,
            order_index INTEGER DEFAULT 0
        )'''),
    ('governance_card_items', '''
        CREATE TABLE IF NOT EXISTS governance_card_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            norm TEXT,
            middelen TEXT,
            order_index INTEGER DEFAULT 0,
            FOREIGN KEY (card_id) REFERENCES governance_card_templates(id) ON DELETE CASCADE
        )'''),
    ('governance_progress', '''
        CREATE TABLE IF NOT EXISTS governance_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            note TEXT,
            completed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            completed_by INTEGER,
            UNIQUE(person_id, item_id),
            FOREIGN KEY (person_id) REFERENCES governance_persons(id) ON DELETE CASCADE,
            FOREIGN KEY (item_id) REFERENCES governance_card_items(id) ON DELETE CASCADE,
            FOREIGN KEY (completed_by) REFERENCES users(id) ON DELETE SET NULL
        )'''),
    ('governance_notes', '''
        CREATE TABLE IF NOT EXISTS governance_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id INTEGER NOT NULL,
            note_type TEXT NOT NULL DEFAULT 'coaching',
            content TEXT NOT NULL,
            created_by INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (person_id) REFERENCES governance_persons(id) ON DELETE CASCADE,
            FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL
        )'''),
]

# Columns added after a table originally shipped. Applied to old databases only.
_ADDED_COLUMNS = [
    ('users', 'is_admin', 'is_admin INTEGER DEFAULT 0'),
    ('users', 'is_comm', 'is_comm INTEGER DEFAULT 0'),
    ('users', 'is_governance', 'is_governance INTEGER DEFAULT 0'),
    ('customers', 'tags', 'tags TEXT'),
    ('customers', 'category', "category TEXT DEFAULT 'klant'"),
    ('customers', 'created_by', 'created_by INTEGER'),
    ('customers', 'custom_fields', 'custom_fields TEXT'),
    ('customers', 'relation_type', "relation_type TEXT DEFAULT 'extern'"),
    ('customers', 'role', 'role TEXT'),
    ('customers', 'verbinding', 'verbinding TEXT'),
    ('customers', 'website', 'website TEXT'),
    ('customers', 'industry', 'industry TEXT'),
    ('customers', 'company_size', 'company_size TEXT'),
    ('customers', 'region', 'region TEXT'),
    ('customer_users', 'reminder_paused_until', 'reminder_paused_until DATE'),
    ('interactions', 'contact_date', 'contact_date DATE'),
    ('comm_tasks', 'priority', "priority TEXT DEFAULT 'medium'"),
    ('comm_tasks', 'tags', 'tags TEXT'),
    ('comm_tasks', 'reminder_note', 'reminder_note TEXT'),
    ('comm_content', 'board_status', 'board_status TEXT'),
    ('governance_card_items', 'norm', 'norm TEXT'),
    ('governance_card_items', 'middelen', 'middelen TEXT'),
    ('governance_card_templates', 'project_type', 'project_type TEXT DEFAULT NULL'),
    ('governance_persons', 'project_type', "project_type TEXT DEFAULT ''"),
    ('governance_persons', 'consent_given', 'consent_given INTEGER DEFAULT 0'),
    ('governance_progress', 'note', 'note TEXT'),
]

# The database had no indexes at all, so every lookup was a full table scan.
_INDEXES = [
    'CREATE INDEX IF NOT EXISTS idx_tasks_customer ON tasks(customer_id)',
    'CREATE INDEX IF NOT EXISTS idx_tasks_user_status ON tasks(user_id, status)',
    'CREATE INDEX IF NOT EXISTS idx_tasks_status_due ON tasks(status, due_date)',
    'CREATE INDEX IF NOT EXISTS idx_notes_customer ON notes(customer_id)',
    'CREATE INDEX IF NOT EXISTS idx_notes_user ON notes(user_id)',
    'CREATE INDEX IF NOT EXISTS idx_interactions_customer ON interactions(customer_id)',
    'CREATE INDEX IF NOT EXISTS idx_interactions_user ON interactions(user_id)',
    'CREATE INDEX IF NOT EXISTS idx_customer_users_customer ON customer_users(customer_id)',
    'CREATE INDEX IF NOT EXISTS idx_customer_users_user ON customer_users(user_id)',
    'CREATE INDEX IF NOT EXISTS idx_customers_relation ON customers(relation_type)',
    'CREATE INDEX IF NOT EXISTS idx_customers_created_by ON customers(created_by)',
    'CREATE INDEX IF NOT EXISTS idx_messages_recipient ON messages(recipient_id, is_read)',
    'CREATE INDEX IF NOT EXISTS idx_messages_pair ON messages(sender_id, recipient_id)',
    'CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)',
    'CREATE INDEX IF NOT EXISTS idx_comm_tasks_status ON comm_tasks(status)',
    'CREATE INDEX IF NOT EXISTS idx_comm_tasks_assigned ON comm_tasks(assigned_to)',
    'CREATE INDEX IF NOT EXISTS idx_comm_tasks_goal ON comm_tasks(goal_id)',
    'CREATE INDEX IF NOT EXISTS idx_comm_content_status ON comm_content(status)',
    'CREATE INDEX IF NOT EXISTS idx_gov_progress_person ON governance_progress(person_id)',
    'CREATE INDEX IF NOT EXISTS idx_gov_items_card ON governance_card_items(card_id)',
    'CREATE INDEX IF NOT EXISTS idx_gov_cards_phase ON governance_card_templates(phase)',
    'CREATE INDEX IF NOT EXISTS idx_gov_notes_person ON governance_notes(person_id)',
    'CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at)',
]


def init_db() -> None:
    """Create or upgrade the schema. Safe to run on every start."""
    # WAL is a property of the file, so it only has to be set once, but asking
    # for it repeatedly is harmless. It lets readers run while a write is in
    # flight instead of every writer locking the whole database.
    with sqlite3.connect(config.DB_PATH, timeout=_BUSY_TIMEOUT_MS / 1000) as bootstrap:
        bootstrap.execute('PRAGMA journal_mode = WAL')
        bootstrap.execute('PRAGMA synchronous = NORMAL')

    with connect() as conn:
        cur = conn.cursor()

        # 1. Tables, in dependency order.
        for _name, ddl in _TABLES:
            cur.execute(ddl)

        # 2. Columns added to tables that already existed in older databases.
        for table, column, ddl in _ADDED_COLUMNS:
            _ensure_column(cur, table, column, ddl)

        # 3. Indexes.
        for stmt in _INDEXES:
            cur.execute(stmt)

        # 4. Data migrations. These run last because they touch table contents,
        #    which requires every table above to exist first.
        _run_data_migrations(cur)

        # 5. Seed the governance card templates on an empty database.
        _seed_governance(cur)


def _run_data_migrations(cur: sqlite3.Cursor) -> None:
    # Interne contacten gebruiken het rolveld, niet klant/netwerk.
    cur.execute(
        "UPDATE customers SET category = NULL "
        "WHERE relation_type = 'intern' AND category IN ('klant', 'netwerk')"
    )

    if not _migration_done(cur, 'clean_open_tasks_2026'):
        cur.execute("DELETE FROM tasks WHERE status = 'open'")
        _mark_migration(cur, 'clean_open_tasks_2026')

    if not _migration_done(cur, 'link_anouk_intern_2026'):
        row = cur.execute(
            "SELECT id FROM users WHERE lower(username) = 'anouk' LIMIT 1"
        ).fetchone()
        if row:
            cur.execute('''
                INSERT OR IGNORE INTO customer_users (customer_id, user_id)
                SELECT c.id, ?
                  FROM customers c
                 WHERE c.relation_type = 'intern'
                   AND NOT EXISTS (
                       SELECT 1 FROM customer_users cu WHERE cu.customer_id = c.id
                   )
            ''', (row[0],))
        _mark_migration(cur, 'link_anouk_intern_2026')

    # Eenmalige schone lei: alles wat op het moment van deze deploy nog open
    # stond gaat het archief in en komt niet vanzelf terug. De backlog bestond
    # grotendeels uit jaren oude herinneringen die het dashboard dichtslibden.
    #
    # De pauze staat daarna op 9999-12-31, dus de herinneringsmotor maakt uit
    # zichzelf niets nieuws aan. Zodra er echte activiteit is bij een klant
    # (notitie, interactie, taak of een nieuwe koppeling) heft
    # reminders.refresh_for_customer die pauze op en start de normale cyclus
    # van 60/180 dagen weer voor die ene klant.
    #
    # Losse taken zijn per stuk terug te halen via het archief.
    if not _migration_done(cur, 'archive_open_backlog_2026'):
        moved = cur.execute("UPDATE tasks SET status = 'archief' WHERE status = 'open'").rowcount
        cur.execute("UPDATE customer_users SET reminder_paused_until = '9999-12-31'")
        _mark_migration(cur, 'archive_open_backlog_2026')
        if moved:
            cur.execute(
                'INSERT INTO audit_logs (user_id, action, table_name, row_id, details) '
                'VALUES (NULL, ?, ?, NULL, ?)',
                ('archive', 'tasks',
                 f'eenmalige schoonmaak: {moved} openstaande taken gearchiveerd'))
            print(f'[migratie] {moved} openstaande taken gearchiveerd (eenmalige schone lei)')

    # Older rows were written before project_type existed on card templates.
    if not _migration_done(cur, 'backfill_card_project_type'):
        for pt in ('communicatie', 'werkveld', 'evenementen', 'onderwijs'):
            cur.execute(
                'UPDATE governance_card_templates SET project_type = ? '
                'WHERE project_type IS NULL AND lower(title) LIKE ?',
                (pt, f'%{pt}%'),
            )
        _mark_migration(cur, 'backfill_card_project_type')


def _seed_governance(cur: sqlite3.Cursor) -> None:
    """Insert the standard project cards for any phase that has none yet."""
    for phase_key, phase_data in GOVERNANCE_PHASES.items():
        existing = cur.execute(
            'SELECT COUNT(*) FROM governance_card_templates WHERE phase = ?', (phase_key,)
        ).fetchone()[0]
        if existing:
            continue
        shared = phase_data.get('shared_items')
        for card_title, card_order, card_items in phase_data['cards']:
            project_type = next(
                (t for t in config.GOV_PROJECT_TYPES if t in card_title.lower()), None
            )
            cur.execute(
                'INSERT INTO governance_card_templates '
                '(title, phase, order_index, project_type) VALUES (?, ?, ?, ?)',
                (card_title, phase_key, card_order, project_type),
            )
            card_id = cur.lastrowid
            for i, (item_title, norm, middelen) in enumerate(
                card_items if card_items is not None else shared
            ):
                cur.execute(
                    'INSERT INTO governance_card_items '
                    '(card_id, title, norm, middelen, order_index) VALUES (?, ?, ?, ?, ?)',
                    (card_id, item_title, norm, middelen, i),
                )


# ── Audit log ─────────────────────────────────────────────────────────────
def log_action(user_id: Optional[int], action: str, table: str,
               row_id: Optional[int] = None, details: str = '') -> None:
    """Record who changed what. Never raises — auditing must not break a request."""
    try:
        execute(
            'INSERT INTO audit_logs (user_id, action, table_name, row_id, details) '
            'VALUES (?, ?, ?, ?, ?)',
            (user_id, action, table, row_id, details),
        )
    except sqlite3.Error as exc:  # pragma: no cover - defensive
        print(f'[audit] kon actie niet loggen: {exc}')


# ── Small shared lookups ──────────────────────────────────────────────────
def users_exist() -> bool:
    return (query_value('SELECT COUNT(*) FROM users', default=0) or 0) > 0


def get_user_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    row = query_one('SELECT * FROM users WHERE id = ?', (user_id,))
    return dict(row) if row else None


def get_user_by_username_or_email(identifier: str) -> Optional[Dict[str, Any]]:
    row = query_one(
        'SELECT * FROM users WHERE username = ? OR email = ?', (identifier, identifier)
    )
    return dict(row) if row else None


def get_customer(customer_id: int) -> Optional[Dict[str, Any]]:
    row = query_one('SELECT * FROM customers WHERE id = ?', (customer_id,))
    return dict(row) if row else None


def get_custom_field_definitions() -> List[sqlite3.Row]:
    return query_all('SELECT * FROM customer_fields ORDER BY id ASC')


def get_linked_user_ids(customer_id: int) -> List[int]:
    return [r[0] for r in query_all(
        'SELECT user_id FROM customer_users WHERE customer_id = ?', (customer_id,)
    )]


def all_users() -> List[sqlite3.Row]:
    return query_all('SELECT id, username FROM users ORDER BY username ASC')


def comm_members() -> List[sqlite3.Row]:
    return query_all(
        'SELECT id, username FROM users '
        'WHERE is_comm = 1 OR is_admin = 1 OR id = 1 ORDER BY username'
    )
