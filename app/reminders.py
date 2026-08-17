"""Follow-up reminder tasks.

One open reminder is kept per customer/accountmanager link. The due date is
always last-contact + N days, where N is 180 for interne relaties and 60 for
externe. (The old docstring claimed 90 days everywhere while the code used
180/60 — the code and the UI text were right, the docstring was not.)

"Last contact" is the most recent of: a note, an interaction (using
contact_date when set so backdating works), or a non-reminder task. New
customers with no activity yet fall back to their creation date.

Every write used to trigger a full sweep over all customer_users links with
three queries per link, on a single-threaded server. Writes now refresh only
the customer that changed; the daily thread still does the full pass.
"""

from __future__ import annotations

import datetime
import sqlite3
import threading
import time
from typing import Iterable, Optional

from . import config
from .db import connect

_LAST_CONTACT_SQL = '''
    SELECT c.id AS customer_id,
           MAX(COALESCE(a.last_contact, DATE(c.created_at))) AS last_contact
      FROM customers c
      LEFT JOIN (
            SELECT customer_id, MAX(DATE(created_at)) AS last_contact
              FROM notes GROUP BY customer_id
            UNION ALL
            SELECT customer_id, MAX(COALESCE(contact_date, DATE(created_at)))
              FROM interactions GROUP BY customer_id
            UNION ALL
            SELECT customer_id, MAX(DATE(created_at))
              FROM tasks WHERE title NOT LIKE 'Herinnering:%' GROUP BY customer_id
      ) a ON a.customer_id = c.id
     {where}
     GROUP BY c.id
'''


def _refresh(conn: sqlite3.Connection, customer_ids: Optional[Iterable[int]] = None) -> None:
    ids = list(customer_ids) if customer_ids is not None else None
    if ids is not None and not ids:
        return

    if ids is None:
        where, args = '', ()
    else:
        placeholders = ','.join('?' * len(ids))
        where, args = f'WHERE c.id IN ({placeholders})', tuple(ids)

    last_contact = {
        r['customer_id']: r['last_contact']
        for r in conn.execute(_LAST_CONTACT_SQL.format(where=where), args)
    }
    if not last_contact:
        return

    link_where = '' if ids is None else f"WHERE cu.customer_id IN ({','.join('?' * len(ids))})"
    links = conn.execute(f'''
        SELECT cu.customer_id, cu.user_id, cu.reminder_paused_until,
               c.name AS customer_name,
               COALESCE(c.relation_type, 'extern') AS relation_type,
               u.username AS account_name
          FROM customer_users cu
          JOIN customers c ON c.id = cu.customer_id
          JOIN users u     ON u.id = cu.user_id
          {link_where}
    ''', args).fetchall()

    today = datetime.date.today().isoformat()
    existing = {
        (r['customer_id'], r['user_id']): r
        for r in conn.execute(
            "SELECT id, customer_id, user_id, due_date FROM tasks "
            "WHERE status = 'open' AND title LIKE 'Herinnering:%'"
        )
    }

    for link in links:
        cid, uid = link['customer_id'], link['user_id']
        paused = link['reminder_paused_until']
        if paused and paused >= today:
            continue

        raw = last_contact.get(cid)
        if not raw:
            continue
        try:
            last_dt = datetime.datetime.strptime(str(raw)[:10], '%Y-%m-%d')
        except ValueError:
            continue

        days = (config.REMINDER_DAYS_INTERN if link['relation_type'] == 'intern'
                else config.REMINDER_DAYS_EXTERN)
        due = (last_dt + datetime.timedelta(days=days)).strftime('%Y-%m-%d')
        description = (f"Taak voor {link['account_name']}: neem contact op met "
                       f"{link['customer_name']}. Laatste contact: {last_dt.strftime('%d-%m-%Y')}.")

        current = existing.get((cid, uid))
        if current:
            if current['due_date'] != due:
                conn.execute('UPDATE tasks SET due_date = ?, description = ? WHERE id = ?',
                             (due, description, current['id']))
        else:
            conn.execute(
                'INSERT INTO tasks (title, description, due_date, customer_id, user_id) '
                'VALUES (?, ?, ?, ?, ?)',
                (f"Herinnering: neem contact op met {link['customer_name']}",
                 description, due, cid, uid),
            )


def refresh_for_customer(customer_id: Optional[int]) -> None:
    """Recalculate reminders for one customer. Cheap enough to call on writes."""
    if not customer_id:
        return
    try:
        with connect() as conn:
            _refresh(conn, [customer_id])
    except sqlite3.Error as exc:  # never let a reminder refresh break a request
        print(f'[reminders] refresh voor klant {customer_id} mislukt: {exc}')


def refresh_all() -> None:
    with connect() as conn:
        _refresh(conn, None)


def pause_reminder(conn: sqlite3.Connection, customer_id: int, user_id: int,
                   due_date: Optional[str]) -> None:
    """Stop a deleted reminder from immediately reappearing."""
    conn.execute(
        'UPDATE customer_users SET reminder_paused_until = ? WHERE customer_id = ? AND user_id = ?',
        (due_date or '9999-12-31', customer_id, user_id),
    )


def _loop() -> None:
    from .auth import purge_expired_sessions
    # Wait a full interval first so a restart does not immediately recreate
    # reminders somebody just dismissed.
    time.sleep(config.REMINDER_INTERVAL_SECONDS)
    while True:
        try:
            refresh_all()
            purge_expired_sessions()
        except Exception as exc:  # pragma: no cover - background thread
            print(f'[reminders] dagelijkse controle mislukt: {exc}')
        time.sleep(config.REMINDER_INTERVAL_SECONDS)


def start_background_thread() -> threading.Thread:
    thread = threading.Thread(target=_loop, name='reminders', daemon=True)
    thread.start()
    return thread
