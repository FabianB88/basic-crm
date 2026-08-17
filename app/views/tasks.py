"""Customer-linked tasks: creation, resolving, search, archive and export."""

from __future__ import annotations

import csv
import datetime
import html
import io
import urllib.parse

from .. import config, reminders
from ..db import connect, get_customer, log_action, query_all, query_one
from ..permissions import can_manage_task
from ..ui import alert, page_footer, page_header, post_button

RESOLVE_RETURN = {'dashboard': '/dashboard',
                  'users/profile': '/users/profile',
                  'tasks/search': '/tasks/search'}
INTERACTION_LABELS = {'call': 'Bellen', 'email': 'E-mail', 'message': 'Bericht', 'meeting': 'Meeting'}


def add(ctx) -> None:
    customer_id = ctx.fint('customer_id')
    if not customer_id:
        ctx.not_found()
        return
    customer = get_customer(customer_id)
    if not customer:
        ctx.not_found()
        return

    title = ctx.f('title')
    if not title:
        # The old code passed a possibly-None customer straight into the detail
        # renderer here, which raised a TypeError instead of showing the error.
        from .customers import _detail_page
        _detail_page(ctx, customer, task_error='Titel is verplicht.')
        return

    assigned = ctx.fint('assigned_user_id') or ctx.user_id
    if not query_one('SELECT id FROM users WHERE id = ?', (assigned,)):
        assigned = ctx.user_id

    with connect() as conn:
        cur = conn.execute(
            'INSERT INTO tasks (title, description, due_date, customer_id, user_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (title, ctx.f('description') or None, ctx.f('due_date') or None,
             customer_id, assigned))
        task_id = cur.lastrowid
    log_action(ctx.user_id, 'create', 'tasks', task_id, f'title={title}')
    reminders.refresh_for_customer(customer_id)
    ctx.redirect(f'/customers/view?id={customer_id}')


def complete(ctx) -> None:
    task_id = ctx.fint('id')
    customer_id = ctx.fint('customer_id')
    if not task_id or not customer_id:
        ctx.not_found()
        return
    if not can_manage_task(ctx, task_id):
        ctx.forbidden('Je kunt alleen je eigen taken afronden.')
        return
    with connect() as conn:
        conn.execute("UPDATE tasks SET status = 'completed' WHERE id = ?", (task_id,))
    log_action(ctx.user_id, 'update', 'tasks', task_id, 'status=completed')
    ctx.redirect(f'/customers/view?id={customer_id}')


def delete(ctx) -> None:
    task_id = ctx.fint('id')
    customer_id = ctx.fint('customer_id')
    if not task_id:
        ctx.not_found()
        return
    if not can_manage_task(ctx, task_id):
        ctx.forbidden('Je kunt alleen je eigen taken verwijderen.')
        return
    with connect() as conn:
        row = conn.execute(
            'SELECT customer_id, user_id, due_date, title FROM tasks WHERE id = ?', (task_id,)
        ).fetchone()
        if row and (row['title'] or '').startswith('Herinnering:'):
            reminders.pause_reminder(conn, row['customer_id'], row['user_id'], row['due_date'])
        conn.execute('DELETE FROM tasks WHERE id = ?', (task_id,))
    log_action(ctx.user_id, 'delete', 'tasks', task_id)
    ctx.redirect(f'/customers/view?id={customer_id}' if customer_id else '/tasks/search')


# ── Resolve: complete a task and log the contact in one step ──────────────
def resolve(ctx) -> None:
    task_id = ctx.qint('id') or ctx.fint('id')
    if not task_id:
        ctx.not_found()
        return
    task = query_one('''
        SELECT t.id, t.title, t.description, t.customer_id, c.name AS customer_name
          FROM tasks t JOIN customers c ON t.customer_id = c.id
         WHERE t.id = ?''', (task_id,))
    if not task:
        ctx.not_found()
        return
    if not can_manage_task(ctx, task_id):
        ctx.forbidden('Deze taak is aan iemand anders toegewezen.')
        return

    origin = ctx.q('from') or ctx.f('from') or 'dashboard'
    if origin not in RESOLVE_RETURN:
        origin = 'dashboard'

    if ctx.method == 'GET':
        _resolve_form(ctx, task, origin)
        return

    interaction_type = ctx.f('interaction_type')
    if interaction_type not in config.INTERACTION_TYPES:
        _resolve_form(ctx, task, origin, error='Kies een contactmoment type.')
        return

    with connect() as conn:
        conn.execute("UPDATE tasks SET status = 'completed' WHERE id = ?", (task_id,))
        cur = conn.execute(
            'INSERT INTO interactions (interaction_type, note, contact_date, customer_id, user_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (interaction_type, ctx.f('note') or None, ctx.f('contact_date') or None,
             task['customer_id'], ctx.user_id))
        interaction_id = cur.lastrowid

    log_action(ctx.user_id, 'update', 'tasks', task_id, 'status=completed via resolve')
    log_action(ctx.user_id, 'create', 'interactions', interaction_id,
               f'type={interaction_type} via resolve')
    reminders.refresh_for_customer(task['customer_id'])

    destination = RESOLVE_RETURN[origin]
    if origin == 'users/profile':
        destination = f'/users/profile?id={ctx.user_id}'
    ctx.redirect(destination)


def _resolve_form(ctx, task, origin: str, error: str = '') -> None:
    today = datetime.date.today().isoformat()
    options = ''.join(f'<option value="{value}">{label}</option>'
                      for value, label in INTERACTION_LABELS.items())
    body = page_header('Taak afronden', ctx)
    body += alert(error, 'danger')
    body += f'''<div class="card" style="max-width:560px;margin:2rem auto;">
        <h2><i data-lucide=check class=icon></i> Taak afronden</h2>
        <p><strong>{html.escape(task['title'])}</strong><br>
        <small style="color:#7A6E66;">Klant:
            <a href="/customers/view?id={task['customer_id']}">
                {html.escape(task['customer_name'])}</a></small></p>
        <form method="POST" action="/tasks/resolve">
            {ctx.csrf_input()}
            <input type="hidden" name="id" value="{task['id']}">
            <input type="hidden" name="from" value="{html.escape(origin)}">
            <div class="mb-3"><label class="form-label">Contactmoment type *</label>
                <select name="interaction_type" class="form-select" required>
                    <option value="">-- Kies type --</option>{options}</select></div>
            <div class="mb-3"><label class="form-label">Datum contact</label>
                <input type="date" name="contact_date" class="form-control" value="{today}"></div>
            <div class="mb-3"><label class="form-label">Notitie (optioneel)</label>
                <textarea name="note" class="form-control" rows="3"
                          placeholder="Wat is er besproken?"></textarea></div>
            <button type="submit" class="btn btn-primary">
                <i data-lucide=check class=icon></i> Afronden &amp; interactie opslaan</button>
            <a href="{RESOLVE_RETURN[origin]}" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


# ── Search ────────────────────────────────────────────────────────────────
def search(ctx) -> None:
    term = ctx.q('q')
    filter_uid = ctx.qint('user_id')
    status = ctx.q('status')

    conditions, args = ['1=1'], []
    if term:
        conditions.append('(t.title LIKE ? OR t.description LIKE ?)')
        args.extend([f'%{term}%'] * 2)
    if filter_uid:
        conditions.append('t.user_id = ?')
        args.append(filter_uid)
    if status == 'verlopen':
        conditions.append("t.status = 'open' AND t.due_date < DATE('now')")
    elif status in ('open', 'completed', 'archief'):
        conditions.append('t.status = ?')
        args.append(status)
    else:
        # Zonder filter blijft het archief buiten beeld; anders vult het
        # opgeruimde werk de zoekresultaten.
        conditions.append("t.status != 'archief'")

    rows = query_all(f'''
        SELECT t.id AS task_id, t.title, t.description, t.due_date, t.status,
               c.name AS customer_name, c.id AS customer_id, u.username AS assigned_to
          FROM tasks t
          JOIN customers c ON t.customer_id = c.id
          JOIN users u     ON t.user_id = u.id
         WHERE {' AND '.join(conditions)}
         ORDER BY COALESCE(t.due_date,'9999-12-31') ASC, t.created_at DESC
         LIMIT 200''', tuple(args))
    users = query_all('SELECT id, username FROM users ORDER BY username ASC')

    today = datetime.date.today().isoformat()
    body = page_header('Taken zoeken', ctx)
    body += '<h2 class="mt-4"><i data-lucide=search class=icon></i> Taken zoeken</h2>'

    user_options = '<option value="">Alle gebruikers</option>' + ''.join(
        f'<option value="{u["id"]}"{" selected" if filter_uid == u["id"] else ""}>'
        f'{html.escape(u["username"])}</option>' for u in users)
    status_options = ''.join(
        f'<option value="{value}"{" selected" if status == value else ""}>{label}</option>'
        for value, label in [('', 'Alles behalve archief'), ('open', 'Open'),
                             ('verlopen', 'Verlopen'), ('completed', 'Voltooid'),
                             ('archief', 'Gearchiveerd')])

    body += f'''<div class="card" style="padding:0.75rem 1rem;">
        <form method="GET" action="/tasks/search"
              style="display:flex;gap:0.75rem;align-items:flex-end;flex-wrap:wrap;">
            <div><label class="form-label">Zoekterm</label>
                <input type="search" name="q" value="{html.escape(term)}" class="form-control"
                       placeholder="Taaktitel of omschrijving..." style="min-width:220px;"></div>
            <div><label class="form-label">Gebruiker</label>
                <select name="user_id" class="form-select">{user_options}</select></div>
            <div><label class="form-label">Status</label>
                <select name="status" class="form-select">{status_options}</select></div>
            <button type="submit" class="btn btn-primary">Zoeken</button>
            <a href="/tasks/search" class="btn btn-link">Wis filter</a>
        </form></div>'''

    if ctx.is_admin:
        body += ('<div style="margin-top:0.75rem;display:flex;gap:0.5rem;'
                 'justify-content:flex-end;flex-wrap:wrap;">')
        body += post_button('/tasks/delete-all-open', ctx,
                            '<i data-lucide=trash-2 class=icon></i> Alle open taken verwijderen',
                            confirm='Alle openstaande taken verwijderen? Dit kan niet ongedaan worden.',
                            css='btn btn-sm btn-danger')
        body += post_button('/tasks/delete-overdue', ctx,
                            '<i data-lucide=trash-2 class=icon></i> Verlopen taken verwijderen',
                            confirm='Alle verlopen taken verwijderen? Dit kan niet ongedaan worden.',
                            css='btn btn-sm btn-danger')
        body += '</div>'

    body += f'<div class="card"><div class="section-title">Resultaten ({len(rows)})</div>'
    if rows:
        body += ('<div class="table-wrap"><table><thead><tr><th>Taak</th><th>Klant</th>'
                 '<th>Toegewezen aan</th><th>Vervaldatum</th><th>Status</th></tr></thead><tbody>')
        for task in rows:
            overdue = task['due_date'] and task['due_date'] < today and task['status'] == 'open'
            color = '#C0392B' if overdue else '#555'
            badge = {
                'completed': '<span class="badge badge-ok">Voltooid</span>',
                'archief': '<span class="badge badge-muted">Gearchiveerd</span>',
            }.get(task['status'], '<span class="badge badge-warn">Open</span>')
            desc = (f'<br><small style="color:#B0A49A;">{html.escape(task["description"])}</small>'
                    if task['description'] else '')
            resolve_link = ''
            if task['status'] == 'open':
                resolve_link = (f' <a href="/tasks/resolve?id={task["task_id"]}&from=tasks/search" '
                                f'class="btn btn-sm btn-secondary">'
                                f'<i data-lucide=check class=icon></i></a>')
            body += (f'<tr><td>{html.escape(task["title"])}{desc}{resolve_link}</td>'
                     f'<td><a href="/customers/view?id={task["customer_id"]}">'
                     f'{html.escape(task["customer_name"])}</a></td>'
                     f'<td>{html.escape(task["assigned_to"] or "")}</td>'
                     f'<td style="color:{color};">{html.escape(str(task["due_date"] or "-"))}</td>'
                     f'<td>{badge}</td></tr>')
        body += '</tbody></table></div>'
    else:
        body += '<p style="color:#B0A49A;">Geen taken gevonden.</p>'
    body += '</div>'
    body += page_footer()
    ctx.html(body)


# ── Archive ───────────────────────────────────────────────────────────────
def archive(ctx) -> None:
    """Voltooide taken plus taken die automatisch zijn opgeruimd."""
    filter_uid = ctx.qint('user_id')
    kind = ctx.choice(ctx.q('soort'), ('completed', 'archief'), '')

    conditions = ["t.status IN ('completed','archief')"]
    args = []
    if filter_uid:
        conditions.append('t.user_id = ?')
        args.append(filter_uid)
    if kind:
        conditions.append('t.status = ?')
        args.append(kind)

    rows = query_all(f'''
        SELECT t.id AS task_id, t.title, t.description, t.due_date, t.created_at, t.status,
               c.name AS customer_name, c.id AS customer_id, u.username AS assigned_to
          FROM tasks t
          JOIN customers c ON t.customer_id = c.id
          JOIN users u     ON t.user_id = u.id
         WHERE {' AND '.join(conditions)}
         ORDER BY t.created_at DESC LIMIT 500''', tuple(args))
    users = query_all('SELECT id, username FROM users ORDER BY username ASC')

    body = page_header('Archief', ctx)
    body += '<h2 class="mt-4"><i data-lucide=archive class=icon></i> Archief</h2>'
    body += (f'<p style="color:#7A6E66;font-size:0.875rem;">Afgeronde taken, en taken die '
             f'automatisch zijn opgeruimd — meer dan {config.TASK_ARCHIVE_AFTER_DAYS} dagen '
             f'over hun vervaldatum, of meegenomen in de eenmalige schoonmaak. '
             f'Gearchiveerde taken kun je per stuk terugzetten.</p>')

    user_options = '<option value="">Alle gebruikers</option>' + ''.join(
        f'<option value="{u["id"]}"{" selected" if filter_uid == u["id"] else ""}>'
        f'{html.escape(u["username"])}</option>' for u in users)
    kind_options = ''.join(
        f'<option value="{value}"{" selected" if kind == value else ""}>{label}</option>'
        for value, label in [('', 'Alles'), ('completed', 'Afgerond'),
                             ('archief', 'Automatisch gearchiveerd')])
    body += f'''<div class="card" style="padding:0.75rem 1rem;">
        <form method="GET" action="/tasks/archive"
              style="display:flex;gap:1rem;align-items:flex-end;flex-wrap:wrap;">
            <div><label class="form-label">Gebruiker</label>
                <select name="user_id" class="form-select" onchange="this.form.submit()">
                    {user_options}</select></div>
            <div><label class="form-label">Soort</label>
                <select name="soort" class="form-select" onchange="this.form.submit()">
                    {kind_options}</select></div>
            <a href="/tasks/archive" class="btn btn-link">Wis filter</a>
        </form></div>'''

    export_link = ('<a href="/tasks/export" style="float:right;font-size:0.85rem;font-weight:normal;">'
                   '<i data-lucide=download class=icon></i> Exporteer alle taken (CSV)</a>'
                   ) if ctx.is_admin else ''
    body += f'<div class="card"><div class="section-title">Taken ({len(rows)}){export_link}</div>'
    if rows:
        body += ('<div class="table-wrap"><table><thead><tr><th>Taak</th><th>Status</th>'
                 '<th>Klant</th><th>Toegewezen aan</th><th>Vervaldatum</th><th>Aangemaakt</th>'
                 '<th class="text-end"></th></tr></thead><tbody>')
        for task in rows:
            desc = (f'<br><small style="color:#B0A49A;">{html.escape(task["description"])}</small>'
                    if task['description'] else '')
            if task['status'] == 'archief':
                badge = '<span class="badge badge-muted">Gearchiveerd</span>'
                action = post_button(
                    '/tasks/reopen', ctx, 'Terugzetten', css='btn btn-sm btn-secondary',
                    fields={'id': task['task_id'], 'from': 'archive'})
            else:
                badge = '<span class="badge badge-ok">Afgerond</span>'
                action = ''
            body += (f'<tr><td>{html.escape(task["title"])}{desc}</td>'
                     f'<td>{badge}</td>'
                     f'<td><a href="/customers/view?id={task["customer_id"]}">'
                     f'{html.escape(task["customer_name"])}</a></td>'
                     f'<td>{html.escape(task["assigned_to"] or "")}</td>'
                     f'<td style="color:#7A6E66;">{html.escape(str(task["due_date"] or "-"))}</td>'
                     f'<td style="color:#B0A49A;font-size:0.85rem;">'
                     f'{html.escape(str(task["created_at"] or "")[:10])}</td>'
                     f'<td class="text-end">{action}</td></tr>')
        body += '</tbody></table></div>'
    else:
        body += '<p style="color:#B0A49A;">Niets in het archief.</p>'
    body += '</div>'
    body += page_footer()
    ctx.html(body)


def reopen(ctx) -> None:
    """Haal een automatisch gearchiveerde taak terug naar de actieve lijst.

    De vervaldatum gaat naar vandaag, anders zou de opruimactie hem bij de
    eerstvolgende ronde meteen opnieuw archiveren. Bij een herinnering vervalt
    ook de pauze, zodat de normale cyclus weer geldt.
    """
    task_id = ctx.fint('id')
    if not task_id:
        ctx.not_found()
        return
    if not can_manage_task(ctx, task_id):
        ctx.forbidden('Je kunt alleen je eigen taken terugzetten.')
        return

    with connect() as conn:
        row = conn.execute(
            'SELECT customer_id, user_id, title, status FROM tasks WHERE id = ?',
            (task_id,)).fetchone()
        if not row or row['status'] != 'archief':
            ctx.redirect('/tasks/archive')
            return
        conn.execute("UPDATE tasks SET status = 'open', due_date = DATE('now') WHERE id = ?",
                     (task_id,))
        if (row['title'] or '').startswith('Herinnering:'):
            conn.execute(
                'UPDATE customer_users SET reminder_paused_until = NULL '
                'WHERE customer_id = ? AND user_id = ?',
                (row['customer_id'], row['user_id']))

    log_action(ctx.user_id, 'update', 'tasks', task_id, 'teruggezet uit archief')
    ctx.redirect('/tasks/archive' if ctx.f('from') == 'archive' else '/dashboard')


# ── Bulk maintenance (admin) ──────────────────────────────────────────────
def _delete_open(ctx, only_overdue: bool) -> None:
    clause = "status = 'open'" + (" AND due_date < DATE('now')" if only_overdue else '')
    with connect() as conn:
        for row in conn.execute(
            f"SELECT customer_id, user_id, due_date FROM tasks "
            f"WHERE {clause} AND title LIKE 'Herinnering:%'"
        ).fetchall():
            reminders.pause_reminder(conn, row['customer_id'], row['user_id'], row['due_date'])
        cur = conn.execute(f'DELETE FROM tasks WHERE {clause}')
        removed = cur.rowcount
    log_action(ctx.user_id, 'delete', 'tasks', None,
               f"bulk delete {'verlopen' if only_overdue else 'open'}: {removed} taken")
    ctx.redirect('/tasks/search')


def delete_all_open(ctx) -> None:
    _delete_open(ctx, only_overdue=False)


def delete_overdue(ctx) -> None:
    _delete_open(ctx, only_overdue=True)


# ── Export ────────────────────────────────────────────────────────────────
def export_csv(ctx) -> None:
    columns = ['id', 'title', 'description', 'status', 'due_date',
               'customer_name', 'assigned_to', 'created_at']
    rows = query_all('''
        SELECT t.id, t.title, t.description, t.status, t.due_date, t.created_at,
               c.name AS customer_name, u.username AS assigned_to
          FROM tasks t
          JOIN customers c ON t.customer_id = c.id
          JOIN users u     ON t.user_id = u.id
         ORDER BY t.created_at DESC''')
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row[c] if row[c] is not None else '' for c in columns])
    log_action(ctx.user_id, 'export', 'tasks', None, f'{len(rows)} rijen')
    ctx.csv('taken_export.csv', buffer.getvalue().encode('utf-8'))
