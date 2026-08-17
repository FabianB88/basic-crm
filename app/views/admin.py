"""Beheer: gebruikers, velden, rapporten, audit log, import, en het profiel."""

from __future__ import annotations

import datetime
import html
import re

from .. import auth, config
from ..db import (connect, get_user_by_id, log_action, query_all, query_one, users_exist)
from ..importer import import_rows, parse_import_file, parse_multipart
from ..permissions import can_view_profile
from ..ui import alert, page_footer, page_header, post_button, stat_card
from .messages import CONVERSATIONS_SQL

FIELD_NAME_RE = re.compile(r'^[a-z0-9_]{1,40}$')


# ── Users ─────────────────────────────────────────────────────────────────
def users_list(ctx, error: str = '') -> None:
    users = query_all('SELECT id, username, email, created_at, is_admin, is_comm, is_governance '
                      'FROM users ORDER BY id ASC')
    body = page_header('Gebruikersbeheer', ctx)
    body += '<h2 class="mt-4">Gebruikers</h2>'
    body += alert(error, 'danger')
    body += '<div class="card"><div class="section-title">Huidige gebruikers</div>'

    if not users:
        body += '<p>Er zijn nog geen gebruikers.</p>'
    for user in users:
        uid = user['id']
        protected = uid == 1                     # het hoofdaccount blijft admin
        self_row = uid == ctx.user_id
        is_admin_user = protected or bool(user['is_admin'])
        badges = ''
        if is_admin_user:
            badges += ('<span class="badge badge-ok" style="background:#5C7A5A;color:#fff;'
                       'border:none;margin-left:0.4rem;">admin</span>')
        if user['is_comm']:
            badges += ('<span class="badge" style="background:#7A6E66;color:#fff;'
                       'margin-left:0.4rem;">comm</span>')
        if user['is_governance']:
            badges += ('<span class="badge" style="background:#7A8FA6;color:#fff;'
                       'margin-left:0.4rem;">gov</span>')

        buttons = f'<a href="/users/profile?id={uid}" class="btn btn-sm btn-secondary">Profiel</a> '
        if not protected:
            buttons += post_button(
                '/users/toggle-admin', ctx,
                'Verwijder admin' if is_admin_user else 'Maak admin',
                confirm=(f'Admin-rechten verwijderen van {user["username"]}?' if is_admin_user
                         else f'{user["username"]} admin maken?'),
                css='btn btn-sm btn-secondary', fields={'id': uid}) + ' '
            buttons += post_button(
                '/users/toggle-comm', ctx,
                'Comm uit' if user['is_comm'] else 'Comm aan',
                css='btn btn-sm btn-secondary', fields={'id': uid}) + ' '
            buttons += post_button(
                '/users/toggle-governance', ctx,
                'Gov uit' if user['is_governance'] else 'Gov aan',
                css='btn btn-sm btn-secondary', fields={'id': uid}) + ' '
            if not self_row:
                buttons += post_button(
                    '/users/delete', ctx, 'Verwijder',
                    confirm=f'Weet je zeker dat je {user["username"]} wilt verwijderen? '
                            f'Al hun taken en koppelingen verdwijnen mee.',
                    fields={'id': uid})

        body += (f'<div class="task-row" style="display:flex;justify-content:space-between;'
                 f'align-items:center;flex-wrap:wrap;gap:0.5rem;">'
                 f'<div><strong>{html.escape(user["username"])}</strong> '
                 f'({html.escape(user["email"])}){badges}'
                 f'<div style="font-size:0.8rem;color:#7A6E66;">Aangemaakt op '
                 f'{html.escape(str(user["created_at"] or "")[:10])}</div></div>'
                 f'<div style="display:flex;gap:0.3rem;flex-wrap:wrap;">{buttons}</div></div>')
    body += '</div>'

    body += f'''<div class="card">
        <div class="section-title">Nieuwe gebruiker toevoegen</div>
        <form method="post" action="/users/add">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Gebruikersnaam</label>
                <input type="text" name="username" class="form-control" required></div>
            <div class="mb-3"><label class="form-label">E‑mail</label>
                <input type="email" name="email" class="form-control" required></div>
            <div class="mb-3"><label class="form-label">Wachtwoord</label>
                <input type="password" name="password" class="form-control" required
                       minlength="{config.MIN_PASSWORD_LENGTH}" autocomplete="new-password">
                <small style="color:#B0A49A;">Minimaal {config.MIN_PASSWORD_LENGTH} tekens.</small></div>
            <button type="submit" class="btn btn-primary">Gebruiker toevoegen</button>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def user_add(ctx) -> None:
    if ctx.method == 'GET':
        ctx.redirect('/users')
        return
    ok, message = auth.create_user(ctx.f('username'), ctx.f('email'), ctx.f('password'))
    if not ok:
        users_list(ctx, error=message)
        return
    log_action(ctx.user_id, 'create', 'users', None, f'nieuwe gebruiker {ctx.f("username")}')
    ctx.redirect('/users')


def user_delete(ctx) -> None:
    uid = ctx.fint('id')
    if not uid or uid == 1:
        ctx.redirect('/users')
        return
    if uid == ctx.user_id:
        users_list(ctx, error='Je kunt je eigen account niet verwijderen.')
        return
    with connect() as conn:
        conn.execute('DELETE FROM users WHERE id = ?', (uid,))
    log_action(ctx.user_id, 'delete', 'users', uid)
    ctx.redirect('/users')


def _toggle(ctx, column: str) -> None:
    uid = ctx.fint('id')
    if not uid or (uid == 1 and column == 'is_admin'):
        ctx.redirect('/users')
        return
    with connect() as conn:
        row = conn.execute(f'SELECT {column} FROM users WHERE id = ?', (uid,)).fetchone()
        if row:
            value = 0 if row[column] else 1
            conn.execute(f'UPDATE users SET {column} = ? WHERE id = ?', (value, uid))
            log_action(ctx.user_id, 'update', 'users', uid, f'{column}={value}')
    ctx.redirect('/users')


def toggle_admin(ctx) -> None:
    _toggle(ctx, 'is_admin')


def toggle_comm(ctx) -> None:
    _toggle(ctx, 'is_comm')


def toggle_governance(ctx) -> None:
    _toggle(ctx, 'is_governance')


# ── Profile ───────────────────────────────────────────────────────────────
def user_profile(ctx) -> None:
    profile_id = ctx.qint('id') or ctx.user_id
    if not can_view_profile(ctx, profile_id):
        ctx.forbidden('Je kunt alleen je eigen profiel bekijken.')
        return
    user = get_user_by_id(profile_id)
    if not user:
        ctx.not_found()
        return

    today = datetime.date.today().isoformat()
    with connect(readonly=True) as conn:
        open_tasks = conn.execute('''
            SELECT t.id AS task_id, t.title, t.due_date, t.description,
                   c.name AS customer_name, c.id AS customer_id
              FROM tasks t JOIN customers c ON t.customer_id = c.id
             WHERE t.user_id = ? AND t.status = 'open'
             ORDER BY COALESCE(t.due_date,'9999-12-31') ASC''', (profile_id,)).fetchall()
        done_tasks = conn.execute('''
            SELECT t.title, c.name AS customer_name, c.id AS customer_id
              FROM tasks t JOIN customers c ON t.customer_id = c.id
             WHERE t.user_id = ? AND t.status = 'completed'
             ORDER BY t.created_at DESC LIMIT 20''', (profile_id,)).fetchall()
        linked = conn.execute('''
            SELECT c.id, c.name, c.company, c.email, c.phone, c.category
              FROM customer_users cu JOIN customers c ON cu.customer_id = c.id
             WHERE cu.user_id = ? ORDER BY c.name ASC''', (profile_id,)).fetchall()
        interactions = conn.execute('''
            SELECT i.interaction_type, i.note, i.contact_date, i.created_at,
                   c.name AS customer_name, c.id AS customer_id
              FROM interactions i JOIN customers c ON i.customer_id = c.id
             WHERE i.user_id = ?
             ORDER BY COALESCE(i.contact_date, DATE(i.created_at)) DESC LIMIT 30''',
            (profile_id,)).fetchall()
        notes = conn.execute('''
            SELECT n.content, n.created_at, c.name AS customer_name, c.id AS customer_id
              FROM notes n JOIN customers c ON n.customer_id = c.id
             WHERE n.user_id = ? ORDER BY n.created_at DESC LIMIT 30''', (profile_id,)).fetchall()
        added = conn.execute('''
            SELECT id, name, company, category, created_at FROM customers
             WHERE created_by = ? ORDER BY created_at DESC LIMIT 100''', (profile_id,)).fetchall()
        # This block used to crash the whole page: the messages table did not exist.
        convs = conn.execute(CONVERSATIONS_SQL + ' LIMIT 10', {'me': profile_id}).fetchall()
        unread_total = conn.execute(
            'SELECT COUNT(*) FROM messages WHERE recipient_id = ? AND is_read = 0',
            (profile_id,)).fetchone()[0]

    overdue = [t for t in open_tasks if t['due_date'] and t['due_date'] < today]

    body = page_header(f'Profiel: {user["username"]}', ctx)
    body += f'<h2 class="mt-4"><i data-lucide=user class=icon></i> {html.escape(user["username"])}</h2>'
    body += (f'<p style="color:#7A6E66;">{html.escape(user["email"])} &middot; '
             f'Account aangemaakt op {html.escape(str(user["created_at"] or "")[:10])}</p>')

    body += '<div class="stat-row">'
    body += stat_card(len(open_tasks), 'Open taken')
    body += stat_card(len(overdue), 'Verlopen taken', '#C0392B' if overdue else '#1C1713')
    body += stat_card(len(linked), 'Gekoppelde klanten')
    body += stat_card(len(interactions), 'Recente interacties')
    body += '</div>'

    # Open tasks
    body += '<div class="card"><div class="section-title">Open taken</div>'
    if open_tasks:
        for task in open_tasks:
            is_overdue = task['due_date'] and task['due_date'] < today
            color = '#C0392B' if is_overdue else '#B0A49A'
            desc = (f'<br><small style="color:#7A6E66;">{html.escape(task["description"])}</small>'
                    if task['description'] else '')
            resolve = ''
            if profile_id == ctx.user_id:
                resolve = (f' <a href="/tasks/resolve?id={task["task_id"]}&from=users/profile" '
                           f'class="btn btn-sm btn-secondary">'
                           f'<i data-lucide=check class=icon></i> Resolve</a>')
            body += (f'<div class="task-row">'
                     f'<a href="/customers/view?id={task["customer_id"]}" '
                     f'style="font-weight:bold;">{html.escape(task["customer_name"])}</a> '
                     f'&mdash; {html.escape(task["title"])}{desc}'
                     f'<span style="float:right;color:{color};font-size:0.85rem;">'
                     f'<i data-lucide=calendar class=icon></i> '
                     f'{html.escape(str(task["due_date"] or "-"))}{resolve}</span></div>')
    else:
        body += '<p style="color:#5C7A5A;">Geen open taken.</p>'
    body += '</div>'

    # Linked customers
    body += '<div class="card"><div class="section-title">Gekoppelde klanten</div>'
    if linked:
        body += ('<div class="table-wrap"><table><thead><tr><th>Naam</th><th>Bedrijf</th>'
                 '<th>Type</th><th>E-mail</th><th>Telefoon</th></tr></thead><tbody>')
        for cust in linked:
            body += (f'<tr><td><a href="/customers/view?id={cust["id"]}">'
                     f'{html.escape(cust["name"])}</a></td>'
                     f'<td>{html.escape(cust["company"] or "-")}</td>'
                     f'<td>{html.escape((cust["category"] or "klant").capitalize())}</td>'
                     f'<td>{html.escape(cust["email"] or "")}</td>'
                     f'<td>{html.escape(cust["phone"] or "-")}</td></tr>')
        body += '</tbody></table></div>'
    else:
        body += '<p style="color:#B0A49A;">Geen gekoppelde klanten.</p>'
    body += '</div>'

    # Interactions
    labels = {'call': 'Bellen', 'email': 'E-mail', 'message': 'Bericht', 'meeting': 'Meeting'}
    body += '<div class="card"><div class="section-title">Recente interacties</div>'
    if interactions:
        for item in interactions:
            date = item['contact_date'] or (item['created_at'] or '')[:10]
            note = f' — <em>{html.escape(item["note"])}</em>' if item['note'] else ''
            body += (f'<div style="border-bottom:1px solid #EDE8E3;padding:0.4rem 0;">'
                     f'<small style="color:#B0A49A;">{html.escape(str(date))}</small> '
                     f'<strong>{html.escape(labels.get(item["interaction_type"], ""))}</strong> '
                     f'&middot; <a href="/customers/view?id={item["customer_id"]}">'
                     f'{html.escape(item["customer_name"])}</a>{note}</div>')
    else:
        body += '<p style="color:#B0A49A;">Nog geen interacties geregistreerd.</p>'
    body += '</div>'

    body += _collapsible('Toegevoegde notities', len(notes), ''.join(
        f'<div style="border-bottom:1px solid #EDE8E3;padding:0.4rem 0;">'
        f'<small style="color:#B0A49A;">{html.escape(str(n["created_at"] or "")[:10])}</small> '
        f'&middot; <a href="/customers/view?id={n["customer_id"]}">'
        f'{html.escape(n["customer_name"])}</a><br>'
        f'<span>{html.escape(((n["content"] or "")[:120] + "…") if len(n["content"] or "") > 120 else (n["content"] or ""))}</span>'
        f'</div>' for n in notes) or '<p>Nog geen notities toegevoegd.</p>')

    body += _collapsible('Toegevoegde klanten', len(added), ''.join(
        f'<div style="border-bottom:1px solid #EDE8E3;padding:0.4rem 0;">'
        f'<a href="/customers/view?id={c["id"]}" style="font-weight:bold;">'
        f'{html.escape(c["name"])}</a>'
        f'{(" &middot; " + html.escape(c["company"])) if c["company"] else ""}'
        f'<span style="font-size:0.8rem;color:#B0A49A;float:right;">'
        f'{html.escape((c["category"] or "klant").capitalize())} &middot; '
        f'{html.escape(str(c["created_at"] or "")[:10])}</span></div>'
        for c in added) or '<p>Nog geen klanten toegevoegd.</p>')

    if done_tasks:
        body += _collapsible('Voltooide taken', len(done_tasks), ''.join(
            f'<div style="border-bottom:1px solid #EDE8E3;padding:0.4rem 0;color:#B0A49A;">'
            f'<i data-lucide=check class=icon></i> {html.escape(t["title"])} &middot; '
            f'<a href="/customers/view?id={t["customer_id"]}" style="color:#B0A49A;">'
            f'{html.escape(t["customer_name"])}</a></div>' for t in done_tasks))

    if profile_id == ctx.user_id:
        badge = (f' <span style="background:#5C7A5A;color:#fff;border-radius:50%;font-size:0.75rem;'
                 f'font-weight:bold;padding:0.05rem 0.45rem;">{unread_total}</span>'
                 ) if unread_total else ''
        body += (f'<div class="card"><div class="section-title">'
                 f'<i data-lucide=message-circle class=icon></i> Berichten{badge} '
                 f'<a href="/messages" style="float:right;font-weight:normal;font-size:0.85rem;">'
                 f'Alle gesprekken</a></div>')
        if convs:
            for conv in convs:
                unread = conv['unread'] or 0
                snippet = html.escape((conv['last_content'] or '')[:60])
                body += (f'<a href="/messages/conversation?with={conv["other_id"]}" '
                         f'style="display:block;padding:0.6rem 0;border-bottom:1px solid #EDE8E3;'
                         f'text-decoration:none;color:inherit;">'
                         f'<span style="font-weight:{"bold" if unread else "normal"};">'
                         f'{html.escape(conv["other_name"])}</span>'
                         f'<div style="font-size:0.82rem;color:#B0A49A;">{snippet}</div></a>')
        else:
            body += '<p style="color:#B0A49A;">Nog geen berichten.</p>'
        body += '</div>'

    body += page_footer()
    ctx.html(body)


def _collapsible(title: str, count: int, inner: str) -> str:
    return (f'<details style="margin-bottom:1rem;"><summary style="cursor:pointer;'
            f'font-weight:bold;padding:0.6rem 1rem;background:#fff;border-radius:8px;'
            f'border:1px solid #E4DDD6;">{html.escape(title)} ({count})</summary>'
            f'<div class="card" style="margin-top:0.25rem;">{inner}</div></details>')


# ── Custom fields ─────────────────────────────────────────────────────────
def fields_list(ctx, error: str = '') -> None:
    fields = query_all('SELECT * FROM customer_fields ORDER BY id ASC')
    body = page_header('Velden beheren', ctx)
    body += '<h2 class="mt-4">Aanpasbare velden</h2>'
    body += alert(error, 'danger')
    body += '<div class="card"><div class="section-title">Huidige velden</div>'
    if fields:
        for field in fields:
            body += (f'<div class="task-row" style="display:flex;justify-content:space-between;'
                     f'align-items:center;"><div><strong>{html.escape(field["label"])}</strong> '
                     f'<small>({html.escape(field["name"])})</small></div>'
                     + post_button('/fields/delete', ctx, 'Verwijder',
                                   confirm='Weet je zeker dat je dit veld wilt verwijderen?',
                                   fields={'id': field['id']}) + '</div>')
    else:
        body += '<p style="color:#B0A49A;">Er zijn nog geen extra velden.</p>'
    body += '</div>'
    body += f'''<div class="card"><div class="section-title">Nieuw veld toevoegen</div>
        <form method="post" action="/fields/add">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Interne naam
                (kleine letters, cijfers en _)</label>
                <input type="text" name="name" class="form-control" required
                       pattern="[a-z0-9_]+" maxlength="40"></div>
            <div class="mb-3"><label class="form-label">Label (weergave)</label>
                <input type="text" name="label" class="form-control" required></div>
            <button type="submit" class="btn btn-primary">Veld toevoegen</button>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def field_add(ctx) -> None:
    name = ctx.f('name').lower()
    label = ctx.f('label')
    if not name or not label:
        fields_list(ctx, error='Naam en label zijn verplicht.')
        return
    if not FIELD_NAME_RE.match(name):
        fields_list(ctx, error='Interne naam mag alleen kleine letters, cijfers en _ bevatten.')
        return
    with connect() as conn:
        if conn.execute('SELECT id FROM customer_fields WHERE name = ?', (name,)).fetchone():
            fields_list(ctx, error='Deze interne naam bestaat al.')
            return
        cur = conn.execute('INSERT INTO customer_fields (name, label) VALUES (?, ?)', (name, label))
        field_id = cur.lastrowid
    log_action(ctx.user_id, 'create', 'customer_fields', field_id, f'name={name}')
    ctx.redirect('/fields')


def field_delete(ctx) -> None:
    field_id = ctx.fint('id')
    if field_id:
        with connect() as conn:
            conn.execute('DELETE FROM customer_fields WHERE id = ?', (field_id,))
        log_action(ctx.user_id, 'delete', 'customer_fields', field_id)
    ctx.redirect('/fields')


# ── Reports ───────────────────────────────────────────────────────────────
def reports(ctx) -> None:
    with connect(readonly=True) as conn:
        customer_stats = conn.execute(
            "SELECT COALESCE(category,'onbekend') AS label, COUNT(*) AS count "
            'FROM customers GROUP BY label ORDER BY count DESC').fetchall()
        task_stats = conn.execute(
            'SELECT status AS label, COUNT(*) AS count FROM tasks '
            'GROUP BY status ORDER BY count DESC').fetchall()
        interaction_stats = conn.execute(
            'SELECT interaction_type AS label, COUNT(*) AS count FROM interactions '
            'GROUP BY interaction_type ORDER BY count DESC').fetchall()
        relation_stats = conn.execute(
            "SELECT COALESCE(relation_type,'extern') AS label, COUNT(*) AS count "
            'FROM customers GROUP BY label ORDER BY count DESC').fetchall()

    def bars(rows, title):
        out = f'<div class="card"><div class="section-title">{html.escape(title)}</div>'
        if not rows:
            return out + '<p style="color:#B0A49A;">Geen gegevens.</p></div>'
        largest = max((r['count'] for r in rows), default=1) or 1
        for row in rows:
            width = int(row['count'] / largest * 100)
            out += (f'<div style="margin:0.4rem 0;">'
                    f'<strong>{html.escape(str(row["label"]).capitalize())}</strong> '
                    f'({row["count"]})'
                    f'<div style="background:#EDE8E3;border-radius:4px;overflow:hidden;height:8px;">'
                    f'<div style="width:{width}%;background:#5C7A5A;height:100%;"></div></div></div>')
        return out + '</div>'

    body = page_header('Rapporten', ctx)
    body += '<h2 class="mt-4">Rapporten</h2>'
    body += bars(customer_stats, 'Klanten per type')
    body += bars(relation_stats, 'Klanten per relatie')
    body += bars(task_stats, 'Taken per status')
    body += bars(interaction_stats, 'Interacties per type')
    body += ('<p class="mt-3"><a href="/export" class="btn btn-secondary">'
             '<i data-lucide=download class=icon></i> Klanten exporteren (CSV)</a> '
             '<a href="/tasks/export" class="btn btn-secondary">'
             '<i data-lucide=download class=icon></i> Taken exporteren (CSV)</a></p>')
    body += page_footer()
    ctx.html(body)


# ── Audit log ─────────────────────────────────────────────────────────────
def audit_logs(ctx) -> None:
    logs = query_all('SELECT a.*, u.username FROM audit_logs a '
                     'LEFT JOIN users u ON a.user_id = u.id '
                     'ORDER BY a.created_at DESC, a.id DESC LIMIT 200')
    body = page_header('Audit logs', ctx)
    body += '<h2 class="mt-4">Audit logs</h2>'
    body += ('<div class="card"><div class="table-wrap"><table><thead><tr><th>ID</th>'
             '<th>Gebruiker</th><th>Actie</th><th>Tabel</th><th>Rij‑ID</th>'
             '<th>Details</th><th>Tijdstip</th></tr></thead><tbody>')
    if logs:
        for entry in logs:
            body += (f'<tr><td>{entry["id"]}</td>'
                     f'<td>{html.escape(entry["username"] or "-")}</td>'
                     f'<td>{html.escape(entry["action"])}</td>'
                     f'<td>{html.escape(entry["table_name"])}</td>'
                     f'<td>{entry["row_id"] if entry["row_id"] is not None else ""}</td>'
                     f'<td>{html.escape(entry["details"] or "")}</td>'
                     f'<td>{html.escape(str(entry["created_at"] or ""))}</td></tr>')
    else:
        body += '<tr><td colspan="7">Geen logboeken gevonden.</td></tr>'
    body += '</tbody></table></div></div>'
    body += page_footer()
    ctx.html(body)


# ── Import ────────────────────────────────────────────────────────────────
def import_page(ctx) -> None:
    if ctx.method == 'GET':
        _import_form(ctx)
        return

    content_type = ctx.handler.headers.get('Content-Type', '') or ''
    if 'multipart/form-data' not in content_type:
        _import_form(ctx, error='Ongeldig formulier.')
        return
    try:
        length = int(ctx.handler.headers.get('Content-Length', '0') or 0)
    except ValueError:
        length = 0
    if length > config.MAX_UPLOAD_BYTES:
        _import_form(ctx, error='Bestand is te groot (max 5MB).')
        return

    try:
        parts = parse_multipart(ctx.body(), content_type)
    except Exception:
        _import_form(ctx, error='Fout bij het verwerken van het uploadformulier.')
        return

    # Multipart bodies bypass ctx.form(), so the CSRF token is validated here.
    token = parts.get('csrf_token')
    submitted = token[1].decode('utf-8', 'replace').strip() if token else ''
    if not auth.csrf_ok(submitted, ctx.csrf_token):
        ctx.forbidden('Je sessie is verlopen. Ververs de pagina en probeer het opnieuw.')
        return

    uploaded = parts.get('file')
    if not uploaded or not uploaded[1]:
        _import_form(ctx, error='Selecteer een bestand om te importeren.')
        return
    filename, payload = uploaded
    filename = filename or 'upload.csv'
    if not filename.lower().endswith(('.csv', '.xlsx', '.txt')):
        _import_form(ctx, error='Alleen .csv of .xlsx bestanden worden ondersteund.')
        return

    dynamic = [f['name'] for f in query_all('SELECT name FROM customer_fields')]
    try:
        rows = parse_import_file(payload, filename, dynamic)
    except Exception as exc:
        _import_form(ctx, error=f'Importfout: {exc}')
        return

    imported, errors = import_rows(rows, ctx.user_id)
    _import_result(ctx, imported, errors, skipped=len(rows) - imported)


def _import_form(ctx, error: str = '') -> None:
    body = page_header('Importeren', ctx)
    body += '<h2 class="mt-4">Klantgegevens importeren</h2>'
    body += alert(error, 'danger')
    body += f'''<div class="card">
        <div class="section-title">CSV/XLSX‑bestand uploaden</div>
        <form method="post" action="/import" enctype="multipart/form-data">
            {ctx.csrf_input()}
            <div class="mb-3"><input type="file" name="file" accept=".csv,.xlsx" required></div>
            <button type="submit" class="btn btn-primary">Importeer</button>
        </form>
        <p class="mt-2"><small>Het bestand moet kolomnamen bevatten. Zowel Nederlandse
            (Naam, Bedrijf, E‑mail, Telefoon, Adres, Tags, Type) als Engelse varianten
            (name, company, email, phone, address, tags, category) worden herkend.
            Voor dynamische velden gebruik <code>cf_veldnaam</code>. Onbekende kolommen
            worden genegeerd.</small></p>
    </div>'''
    body += page_footer()
    ctx.html(body)


def _import_result(ctx, imported: int, errors, skipped: int) -> None:
    body = page_header('Importresultaat', ctx)
    body += '<h2 class="mt-4">Import resultaat</h2>'
    body += f'<div class="card"><p><strong>{imported}</strong> klanten geïmporteerd.'
    if skipped:
        body += f' {skipped} rijen overgeslagen.'
    body += '</p>'
    if errors:
        body += '<div class="mt-3"><strong>Meldingen:</strong><ul>'
        for message in errors[:50]:
            body += f'<li>{html.escape(message)}</li>'
        if len(errors) > 50:
            body += f'<li>… en nog {len(errors) - 50} meldingen.</li>'
        body += '</ul></div>'
    body += '</div>'
    body += '<p class="mt-3"><a href="/customers" class="btn btn-primary">Terug naar klanten</a></p>'
    body += page_footer()
    ctx.html(body)
