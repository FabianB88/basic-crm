"""Communicatie: kanban board, doelen, week, content kalender, datums, events gov."""

from __future__ import annotations

import datetime
import html
from collections import OrderedDict, defaultdict

from .. import config
from ..db import comm_members, connect, log_action, query_all, query_one
from ..ui import (alert, comm_nav, page_footer, page_header, post_button,
                  priority_badge, stat_card)

PLATFORM_CFG = {
    'instagram': ('image', '#C0392B', '#EDF3EC'),
    'linkedin': ('briefcase', '#7A8FA6', '#EDF3EC'),
    'website': ('globe', '#5C7A5A', '#EDF3EC'),
    'email': ('mail', '#B5916A', '#FEF8F0'),
    'overig': ('map-pin', '#7A6E66', '#F2EEE9'),
}
DATE_CFG = {
    'event': ('star', '#7A8FA6', '#EDF3EC'),
    'deadline': ('alert-triangle', '#C0392B', '#fef2f2'),
    'mijlpaal': ('flag', '#7A6E66', '#F2EEE9'),
}
EVENT_STATUSES = [('open', 'Open', '#FEF8F0', '#B5916A'),
                  ('in_check', 'In check', '#EDF3EC', '#7A8FA6'),
                  ('klaar', 'Klaar', '#EDF3EC', '#5C7A5A')]


def _today() -> str:
    return datetime.date.today().isoformat()


# ── Board ─────────────────────────────────────────────────────────────────
def board(ctx) -> None:
    today = _today()
    user_filter = ctx.qint('user_filter') or 0

    with connect(readonly=True) as conn:
        tasks = conn.execute('''
            SELECT ct.id, ct.title, ct.description, ct.status, ct.due_date, ct.goal_id,
                   ct.priority, ct.tags, ct.reminder_note, ct.assigned_to,
                   u.username AS assigned_to_name, cg.title AS goal_title
              FROM comm_tasks ct
              LEFT JOIN users u       ON ct.assigned_to = u.id
              LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
             WHERE ct.status != 'archief'
             ORDER BY COALESCE(ct.due_date,'9999-12-31') ASC,
                      CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                      ct.created_at DESC''').fetchall()
        member_stats = conn.execute('''
            SELECT u.id, u.username,
                   SUM(CASE WHEN ct.status NOT IN ('klaar','archief') THEN 1 ELSE 0 END) AS open_tasks,
                   SUM(CASE WHEN ct.status = 'klaar' THEN 1 ELSE 0 END) AS done_tasks,
                   SUM(CASE WHEN ct.status NOT IN ('klaar','archief')
                             AND ct.due_date < ? THEN 1 ELSE 0 END) AS overdue_tasks
              FROM users u LEFT JOIN comm_tasks ct ON ct.assigned_to = u.id
             WHERE u.is_comm = 1 OR u.is_admin = 1 OR u.id = 1
             GROUP BY u.id ORDER BY u.username ASC''', (today,)).fetchall()
        members = conn.execute('SELECT id, username FROM users '
                               'WHERE is_comm=1 OR is_admin=1 OR id=1 ORDER BY username').fetchall()
        goals = conn.execute("SELECT id, title FROM comm_goals "
                             "WHERE status='actief' ORDER BY title").fetchall()
        # assigned_to was missing from this SELECT, and the filter below called
        # .get() on a sqlite3.Row, which has no .get(). Filtering the board by a
        # team member raised AttributeError whenever a content item was pinned.
        content = conn.execute('''
            SELECT cc.id, cc.title, cc.platform, cc.publish_date, cc.board_status,
                   cc.assigned_to, u.username AS assigned_to_name
              FROM comm_content cc LEFT JOIN users u ON cc.assigned_to = u.id
             WHERE cc.board_status IS NOT NULL AND cc.board_status != ''
             ORDER BY COALESCE(cc.publish_date,'9999-12-31') ASC''').fetchall()

    def keep_task(row):
        return not user_filter or row['assigned_to'] == user_filter

    columns = {status: [t for t in tasks if t['status'] == status and keep_task(t)]
               for status in config.COMM_TASK_STATUSES}
    content_cols = {status: [c for c in content
                             if c['board_status'] == status and keep_task(c)]
                    for status in config.COMM_TASK_STATUSES}

    body = page_header('Communicatie Board', ctx)
    body += '<h2 class="mt-4"><i data-lucide=users class=icon></i> Communicatie Dashboard</h2>'
    body += comm_nav('board', ctx)

    # Filter pills
    body += ('<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin-bottom:0.75rem;'
             'align-items:center;"><span style="font-size:0.82rem;color:#B0A49A;">Filter:</span>')
    style = ('background:#7A8FA6;color:#fff;' if not user_filter else 'background:#EDE8E3;color:#444;')
    body += (f'<a href="/comm/board" style="text-decoration:none;border-radius:14px;'
             f'padding:0.25rem 0.75rem;font-size:0.82rem;font-weight:bold;{style}">Iedereen</a>')
    for member in members:
        active = user_filter == member['id']
        pill = 'background:#5C7A5A;color:#fff;' if active else 'background:#EDF3EC;color:#3d5c3b;'
        href = '/comm/board' if active else f'/comm/board?user_filter={member["id"]}'
        body += (f'<a href="{href}" style="text-decoration:none;border-radius:14px;'
                 f'padding:0.25rem 0.75rem;font-size:0.82rem;{pill}">'
                 f'<i data-lucide=user class=icon></i> {html.escape(member["username"])}</a>')
    body += '</div>'

    # Personal banners
    mine = [t for t in tasks if t['assigned_to'] == ctx.user_id
            and t['status'] not in ('klaar', 'archief')]
    soon_limit = (datetime.date.today() + datetime.timedelta(days=2)).isoformat()
    overdue = [t for t in mine if t['due_date'] and t['due_date'] < today]
    due_today = [t for t in mine if t['due_date'] == today]
    soon = [t for t in mine if t['due_date'] and today < t['due_date'] <= soon_limit]

    def banner(icon, color, bg, label, items):
        if not items:
            return ''
        names = ', '.join(f'<em>{html.escape(t["title"])}</em>' for t in items[:5])
        extra = '…' if len(items) > 5 else ''
        return (f'<div style="background:{bg};border-left:4px solid {color};border-radius:4px;'
                f'padding:0.65rem 1rem;margin-bottom:0.75rem;">'
                f'<i data-lucide={icon} class=icon></i> <strong>{label}</strong> {names}{extra}</div>')

    body += banner('alert-triangle', '#C0392B', '#fef2f2',
                   f'{len(overdue)} verlopen {"taak" if len(overdue) == 1 else "taken"} van jou:', overdue)
    body += banner('calendar', '#B5916A', '#FEF8F0', 'Vervalt vandaag:', due_today)
    body += banner('clock', '#B5916A', '#FEF8F0', 'Deadline binnen 48 uur:', soon)

    notes = [t for t in mine if t['reminder_note']]
    if notes:
        body += ('<div style="background:#EDF3EC;border-left:4px solid #5C7A5A;border-radius:4px;'
                 'padding:0.65rem 1rem;margin-bottom:0.75rem;">'
                 '<i data-lucide=bell class=icon></i> <strong>Herinneringen:</strong>'
                 '<ul style="margin:0.3rem 0 0 1.2rem;">')
        for task in notes:
            body += (f'<li><em>{html.escape(task["title"])}</em>: '
                     f'{html.escape(task["reminder_note"])}</li>')
        body += '</ul></div>'

    # Stats
    total_open = len(columns['backlog']) + len(columns['bezig'])
    total_overdue = sum(1 for t in tasks if t['status'] not in ('klaar', 'archief')
                        and t['due_date'] and t['due_date'] < today)
    body += '<div class="stat-row">'
    body += stat_card(total_open, 'Open taken', '#B5916A' if total_open else '#1C1713')
    body += stat_card(total_overdue, 'Verlopen', '#C0392B' if total_overdue else '#1C1713')
    body += stat_card(len(columns['klaar']), 'Afgerond')
    body += stat_card(len(goals), 'Actieve doelen')
    body += '</div>'

    body += ('<details style="margin-bottom:0.75rem;"><summary style="cursor:pointer;'
             'font-weight:bold;padding:0.55rem 1rem;background:#fff;border-radius:8px;'
             'border:1px solid #E4DDD6;"><i data-lucide=trending-up class=icon></i> '
             'Statistieken per teamlid</summary><div class="card" style="margin-top:0.25rem;">'
             '<div class="table-wrap"><table><thead><tr><th>Teamlid</th><th>Open</th>'
             '<th>Verlopen</th><th>Afgerond</th></tr></thead><tbody>')
    for member in member_stats:
        overdue_count = member['overdue_tasks'] or 0
        color = '#C0392B' if overdue_count else '#5C7A5A'
        body += (f'<tr><td><a href="/comm/profile?id={member["id"]}">'
                 f'{html.escape(member["username"])}</a></td>'
                 f'<td>{member["open_tasks"] or 0}</td>'
                 f'<td style="color:{color};font-weight:bold;">{overdue_count}</td>'
                 f'<td style="color:#5C7A5A;">{member["done_tasks"] or 0}</td></tr>')
    body += '</tbody></table></div></div></details>'

    body += _task_add_form(ctx, members, goals)
    body += _board_columns(ctx, columns, content_cols, today)
    body += _board_script(ctx)
    body += page_footer()
    ctx.html(body)


def _task_add_form(ctx, members, goals) -> str:
    member_options = '<option value="">Niet toegewezen</option>' + ''.join(
        f'<option value="{m["id"]}"{" selected" if m["id"] == ctx.user_id else ""}>'
        f'{html.escape(m["username"])}</option>' for m in members)
    goal_options = '<option value="">Geen doel</option>' + ''.join(
        f'<option value="{g["id"]}">{html.escape(g["title"])}</option>' for g in goals)
    return f'''<div class="card" style="margin-bottom:1rem;">
        <div class="section-title">+ Nieuwe taak</div>
        <form method="POST" action="/comm/tasks/add"
              style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
            {ctx.csrf_input()}
            <div style="flex:2;min-width:160px;"><label class="form-label">Taak *</label>
                <input type="text" name="title" required class="form-control"
                       placeholder="Wat moet er gebeuren?"></div>
            <div style="flex:1;min-width:120px;"><label class="form-label">Toegewezen aan</label>
                <select name="assigned_to" class="form-select">{member_options}</select></div>
            <div style="flex:1;min-width:120px;"><label class="form-label">Doel</label>
                <select name="goal_id" class="form-select">{goal_options}</select></div>
            <div style="min-width:120px;"><label class="form-label">Deadline</label>
                <input type="date" name="due_date" class="form-control"></div>
            <div style="min-width:100px;"><label class="form-label">Prioriteit</label>
                <select name="priority" class="form-select">
                    <option value="laag">&#9660; Laag</option>
                    <option value="medium" selected>&#9654; Medium</option>
                    <option value="hoog">&#9650; Hoog</option></select></div>
            <div style="min-width:100px;"><label class="form-label">Kolom</label>
                <select name="status" class="form-select">
                    <option value="backlog">Backlog</option>
                    <option value="bezig">Bezig</option></select></div>
            <div style="min-width:110px;"><label class="form-label">Tags</label>
                <input type="text" name="tags" class="form-control" placeholder="social,pr"></div>
            <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
        </form></div>'''


def _task_card(ctx, task, today) -> str:
    overdue = task['due_date'] and task['due_date'] < today and task['status'] != 'klaar'
    is_today = task['due_date'] == today
    color = '#C0392B' if overdue else ('#B5916A' if is_today else '#555')
    border = ('border-left:3px solid #C0392B;' if overdue
              else ('border-left:3px solid #B5916A;' if is_today else ''))
    date_html = (f'<div style="font-size:0.75rem;color:{color};">'
                 f'<i data-lucide=calendar class=icon></i> {html.escape(str(task["due_date"]))}</div>'
                 ) if task['due_date'] else ''
    assigned = (f'<span style="font-size:0.75rem;color:#B0A49A;"><i data-lucide=user class=icon></i> '
                f'{html.escape(task["assigned_to_name"])}</span>') if task['assigned_to_name'] else ''
    goal = (f'<span style="font-size:0.7rem;background:#F2EEE9;color:#7A6E66;border-radius:3px;'
            f'padding:0.05rem 0.3rem;"><i data-lucide=target class=icon></i> '
            f'{html.escape(task["goal_title"])}</span>') if task['goal_title'] else ''
    tags = ''.join(f'<span style="font-size:0.7rem;background:#EDF3EC;color:#7A8FA6;'
                   f'border-radius:3px;padding:0.05rem 0.3rem;">{html.escape(t.strip())}</span>'
                   for t in (task['tags'] or '').split(',') if t.strip())
    reminder = (f'<div style="font-size:0.75rem;color:#5C7A5A;margin-top:0.2rem;">'
                f'<i data-lucide=bell class=icon></i> {html.escape(task["reminder_note"])}</div>'
                ) if task['reminder_note'] else ''
    description = task['description'] or ''
    desc = (f'<div style="font-size:0.78rem;color:#7A6E66;margin:0.2rem 0;">'
            f'{html.escape(description[:80])}{"…" if len(description) > 80 else ""}</div>'
            ) if description else ''

    moves = ''
    if task['status'] == 'backlog':
        moves = post_button('/comm/tasks/move', ctx, '→ Bezig', css='btn btn-sm btn-secondary',
                            fields={'id': task['id'], 'status': 'bezig'})
    elif task['status'] == 'bezig':
        moves = post_button('/comm/tasks/move', ctx, '← Back', css='btn btn-sm btn-secondary',
                            fields={'id': task['id'], 'status': 'backlog'}) + ' '
        moves += post_button('/comm/tasks/move', ctx,
                             '<i data-lucide=check class=icon></i> Klaar',
                             css='btn btn-sm btn-primary',
                             fields={'id': task['id'], 'status': 'klaar'})
    else:
        moves = post_button('/comm/tasks/move', ctx, '↩ Heropenen', css='btn btn-sm btn-secondary',
                            fields={'id': task['id'], 'status': 'bezig'})

    edit = (f'<a href="/comm/tasks/edit?id={task["id"]}" style="color:#7A8FA6;font-size:0.78rem;'
            f'margin-right:0.4rem;"><i data-lucide=pencil class=icon></i></a>')
    remove = post_button('/comm/tasks/delete', ctx, '<i data-lucide=x class=icon></i>',
                         confirm='Taak verwijderen?', css='btn-link',
                         style='color:#C0392B;font-size:0.78rem;', fields={'id': task['id']})

    return f'''<div class="comm-card" draggable="true" data-task-id="{task['id']}"
            data-status="{html.escape(task['status'])}"
            style="background:#fff;border-radius:6px;padding:0.6rem 0.7rem;margin-bottom:0.5rem;
                   box-shadow:0 1px 3px rgba(0,0,0,0.1);cursor:grab;{border}">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div style="font-weight:bold;font-size:0.88rem;flex:1;">{html.escape(task['title'])}</div>
            <div style="white-space:nowrap;margin-left:0.5rem;display:flex;align-items:center;">
                {edit}{remove}</div>
        </div>
        {desc}
        <div style="display:flex;gap:0.3rem;flex-wrap:wrap;margin:0.3rem 0;">
            {priority_badge(task['priority'])}{goal}{tags}{assigned}</div>
        {date_html}{reminder}
        <div style="margin-top:0.4rem;display:flex;gap:0.3rem;flex-wrap:wrap;">{moves}</div>
    </div>'''


def _content_card(ctx, item) -> str:
    icon, color, _bg = PLATFORM_CFG.get(item['platform'], ('map-pin', '#7A6E66', '#F2EEE9'))
    assigned = (f'<span style="font-size:0.72rem;color:#B0A49A;"><i data-lucide=user class=icon></i> '
                f'{html.escape(item["assigned_to_name"])}</span>') if item['assigned_to_name'] else ''
    date = (f'<span style="font-size:0.72rem;color:#B0A49A;">'
            f'<i data-lucide=calendar class=icon></i> {html.escape(str(item["publish_date"]))}</span>'
            ) if item['publish_date'] else ''
    options = ''.join(
        f'<option value="{value}"{" selected" if item["board_status"] == value else ""}>{label}</option>'
        for value, label in [('backlog', 'Backlog'), ('bezig', 'Bezig'),
                             ('klaar', 'Klaar'), ('', 'Verwijder uit board')])
    return f'''<div style="background:#fff;border-radius:6px;padding:0.5rem 0.65rem;
            margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.08);
            border-left:3px solid {color};opacity:0.92;">
        <div style="font-size:0.82rem;font-weight:bold;">
            <i data-lucide={icon} class=icon></i> {html.escape(item['title'])}</div>
        <div style="display:flex;gap:0.3rem;align-items:center;margin-top:0.25rem;flex-wrap:wrap;">
            {assigned}{date}
            <form method="POST" action="/comm/content/board-status" class="inline-form">
                {ctx.csrf_input()}
                <input type="hidden" name="id" value="{item['id']}">
                <select name="status" style="font-size:0.68rem;padding:0.1rem;"
                        onchange="this.form.submit()">{options}</select></form>
        </div></div>'''


def _board_columns(ctx, columns, content_cols, today) -> str:
    style = ('flex:1;min-width:240px;background:#EDE8E3;border-radius:8px;padding:0.75rem;'
             'transition:background 0.15s;')
    labels = {'backlog': ('clipboard-list', 'Backlog', '#888'),
              'bezig': ('zap', 'Bezig', '#7A8FA6'),
              'klaar': ('check', 'Klaar', '#5C7A5A')}
    out = '<div style="display:flex;gap:1rem;flex-wrap:wrap;align-items:flex-start;">'
    for status in config.COMM_TASK_STATUSES:
        icon, label, badge = labels[status]
        total = len(columns[status]) + len(content_cols[status])
        out += (f'<div class="comm-column" data-status="{status}" style="{style}">'
                f'<div style="font-weight:bold;margin-bottom:0.75rem;">'
                f'<i data-lucide={icon} class=icon></i> {label} '
                f'<span style="background:{badge};color:#fff;border-radius:10px;'
                f'padding:0.1rem 0.5rem;font-size:0.78rem;">{total}</span></div>')
        cards = (''.join(_task_card(ctx, t, today) for t in columns[status])
                 + ''.join(_content_card(ctx, c) for c in content_cols[status]))
        out += cards or ('<div class="comm-empty" style="color:#B0A49A;font-size:0.85rem;'
                         'padding:1rem 0;text-align:center;">Sleep hier naartoe</div>')
        if status == 'klaar' and columns['klaar']:
            out += ('<div style="margin-top:0.5rem;">' + post_button(
                '/comm/tasks/archive-done', ctx,
                f'<i data-lucide=archive class=icon></i> Archiveer alle ({len(columns["klaar"])})',
                confirm='Alle afgeronde taken archiveren?',
                css='btn btn-sm btn-secondary') + '</div>')
        out += '</div>'
    return out + '</div>'


def _board_script(ctx) -> str:
    """Drag & drop. Moves post a CSRF token instead of firing a bare GET."""
    return f'''<script>
(function() {{
    var dragging = null;
    var CSRF = {ctx.csrf_token!r};
    document.querySelectorAll('.comm-card').forEach(function(card) {{
        card.addEventListener('dragstart', function(e) {{
            dragging = card;
            setTimeout(function() {{ card.style.opacity = '0.4'; }}, 0);
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/plain', card.dataset.taskId);
        }});
        card.addEventListener('dragend', function() {{
            card.style.opacity = '1';
            dragging = null;
            document.querySelectorAll('.comm-column').forEach(function(col) {{
                col.style.background = '#EDE8E3'; col.style.outline = '';
            }});
        }});
    }});
    document.querySelectorAll('.comm-column').forEach(function(col) {{
        col.addEventListener('dragover', function(e) {{
            e.preventDefault();
            col.style.background = '#dceeff';
            col.style.outline = '2px dashed #7A8FA6';
        }});
        col.addEventListener('dragleave', function(e) {{
            if (!col.contains(e.relatedTarget)) {{
                col.style.background = '#EDE8E3'; col.style.outline = '';
            }}
        }});
        col.addEventListener('drop', function(e) {{
            e.preventDefault();
            col.style.background = '#EDE8E3'; col.style.outline = '';
            if (!dragging) return;
            var id = dragging.dataset.taskId;
            var newStatus = col.dataset.status;
            if (newStatus === dragging.dataset.status) return;
            col.appendChild(dragging);
            dragging.dataset.status = newStatus;
            var data = new URLSearchParams();
            data.append('id', id);
            data.append('status', newStatus);
            data.append('csrf_token', CSRF);
            fetch('/comm/tasks/move', {{
                method: 'POST', credentials: 'same-origin', body: data,
                headers: {{'Content-Type': 'application/x-www-form-urlencoded'}}
            }}).then(function() {{ location.reload(); }})
              .catch(function() {{ location.reload(); }});
        }});
    }});
}})();
</script>'''


# ── Comm tasks ────────────────────────────────────────────────────────────
def _task_fields(ctx):
    return {
        'title': ctx.f('title'),
        'description': ctx.f('description') or None,
        'due_date': ctx.f('due_date') or None,
        'assigned_to': ctx.fint('assigned_to'),
        'goal_id': ctx.fint('goal_id'),
        'status': ctx.choice(ctx.f('status', 'backlog'), config.COMM_TASK_STATUSES, 'backlog'),
        'priority': ctx.choice(ctx.f('priority', 'medium'), config.PRIORITIES, 'medium'),
        'tags': ctx.f('tags') or None,
        'reminder_note': ctx.f('reminder_note') or None,
    }


def task_add(ctx) -> None:
    values = _task_fields(ctx)
    if values['title']:
        with connect() as conn:
            cur = conn.execute(
                'INSERT INTO comm_tasks (title, description, status, due_date, assigned_to, '
                ' created_by, goal_id, priority, tags, reminder_note) '
                'VALUES (:title, :description, :status, :due_date, :assigned_to, :created_by, '
                ' :goal_id, :priority, :tags, :reminder_note)',
                {**values, 'created_by': ctx.user_id})
            log_action(ctx.user_id, 'create', 'comm_tasks', cur.lastrowid, values['title'])
    ctx.redirect('/comm/board')


def task_edit(ctx) -> None:
    task_id = ctx.qint('id')
    if not task_id:
        ctx.redirect('/comm/board')
        return
    if ctx.method == 'POST':
        values = _task_fields(ctx)
        if values['title']:
            with connect() as conn:
                conn.execute(
                    'UPDATE comm_tasks SET title=:title, description=:description, status=:status, '
                    ' due_date=:due_date, assigned_to=:assigned_to, goal_id=:goal_id, '
                    ' priority=:priority, tags=:tags, reminder_note=:reminder_note WHERE id=:id',
                    {**values, 'id': task_id})
            log_action(ctx.user_id, 'update', 'comm_tasks', task_id, values['title'])
        ctx.redirect('/comm/board')
        return

    task = query_one('SELECT * FROM comm_tasks WHERE id = ?', (task_id,))
    if not task:
        ctx.redirect('/comm/board')
        return
    members = comm_members()
    goals = query_all("SELECT id, title FROM comm_goals WHERE status='actief' ORDER BY title")

    member_options = '<option value="">Niet toegewezen</option>' + ''.join(
        f'<option value="{m["id"]}"{" selected" if task["assigned_to"] == m["id"] else ""}>'
        f'{html.escape(m["username"])}</option>' for m in members)
    goal_options = '<option value="">Geen doel</option>' + ''.join(
        f'<option value="{g["id"]}"{" selected" if task["goal_id"] == g["id"] else ""}>'
        f'{html.escape(g["title"])}</option>' for g in goals)
    status_options = ''.join(
        f'<option value="{v}"{" selected" if task["status"] == v else ""}>{l}</option>'
        for v, l in [('backlog', 'Backlog'), ('bezig', 'Bezig'), ('klaar', 'Klaar')])
    prio_options = ''.join(
        f'<option value="{v}"{" selected" if (task["priority"] or "medium") == v else ""}>{l}</option>'
        for v, l in [('hoog', '&#9650; Hoog'), ('medium', '&#9654; Medium'), ('laag', '&#9660; Laag')])

    body = page_header('Taak bewerken', ctx)
    body += '<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Taak bewerken</h2>'
    body += f'''<div class="card" style="max-width:640px;">
        <form method="POST" action="/comm/tasks/edit?id={task_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Taak *</label>
                <input type="text" name="title" value="{html.escape(task['title'])}"
                       required class="form-control"></div>
            <div class="mb-3"><label class="form-label">Omschrijving</label>
                <textarea name="description" class="form-control" rows="3">{html.escape(task['description'] or '')}</textarea></div>
            <div style="display:flex;gap:0.5rem;flex-wrap:wrap;">
                <div style="flex:1;min-width:120px;"><label class="form-label">Toegewezen aan</label>
                    <select name="assigned_to" class="form-select">{member_options}</select></div>
                <div style="flex:1;min-width:120px;"><label class="form-label">Doel</label>
                    <select name="goal_id" class="form-select">{goal_options}</select></div>
                <div style="flex:1;min-width:110px;"><label class="form-label">Status</label>
                    <select name="status" class="form-select">{status_options}</select></div>
                <div style="flex:1;min-width:110px;"><label class="form-label">Prioriteit</label>
                    <select name="priority" class="form-select">{prio_options}</select></div>
            </div>
            <div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-top:0.6rem;">
                <div style="flex:1;min-width:130px;"><label class="form-label">Deadline</label>
                    <input type="date" name="due_date" value="{html.escape(str(task['due_date'] or ''))}"
                           class="form-control"></div>
                <div style="flex:2;min-width:150px;"><label class="form-label">Tags</label>
                    <input type="text" name="tags" value="{html.escape(task['tags'] or '')}"
                           class="form-control" placeholder="social,pr,intern"></div>
            </div>
            <div class="mb-3 mt-3"><label class="form-label">Herinnering / notitie</label>
                <input type="text" name="reminder_note" value="{html.escape(task['reminder_note'] or '')}"
                       class="form-control" placeholder="Bijv. Wacht op goedkeuring van Jan"></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/comm/board" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def task_move(ctx) -> None:
    task_id = ctx.fint('id')
    status = ctx.choice(ctx.f('status'), config.COMM_TASK_STATUSES, 'backlog')
    if task_id:
        with connect() as conn:
            conn.execute('UPDATE comm_tasks SET status = ? WHERE id = ?', (status, task_id))
    ctx.redirect('/comm/board')


def task_delete(ctx) -> None:
    task_id = ctx.fint('id')
    if task_id:
        with connect() as conn:
            conn.execute('DELETE FROM comm_tasks WHERE id = ?', (task_id,))
        log_action(ctx.user_id, 'delete', 'comm_tasks', task_id)
    ctx.redirect('/comm/board')


def task_comment(ctx) -> None:
    task_id = ctx.fint('task_id')
    content = ctx.f('content')
    if task_id and content:
        with connect() as conn:
            conn.execute('INSERT INTO comm_task_comments (task_id, user_id, content) '
                         'VALUES (?, ?, ?)', (task_id, ctx.user_id, content))
    ctx.redirect('/comm/board')


def tasks_archive_done(ctx) -> None:
    with connect() as conn:
        conn.execute("UPDATE comm_tasks SET status = 'archief' WHERE status = 'klaar'")
    log_action(ctx.user_id, 'update', 'comm_tasks', None, 'afgeronde taken gearchiveerd')
    ctx.redirect('/comm/board')


# ── Goals ─────────────────────────────────────────────────────────────────
def goals(ctx) -> None:
    today = _today()
    with connect(readonly=True) as conn:
        rows = conn.execute('''
            SELECT g.id, g.title, g.description, g.target_date, g.status
              FROM comm_goals g
             ORDER BY g.status ASC, COALESCE(g.target_date,'9999-12-31') ASC''').fetchall()
        linked = conn.execute('''
            SELECT ct.id, ct.title, ct.status, ct.due_date, ct.goal_id, ct.priority,
                   u.username AS assigned_to_name
              FROM comm_tasks ct LEFT JOIN users u ON ct.assigned_to = u.id
             WHERE ct.goal_id IS NOT NULL
             ORDER BY COALESCE(ct.due_date,'9999-12-31') ASC''').fetchall()

    by_goal = defaultdict(list)
    for task in linked:
        by_goal[task['goal_id']].append(task)

    body = page_header('Communicatie Doelen', ctx)
    body += '<h2 class="mt-4"><i data-lucide=target class=icon></i> Doelen</h2>'
    body += comm_nav('goals', ctx)

    active = [g for g in rows if g['status'] == 'actief']
    achieved = [g for g in rows if g['status'] == 'behaald']
    body += ('<div class="stat-row">' + stat_card(len(active), 'Actief')
             + stat_card(len(achieved), 'Behaald') + '</div>')

    body += f'''<div class="card" style="margin-bottom:1rem;">
        <div class="section-title">+ Nieuw doel</div>
        <form method="POST" action="/comm/goals/add"
              style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
            {ctx.csrf_input()}
            <div style="flex:2;min-width:160px;"><label class="form-label">Doel *</label>
                <input type="text" name="title" required class="form-control"
                       placeholder="Bijv. Q2 campagne"></div>
            <div style="flex:2;min-width:180px;"><label class="form-label">Omschrijving</label>
                <input type="text" name="description" class="form-control"></div>
            <div style="min-width:130px;"><label class="form-label">Streefdatum</label>
                <input type="date" name="target_date" class="form-control"></div>
            <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
        </form></div>'''

    if not rows:
        body += '<div class="card"><p style="color:#B0A49A;">Nog geen doelen.</p></div>'

    for goal in rows:
        done = goal['status'] == 'behaald'
        overdue = goal['target_date'] and goal['target_date'] < today and not done
        color = '#C0392B' if overdue else ('#5C7A5A' if done else '#555')
        date = (f' <i data-lucide=calendar class=icon></i> <span style="color:{color};">'
                f'{html.escape(str(goal["target_date"]))}</span>') if goal['target_date'] else ''
        badge = ('<span class="badge badge-ok">Behaald</span>' if done
                 else '<span class="badge badge-muted">Actief</span>')
        desc = (f'<div style="font-size:0.85rem;color:#7A6E66;margin:0.25rem 0;">'
                f'{html.escape(goal["description"])}</div>') if goal['description'] else ''

        if done:
            action = post_button('/comm/goals/reopen', ctx, '↩ Heropenen',
                                 css='btn btn-sm btn-secondary', fields={'id': goal['id']})
        else:
            action = post_button('/comm/goals/complete', ctx,
                                 '<i data-lucide=check class=icon></i> Behaald',
                                 confirm='Doel behaald markeren?', css='btn btn-sm btn-primary',
                                 fields={'id': goal['id']})
        action += (f' <a href="/comm/goals/edit?id={goal["id"]}" class="btn btn-sm btn-secondary">'
                   f'<i data-lucide=pencil class=icon></i></a> ')
        action += post_button('/comm/goals/delete', ctx, 'Verwijder',
                              confirm='Doel verwijderen?', fields={'id': goal['id']})

        tasks_for_goal = by_goal.get(goal['id'], [])
        finished = sum(1 for t in tasks_for_goal if t['status'] == 'klaar')
        pct = int(finished / len(tasks_for_goal) * 100) if tasks_for_goal else 0
        progress = ''
        if tasks_for_goal:
            progress = (f'<div style="margin:0.4rem 0;"><div style="font-size:0.75rem;'
                        f'color:#7A6E66;margin-bottom:0.2rem;">Deliverables: {finished}/'
                        f'{len(tasks_for_goal)} ({pct}%)</div>'
                        f'<div style="background:#EDE8E3;border-radius:4px;height:7px;">'
                        f'<div style="background:#7A6E66;border-radius:4px;height:7px;'
                        f'width:{pct}%;"></div></div></div>')

        body += (f'<div class="card" style="opacity:{"0.7" if done else "1"};">'
                 f'<div style="display:flex;justify-content:space-between;align-items:flex-start;'
                 f'flex-wrap:wrap;gap:0.5rem;"><div style="flex:1;">'
                 f'<strong><i data-lucide=target class=icon></i> {html.escape(goal["title"])}</strong> '
                 f'{badge}{date}{desc}{progress}</div>'
                 f'<div style="display:flex;gap:0.3rem;align-items:center;">{action}</div></div>')
        if tasks_for_goal:
            body += ('<div style="margin-top:0.6rem;border-top:1px solid #EDE8E3;padding-top:0.4rem;">'
                     '<div style="font-size:0.78rem;font-weight:bold;color:#7A6E66;">Deliverables</div>')
            for task in tasks_for_goal:
                complete = task['status'] == 'klaar'
                late = task['due_date'] and task['due_date'] < today and not complete
                icon = ('check' if complete else ('zap' if task['status'] == 'bezig' else 'circle'))
                icon_color = '#5C7A5A' if complete else ('#7A8FA6' if task['status'] == 'bezig' else '#888')
                due = (f' <small style="color:{"#C0392B" if late else "#888"};">'
                       f'<i data-lucide=calendar class=icon></i> '
                       f'{html.escape(str(task["due_date"]))}</small>') if task['due_date'] else ''
                who = (f' <small style="color:#B0A49A;"><i data-lucide=user class=icon></i> '
                       f'{html.escape(task["assigned_to_name"])}</small>'
                       ) if task['assigned_to_name'] else ''
                body += (f'<div style="padding:0.2rem 0;font-size:0.83rem;display:flex;gap:0.4rem;'
                         f'align-items:center;"><span style="color:{icon_color};">'
                         f'<i data-lucide={icon} class=icon></i></span> '
                         f'<span>{html.escape(task["title"])}</span>{who}{due} '
                         f'{priority_badge(task["priority"])}</div>')
            body += '</div>'
        elif not done:
            body += ('<div style="margin-top:0.4rem;font-size:0.78rem;color:#B0A49A;">'
                     'Nog geen deliverables — koppel taken via het '
                     '<a href="/comm/board">board</a>.</div>')
        body += '</div>'

    body += page_footer()
    ctx.html(body)


def goal_add(ctx) -> None:
    title = ctx.f('title')
    if title:
        with connect() as conn:
            cur = conn.execute(
                'INSERT INTO comm_goals (title, description, target_date, created_by) '
                'VALUES (?, ?, ?, ?)',
                (title, ctx.f('description') or None, ctx.f('target_date') or None, ctx.user_id))
            log_action(ctx.user_id, 'create', 'comm_goals', cur.lastrowid, title)
    ctx.redirect('/comm/goals')


def goal_edit(ctx) -> None:
    goal_id = ctx.qint('id')
    if not goal_id:
        ctx.redirect('/comm/goals')
        return
    if ctx.method == 'POST':
        title = ctx.f('title')
        if title:
            with connect() as conn:
                conn.execute('UPDATE comm_goals SET title=?, description=?, target_date=? WHERE id=?',
                             (title, ctx.f('description') or None,
                              ctx.f('target_date') or None, goal_id))
        ctx.redirect('/comm/goals')
        return

    goal = query_one('SELECT * FROM comm_goals WHERE id = ?', (goal_id,))
    if not goal:
        ctx.redirect('/comm/goals')
        return
    body = page_header('Doel bewerken', ctx)
    body += '<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Doel bewerken</h2>'
    body += f'''<div class="card" style="max-width:540px;">
        <form method="POST" action="/comm/goals/edit?id={goal_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Doel *</label>
                <input type="text" name="title" value="{html.escape(goal['title'])}"
                       required class="form-control"></div>
            <div class="mb-3"><label class="form-label">Omschrijving</label>
                <textarea name="description" class="form-control" rows="3">{html.escape(goal['description'] or '')}</textarea></div>
            <div class="mb-3"><label class="form-label">Streefdatum</label>
                <input type="date" name="target_date" class="form-control"
                       value="{html.escape(str(goal['target_date'] or ''))}"></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/comm/goals" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def _set_goal_status(ctx, status: str) -> None:
    goal_id = ctx.fint('id')
    if goal_id:
        with connect() as conn:
            conn.execute('UPDATE comm_goals SET status = ? WHERE id = ?', (status, goal_id))
    ctx.redirect('/comm/goals')


def goal_complete(ctx) -> None:
    _set_goal_status(ctx, 'behaald')


def goal_reopen(ctx) -> None:
    _set_goal_status(ctx, 'actief')


def goal_delete(ctx) -> None:
    goal_id = ctx.fint('id')
    if goal_id:
        with connect() as conn:
            conn.execute('DELETE FROM comm_goals WHERE id = ?', (goal_id,))
        log_action(ctx.user_id, 'delete', 'comm_goals', goal_id)
    ctx.redirect('/comm/goals')


# ── Week ──────────────────────────────────────────────────────────────────
def week(ctx) -> None:
    today = datetime.date.today()
    today_iso = today.isoformat()
    horizon = (today + datetime.timedelta(days=7)).isoformat()
    with connect(readonly=True) as conn:
        this_week = conn.execute('''
            SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority,
                   u.username AS assigned_to_name, cg.title AS goal_title
              FROM comm_tasks ct
              LEFT JOIN users u       ON ct.assigned_to = u.id
              LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
             WHERE ct.status NOT IN ('klaar','archief') AND ct.due_date IS NOT NULL
               AND ct.due_date <= ?
             ORDER BY ct.due_date ASC,
                      CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END''',
            (horizon,)).fetchall()
        high_later = conn.execute('''
            SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority,
                   u.username AS assigned_to_name, cg.title AS goal_title
              FROM comm_tasks ct
              LEFT JOIN users u       ON ct.assigned_to = u.id
              LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
             WHERE ct.status NOT IN ('klaar','archief')
               AND (ct.due_date IS NULL OR ct.due_date > ?) AND ct.priority = 'hoog'
             ORDER BY COALESCE(ct.due_date,'9999-12-31') ASC''', (horizon,)).fetchall()

    tomorrow = (today + datetime.timedelta(days=1)).isoformat()
    groups = [
        ('<i data-lucide=alert-triangle class=icon></i> Verlopen', '#C0392B',
         [t for t in this_week if t['due_date'] < today_iso]),
        ('<i data-lucide=calendar class=icon></i> Vandaag', '#B5916A',
         [t for t in this_week if t['due_date'] == today_iso]),
        ('<i data-lucide=sun class=icon></i> Morgen', '#7A8FA6',
         [t for t in this_week if t['due_date'] == tomorrow]),
        ('<i data-lucide=clock class=icon></i> Deze week', '#5C7A5A',
         [t for t in this_week if t['due_date'] > tomorrow]),
        ('<i data-lucide=chevron-up class=icon></i> Hoge prioriteit (later)', '#5C7A5A', high_later),
    ]

    body = page_header('Week Overzicht', ctx)
    body += (f'<h2 class="mt-4"><i data-lucide=calendar class=icon></i> Week overzicht — '
             f'{today.strftime("%d-%m-%Y")}</h2>')
    body += comm_nav('week', ctx)

    any_items = False
    for label, color, items in groups:
        if not items:
            continue
        any_items = True
        body += (f'<div class="card" style="border-left:4px solid {color};">'
                 f'<div style="font-weight:bold;margin-bottom:0.5rem;">{label} ({len(items)})</div>')
        for task in items:
            who = (f'<span style="color:#B0A49A;font-size:0.78rem;">'
                   f'<i data-lucide=user class=icon></i> '
                   f'{html.escape(task["assigned_to_name"])}</span>'
                   ) if task['assigned_to_name'] else ''
            goal = (f'<span style="font-size:0.72rem;background:#F2EEE9;color:#7A6E66;'
                    f'border-radius:3px;padding:0.05rem 0.3rem;">'
                    f'<i data-lucide=target class=icon></i> {html.escape(task["goal_title"])}</span>'
                    ) if task['goal_title'] else ''
            body += (f'<div style="padding:0.45rem 0;border-bottom:1px solid #EDE8E3;display:flex;'
                     f'gap:0.5rem;align-items:center;flex-wrap:wrap;">'
                     f'<span style="min-width:80px;font-size:0.78rem;color:#B0A49A;">'
                     f'{html.escape(str(task["due_date"] or ""))}</span>'
                     f'<span style="flex:1;font-weight:bold;font-size:0.88rem;">'
                     f'{html.escape(task["title"])}</span>'
                     f'{priority_badge(task["priority"])}{goal}{who}</div>')
        body += '</div>'
    if not any_items:
        body += '<div class="card"><p style="color:#B0A49A;">Geen taken deze week.</p></div>'
    body += page_footer()
    ctx.html(body)


# ── Overview ──────────────────────────────────────────────────────────────
def overview(ctx) -> None:
    today = datetime.date.today()
    today_iso = today.isoformat()
    horizon = (today + datetime.timedelta(days=30)).isoformat()
    with connect(readonly=True) as conn:
        upcoming_tasks = conn.execute('''
            SELECT ct.id, ct.title, ct.due_date, ct.priority, u.username AS assigned_to_name
              FROM comm_tasks ct LEFT JOIN users u ON ct.assigned_to = u.id
             WHERE ct.status NOT IN ('klaar','archief') AND ct.due_date IS NOT NULL
               AND ct.due_date <= ? ORDER BY ct.due_date ASC''', (horizon,)).fetchall()
        upcoming_dates = conn.execute(
            'SELECT id, title, date AS event_date, type AS event_type FROM comm_dates '
            'WHERE date >= ? AND date <= ? ORDER BY date ASC', (today_iso, horizon)).fetchall()
        upcoming_content = conn.execute('''
            SELECT cc.id, cc.title, cc.platform, cc.publish_date, u.username AS assigned_to_name
              FROM comm_content cc LEFT JOIN users u ON cc.assigned_to = u.id
             WHERE cc.publish_date IS NOT NULL AND cc.publish_date >= ?
               AND cc.publish_date <= ? AND cc.status != 'gepubliceerd'
             ORDER BY cc.publish_date ASC''', (today_iso, horizon)).fetchall()

    items = []
    for task in upcoming_tasks:
        items.append((task['due_date'], 'task', task, task['due_date'] < today_iso))
    for date_row in upcoming_dates:
        items.append((date_row['event_date'], 'date', date_row, False))
    for item in upcoming_content:
        items.append((item['publish_date'], 'content', item, item['publish_date'] < today_iso))
    items.sort(key=lambda entry: entry[0])

    body = page_header('Overzicht', ctx)
    body += ('<h2 class="mt-4"><i data-lucide=clipboard-list class=icon></i> '
             'Overzicht — komende 30 dagen</h2>')
    body += comm_nav('overview', ctx)

    if not items:
        body += '<div class="card"><p style="color:#B0A49A;">Geen aankomende items.</p></div>'
    else:
        body += '<div class="card" style="padding:0;">'
        previous = None
        for date_str, kind, item, overdue in items:
            if date_str != previous:
                delta = (datetime.date.fromisoformat(date_str) - today).days
                if delta < 0:
                    label, color = f'{abs(delta)} dag(en) geleden', '#C0392B'
                elif delta == 0:
                    label, color = 'Vandaag', '#B5916A'
                elif delta == 1:
                    label, color = 'Morgen', '#7A8FA6'
                else:
                    label, color = f'Over {delta} dagen', '#555'
                body += (f'<div style="background:#F7F4F0;padding:0.4rem 0.9rem;font-size:0.78rem;'
                         f'font-weight:bold;color:{color};border-bottom:1px solid #EDE8E3;">'
                         f'<i data-lucide=calendar class=icon></i> {html.escape(date_str)} — '
                         f'{label}</div>')
                previous = date_str
            style = 'color:#C0392B;' if overdue else ''
            if kind == 'task':
                who = (f'<span style="font-size:0.72rem;color:#B0A49A;">'
                       f'{html.escape(item["assigned_to_name"])}</span>'
                       ) if item['assigned_to_name'] else ''
                tag = ('<span style="font-size:0.75rem;background:#FEF8F0;color:#B5916A;'
                       'border-radius:3px;padding:0.05rem 0.3rem;">Taak</span>')
                link = '/comm/board'
                extra = priority_badge(item['priority']) + ' ' + who
            elif kind == 'date':
                icon, _c, _b = DATE_CFG.get(item['event_type'], ('calendar', '#555', '#eee'))
                tag = (f'<span style="font-size:0.75rem;background:#EDF3EC;color:#5C7A5A;'
                       f'border-radius:3px;padding:0.05rem 0.3rem;">'
                       f'<i data-lucide={icon} class=icon></i> Datum</span>')
                link, extra = '/comm/dates', ''
            else:
                icon, _c, _b = PLATFORM_CFG.get(item['platform'], ('map-pin', '#7A6E66', '#eee'))
                tag = (f'<span style="font-size:0.75rem;background:#EDF3EC;color:#7A8FA6;'
                       f'border-radius:3px;padding:0.05rem 0.3rem;">'
                       f'<i data-lucide={icon} class=icon></i> Content</span>')
                link = '/comm/content'
                extra = (f'<span style="font-size:0.72rem;color:#B0A49A;">'
                         f'{html.escape(item["assigned_to_name"])}</span>'
                         ) if item['assigned_to_name'] else ''
            body += (f'<div style="padding:0.45rem 0.9rem;border-bottom:1px solid #EDE8E3;'
                     f'display:flex;gap:0.4rem;align-items:center;flex-wrap:wrap;">{tag} '
                     f'<a href="{link}" style="flex:1;font-size:0.86rem;font-weight:bold;'
                     f'{style}color:inherit;">{html.escape(item["title"])}</a> {extra}</div>')
        body += '</div>'

    overdue_count = sum(1 for _d, _k, _i, ov in items if ov)
    today_count = sum(1 for d, _k, _i, _ov in items if d == today_iso)
    body += '<div class="stat-row mt-3">'
    body += stat_card(overdue_count, 'Verlopen', '#C0392B')
    body += stat_card(today_count, 'Vandaag', '#B5916A')
    body += stat_card(len(items), 'Totaal (30d)', '#5C7A5A')
    body += '</div>'
    body += page_footer()
    ctx.html(body)


# ── Archived ──────────────────────────────────────────────────────────────
def archived(ctx) -> None:
    rows = query_all('''
        SELECT ct.id, ct.title, ct.due_date, ct.priority, ct.tags,
               u.username AS assigned_to_name, cg.title AS goal_title
          FROM comm_tasks ct
          LEFT JOIN users u       ON ct.assigned_to = u.id
          LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
         WHERE ct.status = 'archief' ORDER BY ct.created_at DESC''')
    body = page_header('Archief', ctx)
    body += '<h2 class="mt-4"><i data-lucide=archive class=icon></i> Archief — afgeronde taken</h2>'
    body += comm_nav('archived', ctx)
    body += f'<div class="card"><div class="section-title">{len(rows)} gearchiveerde taken</div>'
    if rows:
        body += ('<div class="table-wrap"><table><thead><tr><th>Taak</th><th>Toegewezen aan</th>'
                 '<th>Doel</th><th>Prioriteit</th><th>Tags</th></tr></thead><tbody>')
        for task in rows:
            body += (f'<tr><td>{html.escape(task["title"])}</td>'
                     f'<td>{html.escape(task["assigned_to_name"] or "-")}</td>'
                     f'<td>{html.escape(task["goal_title"] or "-")}</td>'
                     f'<td>{priority_badge(task["priority"])}</td>'
                     f'<td>{html.escape(task["tags"] or "-")}</td></tr>')
        body += '</tbody></table></div>'
    else:
        body += '<p style="color:#B0A49A;">Nog niets gearchiveerd.</p>'
    body += '</div>'
    body += page_footer()
    ctx.html(body)


# ── Search ────────────────────────────────────────────────────────────────
def search(ctx) -> None:
    term = ctx.q('q')
    filter_uid = ctx.qint('uid')
    status = ctx.q('status')
    priority = ctx.q('priority')

    conditions = ["ct.status != 'archief'"]
    args = []
    if term:
        conditions.append('(ct.title LIKE ? OR ct.description LIKE ? OR ct.tags LIKE ?)')
        args.extend([f'%{term}%'] * 3)
    if filter_uid:
        conditions.append('ct.assigned_to = ?')
        args.append(filter_uid)
    if status in config.COMM_TASK_STATUSES:
        conditions.append('ct.status = ?')
        args.append(status)
    if priority in config.PRIORITIES:
        conditions.append('ct.priority = ?')
        args.append(priority)

    rows = query_all(f'''
        SELECT ct.*, u.username AS assigned_to_name, cg.title AS goal_title
          FROM comm_tasks ct
          LEFT JOIN users u       ON ct.assigned_to = u.id
          LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
         WHERE {' AND '.join(conditions)}
         ORDER BY CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                  COALESCE(ct.due_date,'9999-12-31') ASC''', tuple(args))
    members = comm_members()
    today = _today()

    body = page_header('Zoeken', ctx)
    body += '<h2 class="mt-4"><i data-lucide=search class=icon></i> Taken zoeken</h2>'
    body += comm_nav('search', ctx)

    member_options = '<option value="">Alle teamleden</option>' + ''.join(
        f'<option value="{m["id"]}"{" selected" if filter_uid == m["id"] else ""}>'
        f'{html.escape(m["username"])}</option>' for m in members)
    status_options = ''.join(
        f'<option value="{v}"{" selected" if status == v else ""}>{l}</option>'
        for v, l in [('', 'Alle statussen'), ('backlog', 'Backlog'),
                     ('bezig', 'Bezig'), ('klaar', 'Klaar')])
    prio_options = ''.join(
        f'<option value="{v}"{" selected" if priority == v else ""}>{l}</option>'
        for v, l in [('', 'Alle prioriteiten'), ('hoog', 'Hoog'),
                     ('medium', 'Medium'), ('laag', 'Laag')])

    body += f'''<div class="card" style="margin-bottom:1rem;">
        <form method="GET" action="/comm/search"
              style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
            <div style="flex:2;min-width:160px;"><label class="form-label">Zoekterm</label>
                <input type="search" name="q" value="{html.escape(term)}" class="form-control"
                       placeholder="Zoek in titel, omschrijving, tags..."></div>
            <div><label class="form-label">Teamlid</label>
                <select name="uid" class="form-select">{member_options}</select></div>
            <div><label class="form-label">Status</label>
                <select name="status" class="form-select">{status_options}</select></div>
            <div><label class="form-label">Prioriteit</label>
                <select name="priority" class="form-select">{prio_options}</select></div>
            <div><button type="submit" class="btn btn-primary">Zoeken</button>
                <a href="/comm/search" class="btn btn-link">Wis</a></div>
        </form></div>'''

    body += f'<div class="card"><div class="section-title">Resultaten ({len(rows)})</div>'
    if rows:
        body += ('<div class="table-wrap"><table><thead><tr><th>Taak</th><th>Toegewezen aan</th>'
                 '<th>Doel</th><th>Prioriteit</th><th>Status</th><th>Deadline</th>'
                 '</tr></thead><tbody>')
        labels = {'backlog': 'Backlog', 'bezig': 'Bezig', 'klaar': 'Klaar'}
        for task in rows:
            late = (task['due_date'] and task['due_date'] < today
                    and task['status'] not in ('klaar', 'archief'))
            tags = (f'<br><small style="color:#7A8FA6;">{html.escape(task["tags"])}</small>'
                    if task['tags'] else '')
            body += (f'<tr><td><strong>{html.escape(task["title"])}</strong>{tags}</td>'
                     f'<td>{html.escape(task["assigned_to_name"] or "-")}</td>'
                     f'<td>{html.escape(task["goal_title"] or "-")}</td>'
                     f'<td>{priority_badge(task["priority"])}</td>'
                     f'<td>{labels.get(task["status"], task["status"])}</td>'
                     f'<td style="color:{"#C0392B" if late else "#555"};">'
                     f'{html.escape(str(task["due_date"] or "-"))}</td></tr>')
        body += '</tbody></table></div>'
    else:
        body += '<p style="color:#B0A49A;">Geen resultaten.</p>'
    body += '</div>'
    body += page_footer()
    ctx.html(body)


# ── Profile ───────────────────────────────────────────────────────────────
def profile(ctx) -> None:
    profile_id = ctx.qint('id') or ctx.user_id
    if profile_id != ctx.user_id and not ctx.is_admin:
        ctx.redirect(f'/comm/profile?id={ctx.user_id}')
        return
    user = query_one('SELECT id, username, email, created_at FROM users WHERE id = ?', (profile_id,))
    if not user:
        ctx.redirect('/comm/board')
        return

    today = _today()
    with connect(readonly=True) as conn:
        open_tasks = conn.execute('''
            SELECT ct.id, ct.title, ct.status, ct.due_date, ct.priority, ct.reminder_note,
                   cg.title AS goal_title
              FROM comm_tasks ct LEFT JOIN comm_goals cg ON ct.goal_id = cg.id
             WHERE ct.assigned_to = ? AND ct.status NOT IN ('klaar','archief')
             ORDER BY CASE ct.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                      COALESCE(ct.due_date,'9999-12-31') ASC''', (profile_id,)).fetchall()
        done = conn.execute("SELECT title FROM comm_tasks WHERE assigned_to = ? "
                            "AND status = 'klaar' ORDER BY created_at DESC LIMIT 10",
                            (profile_id,)).fetchall()
        goals_mine = conn.execute('''
            SELECT id, title, target_date, status FROM comm_goals WHERE created_by = ?
             ORDER BY status ASC, COALESCE(target_date,'9999-12-31') ASC''',
            (profile_id,)).fetchall()
        ext = conn.execute('SELECT * FROM comm_profiles WHERE user_id = ?', (profile_id,)).fetchone()
        gov_tasks = conn.execute('''
            SELECT id, title, status, due_date, priority, event_context
              FROM events_gov_tasks WHERE assigned_to = ? AND status != 'klaar'
             ORDER BY CASE priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                      COALESCE(due_date,'9999-12-31') ASC''', (profile_id,)).fetchall()

    avatar = (ext['avatar_color'] if ext and ext['avatar_color'] else '#5C7A5A')
    role_title = (ext['role_title'] if ext and ext['role_title'] else '')
    bio = (ext['bio'] if ext and ext['bio'] else '')
    skills = (ext['skills'] if ext and ext['skills'] else '')

    overdue = sum(1 for t in open_tasks if t['due_date'] and t['due_date'] < today)
    soon_limit = (datetime.date.today() + datetime.timedelta(days=2)).isoformat()
    soon = [t for t in open_tasks if t['due_date'] and today < t['due_date'] <= soon_limit]
    due_today = [t for t in open_tasks if t['due_date'] == today]

    body = page_header(f'Profiel: {user["username"]}', ctx)
    body += f'<h2 class="mt-4"><i data-lucide=user class=icon></i> {html.escape(user["username"])}</h2>'
    body += comm_nav('profile', ctx)

    edit_link = (f'<a href="/comm/profile/edit?id={profile_id}" class="btn btn-sm btn-secondary" '
                 f'style="margin-top:0.5rem;"><i data-lucide=pencil class=icon></i> '
                 f'Profiel bewerken</a>') if (profile_id == ctx.user_id or ctx.is_admin) else ''
    skills_html = ''
    if skills:
        skills_html = ('<div style="margin-top:0.5rem;display:flex;gap:0.3rem;flex-wrap:wrap;">'
                       + ''.join(f'<span style="background:#EDF3EC;color:#5C7A5A;'
                                 f'border-radius:12px;padding:0.15rem 0.6rem;font-size:0.78rem;">'
                                 f'{html.escape(s.strip())}</span>'
                                 for s in skills.split(',') if s.strip()) + '</div>')
    body += f'''<div class="card" style="display:flex;gap:1rem;align-items:flex-start;flex-wrap:wrap;">
        <div style="width:60px;height:60px;border-radius:50%;background:{avatar};color:#fff;
                    display:flex;align-items:center;justify-content:center;font-size:1.7rem;
                    font-weight:bold;flex-shrink:0;">{html.escape(user["username"][0].upper())}</div>
        <div style="flex:1;">
            <div style="font-size:1.1rem;font-weight:bold;">{html.escape(user["username"])}</div>
            {f'<div style="font-size:0.88rem;color:#5C7A5A;font-weight:bold;">{html.escape(role_title)}</div>' if role_title else ''}
            <div style="color:#B0A49A;font-size:0.82rem;">{html.escape(user["email"])} &middot;
                Lid sinds {html.escape(str(user["created_at"] or "")[:10])}</div>
            {f'<div style="font-size:0.85rem;color:#7A6E66;margin-top:0.35rem;font-style:italic;">{html.escape(bio)}</div>' if bio else ''}
            {skills_html}{edit_link}
        </div></div>'''

    body += '<div class="stat-row">'
    body += stat_card(len(open_tasks), 'Open taken', '#B5916A' if open_tasks else '#1C1713')
    body += stat_card(overdue, 'Verlopen', '#C0392B' if overdue else '#1C1713')
    body += stat_card(len(soon) + len(due_today), 'Bijna deadline')
    body += stat_card(len(done), 'Afgerond')
    body += stat_card(len(goals_mine), 'Doelen')
    body += '</div>'

    def task_row(task):
        late = task['due_date'] and task['due_date'] < today
        due = (f'<small style="color:{"#C0392B" if late else "#555"};">'
               f'<i data-lucide=calendar class=icon></i> '
               f'{html.escape(str(task["due_date"]))}</small>') if task['due_date'] else ''
        goal = (f'<span style="font-size:0.7rem;background:#F2EEE9;color:#7A6E66;'
                f'border-radius:3px;padding:0.05rem 0.3rem;">'
                f'{html.escape(task["goal_title"])}</span>') if task['goal_title'] else ''
        return (f'<div style="padding:0.35rem 0;border-bottom:1px solid #EDE8E3;display:flex;'
                f'gap:0.4rem;align-items:center;flex-wrap:wrap;">'
                f'<span style="flex:1;font-size:0.86rem;">{html.escape(task["title"])}</span>'
                f'{priority_badge(task["priority"])}{goal}{due}</div>')

    body += '<div class="card"><div class="section-title">Open taken</div>'
    body += (''.join(task_row(t) for t in open_tasks) if open_tasks
             else '<p style="color:#B0A49A;">Geen open taken.</p>')
    body += '</div>'

    body += '<div class="card"><div class="section-title">Mijn doelen</div>'
    if goals_mine:
        for goal in goals_mine:
            complete = goal['status'] == 'behaald'
            icon = 'check' if complete else 'circle'
            color = '#5C7A5A' if complete else '#7A6E66'
            date = (f' <small style="color:#B0A49A;">{html.escape(str(goal["target_date"]))}</small>'
                    ) if goal['target_date'] else ''
            body += (f'<div style="padding:0.35rem 0;border-bottom:1px solid #EDE8E3;'
                     f'font-size:0.86rem;"><span style="color:{color};">'
                     f'<i data-lucide={icon} class=icon></i></span> '
                     f'<strong>{html.escape(goal["title"])}</strong>{date}</div>')
    else:
        body += '<p style="color:#B0A49A;">Geen doelen gevonden.</p>'
    body += '</div>'

    if gov_tasks:
        body += ('<div class="card" style="border-left:4px solid #7A6E66;">'
                 '<div class="section-title"><i data-lucide=flag class=icon></i> '
                 'Events Gov — mijn checks</div>')
        labels = {'open': ('Open', '#B5916A'), 'in_check': ('In check', '#7A8FA6'),
                  'klaar': ('Klaar', '#5C7A5A')}
        for task in gov_tasks:
            label, color = labels.get(task['status'], ('?', '#888'))
            context = (f' <span style="font-size:0.75rem;color:#7A6E66;">'
                       f'{html.escape(task["event_context"])}</span>') if task['event_context'] else ''
            body += (f'<div style="padding:0.35rem 0;border-bottom:1px solid #EDE8E3;'
                     f'font-size:0.85rem;"><span style="font-size:0.72rem;background:{color};'
                     f'color:#fff;border-radius:3px;padding:0.1rem 0.3rem;">{label}</span> '
                     f'<strong>{html.escape(task["title"])}</strong>{context}</div>')
        body += '<div class="mt-2"><a href="/comm/events-gov">→ Naar Events Gov board</a></div></div>'

    body += page_footer()
    ctx.html(body)


def profile_edit(ctx) -> None:
    target_id = ctx.qint('id') or ctx.user_id
    if target_id != ctx.user_id and not ctx.is_admin:
        ctx.redirect(f'/comm/profile?id={ctx.user_id}')
        return

    if ctx.method == 'POST':
        color = ctx.f('avatar_color', '#5C7A5A')
        if not (color.startswith('#') and len(color) in (4, 7)):
            color = '#5C7A5A'
        with connect() as conn:
            conn.execute('''
                INSERT INTO comm_profiles (user_id, role_title, bio, skills, avatar_color, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    role_title = excluded.role_title, bio = excluded.bio,
                    skills = excluded.skills, avatar_color = excluded.avatar_color,
                    updated_at = CURRENT_TIMESTAMP''',
                (target_id, ctx.f('role_title') or None, ctx.f('bio') or None,
                 ctx.f('skills') or None, color))
        ctx.redirect(f'/comm/profile?id={target_id}')
        return

    user = query_one('SELECT id, username FROM users WHERE id = ?', (target_id,))
    if not user:
        ctx.redirect('/comm/board')
        return
    profile_row = query_one('SELECT * FROM comm_profiles WHERE user_id = ?', (target_id,))
    role_title = (profile_row['role_title'] if profile_row else '') or ''
    bio = (profile_row['bio'] if profile_row else '') or ''
    skills = (profile_row['skills'] if profile_row else '') or ''
    current = (profile_row['avatar_color'] if profile_row else '') or '#5C7A5A'

    palette = ['#5C7A5A', '#7A6E66', '#7A8FA6', '#B5916A', '#C0392B', '#3d5c3b']
    swatches = ''.join(
        f'<label style="cursor:pointer;"><input type="radio" name="avatar_color" value="{c}"'
        f'{" checked" if current == c else ""} style="display:none;">'
        f'<span style="display:inline-block;width:30px;height:30px;border-radius:50%;'
        f'background:{c};border:3px solid {"#333" if current == c else "transparent"};'
        f'margin:2px;"></span></label>' for c in palette)

    body = page_header('Profiel bewerken', ctx)
    body += (f'<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Profiel bewerken — '
             f'{html.escape(user["username"])}</h2>')
    body += f'''<div class="card" style="max-width:560px;">
        <form method="POST" action="/comm/profile/edit?id={target_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Functietitel</label>
                <input type="text" name="role_title" value="{html.escape(role_title)}"
                       class="form-control" placeholder="Bijv. Social Media Manager"></div>
            <div class="mb-3"><label class="form-label">Bio / Over mij</label>
                <textarea name="bio" class="form-control" rows="3">{html.escape(bio)}</textarea></div>
            <div class="mb-3"><label class="form-label">Skills &amp; vaardigheden</label>
                <input type="text" name="skills" value="{html.escape(skills)}" class="form-control"
                       placeholder="Copywriting, Canva, SEO">
                <small style="color:#B0A49A;">Komma-gescheiden.</small></div>
            <div class="mb-3"><label class="form-label">Profielkleur</label>
                <div style="margin-top:0.3rem;">{swatches}</div></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/comm/profile?id={target_id}" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


# ── Dates ─────────────────────────────────────────────────────────────────
def dates(ctx) -> None:
    today = _today()
    rows = query_all('SELECT d.id, d.title, d.description, d.date, d.type FROM comm_dates d '
                     'ORDER BY d.date ASC')

    body = page_header('Belangrijke Datums', ctx)
    body += '<h2 class="mt-4"><i data-lucide=calendar class=icon></i> Belangrijke Datums</h2>'
    body += comm_nav('dates', ctx)

    week_end = (datetime.date.today() + datetime.timedelta(days=7)).isoformat()
    body += '<div class="stat-row">'
    body += stat_card(sum(1 for d in rows if today <= d['date'] <= week_end), 'Deze week', '#B5916A')
    body += stat_card(sum(1 for d in rows if d['date'] >= today), 'Aankomend', '#5C7A5A')
    body += stat_card(sum(1 for d in rows if d['date'] < today), 'Geweest', '#B0A49A')
    body += '</div>'

    body += f'''<div class="card" style="margin-bottom:1rem;">
        <div class="section-title">+ Datum toevoegen</div>
        <form method="POST" action="/comm/dates/add"
              style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
            {ctx.csrf_input()}
            <div style="flex:2;min-width:160px;"><label class="form-label">Titel *</label>
                <input type="text" name="title" required class="form-control"></div>
            <div style="min-width:130px;"><label class="form-label">Datum *</label>
                <input type="date" name="date" required class="form-control"></div>
            <div style="min-width:120px;"><label class="form-label">Type</label>
                <select name="type" class="form-select">
                    <option value="event">Event</option>
                    <option value="deadline">Deadline</option>
                    <option value="mijlpaal">Mijlpaal</option></select></div>
            <div style="flex:2;min-width:160px;"><label class="form-label">Omschrijving</label>
                <input type="text" name="description" class="form-control"></div>
            <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
        </form></div>'''

    if not rows:
        body += '<div class="card"><p style="color:#B0A49A;">Nog geen datums.</p></div>'

    months = OrderedDict()
    for row in rows:
        months.setdefault(row['date'][:7], []).append(row)

    for month_key, entries in months.items():
        past_month = month_key < today[:7]
        try:
            label = datetime.datetime.strptime(month_key, '%Y-%m').strftime('%B %Y').capitalize()
        except ValueError:
            label = month_key
        body += (f'<div class="card" style="opacity:{"0.65" if past_month else "1"};">'
                 f'<div style="font-weight:bold;margin-bottom:0.5rem;">{html.escape(label)}</div>')
        for entry in entries:
            icon, color, bg = DATE_CFG.get(entry['type'], ('calendar', '#555', '#f5f5f5'))
            delta = (datetime.date.fromisoformat(entry['date']) - datetime.date.today()).days
            if delta == 0:
                relative = '<span style="color:#B5916A;font-weight:bold;">Vandaag!</span>'
            elif delta < 0:
                relative = f'<span style="color:#B0A49A;">{abs(delta)} dagen geleden</span>'
            elif delta <= 7:
                relative = f'<span style="color:#B5916A;font-weight:bold;">Over {delta} dag(en)</span>'
            else:
                relative = f'<span style="color:#7A6E66;">Over {delta} dagen</span>'
            desc = (f'<div style="font-size:0.82rem;color:#7A6E66;">'
                    f'{html.escape(entry["description"])}</div>') if entry['description'] else ''
            buttons = post_button('/comm/dates/to-task', ctx, '→ Taak',
                                  confirm='Taak aanmaken van deze datum?',
                                  css='btn btn-sm btn-secondary', fields={'id': entry['id']})
            buttons += (f' <a href="/comm/dates/edit?id={entry["id"]}" '
                        f'class="btn btn-sm btn-secondary">'
                        f'<i data-lucide=pencil class=icon></i></a> ')
            buttons += post_button('/comm/dates/delete', ctx, '<i data-lucide=x class=icon></i>',
                                   confirm='Verwijderen?', fields={'id': entry['id']})
            body += (f'<div style="display:flex;align-items:center;gap:0.75rem;padding:0.45rem 0;'
                     f'border-bottom:1px solid #EDE8E3;flex-wrap:wrap;">'
                     f'<div style="min-width:90px;font-size:0.82rem;font-weight:bold;color:{color};">'
                     f'{html.escape(entry["date"])}</div>'
                     f'<span style="background:{bg};color:{color};border-radius:4px;'
                     f'padding:0.1rem 0.4rem;font-size:0.75rem;">'
                     f'<i data-lucide={icon} class=icon></i> '
                     f'{html.escape(entry["type"].capitalize())}</span>'
                     f'<div style="flex:1;"><strong style="font-size:0.9rem;">'
                     f'{html.escape(entry["title"])}</strong> {relative}{desc}</div>'
                     f'<div style="display:flex;gap:0.3rem;align-items:center;">{buttons}</div></div>')
        body += '</div>'

    body += page_footer()
    ctx.html(body)


def date_add(ctx) -> None:
    title = ctx.f('title')
    date_value = ctx.f('date')
    if title and date_value:
        with connect() as conn:
            conn.execute(
                'INSERT INTO comm_dates (title, description, date, type, created_by) '
                'VALUES (?, ?, ?, ?, ?)',
                (title, ctx.f('description') or None, date_value,
                 ctx.choice(ctx.f('type', 'event'), config.COMM_DATE_TYPES, 'event'), ctx.user_id))
    ctx.redirect('/comm/dates')


def date_edit(ctx) -> None:
    date_id = ctx.qint('id')
    if not date_id:
        ctx.redirect('/comm/dates')
        return
    if ctx.method == 'POST':
        title = ctx.f('title')
        date_value = ctx.f('date')
        if title and date_value:
            with connect() as conn:
                conn.execute('UPDATE comm_dates SET title=?, description=?, date=?, type=? WHERE id=?',
                             (title, ctx.f('description') or None, date_value,
                              ctx.choice(ctx.f('type', 'event'), config.COMM_DATE_TYPES, 'event'),
                              date_id))
        ctx.redirect('/comm/dates')
        return

    row = query_one('SELECT * FROM comm_dates WHERE id = ?', (date_id,))
    if not row:
        ctx.redirect('/comm/dates')
        return
    options = ''.join(f'<option value="{v}"{" selected" if row["type"] == v else ""}>{l}</option>'
                      for v, l in [('event', 'Event'), ('deadline', 'Deadline'),
                                   ('mijlpaal', 'Mijlpaal')])
    body = page_header('Datum bewerken', ctx)
    body += '<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Datum bewerken</h2>'
    body += f'''<div class="card" style="max-width:520px;">
        <form method="POST" action="/comm/dates/edit?id={date_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Titel *</label>
                <input type="text" name="title" value="{html.escape(row['title'])}"
                       required class="form-control"></div>
            <div style="display:flex;gap:0.5rem;flex-wrap:wrap;">
                <div style="flex:1;min-width:130px;"><label class="form-label">Datum *</label>
                    <input type="date" name="date" value="{html.escape(str(row['date']))}"
                           required class="form-control"></div>
                <div style="flex:1;min-width:120px;"><label class="form-label">Type</label>
                    <select name="type" class="form-select">{options}</select></div>
            </div>
            <div class="mb-3 mt-3"><label class="form-label">Omschrijving</label>
                <input type="text" name="description" value="{html.escape(row['description'] or '')}"
                       class="form-control"></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/comm/dates" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def date_delete(ctx) -> None:
    date_id = ctx.fint('id')
    if date_id:
        with connect() as conn:
            conn.execute('DELETE FROM comm_dates WHERE id = ?', (date_id,))
    ctx.redirect('/comm/dates')


def date_to_task(ctx) -> None:
    date_id = ctx.fint('id')
    if date_id:
        with connect() as conn:
            row = conn.execute('SELECT * FROM comm_dates WHERE id = ?', (date_id,)).fetchone()
            if row:
                conn.execute(
                    'INSERT INTO comm_tasks (title, description, status, due_date, created_by, '
                    ' priority) VALUES (?, ?, ?, ?, ?, ?)',
                    (row['title'], row['description'], 'backlog', row['date'],
                     ctx.user_id, 'medium'))
    ctx.redirect('/comm/board')


# ── Content calendar ──────────────────────────────────────────────────────
def content(ctx) -> None:
    today = _today()
    with connect(readonly=True) as conn:
        items = conn.execute('''
            SELECT cc.id, cc.title, cc.description, cc.platform, cc.publish_date, cc.status,
                   cc.tags, cc.assigned_to, cc.board_status, u.username AS assigned_to_name
              FROM comm_content cc LEFT JOIN users u ON cc.assigned_to = u.id
             ORDER BY COALESCE(cc.publish_date,'9999-12-31') ASC, cc.created_at DESC''').fetchall()
        members = conn.execute('SELECT id, username FROM users '
                               'WHERE is_comm=1 OR is_admin=1 OR id=1 ORDER BY username').fetchall()

    by_status = {s: [i for i in items if i['status'] == s] for s in config.COMM_CONTENT_STATUSES}

    body = page_header('Content Kalender', ctx)
    body += '<h2 class="mt-4"><i data-lucide=newspaper class=icon></i> Content Kalender</h2>'
    body += comm_nav('content', ctx)

    body += '<div class="stat-row">'
    for status, label, color in [('idee', 'Ideeën', '#888'), ('gepland', 'Gepland', '#7A8FA6'),
                                 ('klaar', 'Klaar', '#B5916A'),
                                 ('gepubliceerd', 'Gepubliceerd', '#5C7A5A')]:
        body += stat_card(len(by_status[status]), label, color)
    body += '</div>'

    member_options = '<option value="">Niet toegewezen</option>' + ''.join(
        f'<option value="{m["id"]}"{" selected" if m["id"] == ctx.user_id else ""}>'
        f'{html.escape(m["username"])}</option>' for m in members)
    body += f'''<div class="card" style="margin-bottom:1rem;">
        <div class="section-title">+ Nieuw content item</div>
        <form method="POST" action="/comm/content/add"
              style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
            {ctx.csrf_input()}
            <div style="flex:2;min-width:160px;"><label class="form-label">Titel *</label>
                <input type="text" name="title" required class="form-control"></div>
            <div style="min-width:110px;"><label class="form-label">Platform</label>
                <select name="platform" class="form-select">
                    <option value="instagram">Instagram</option>
                    <option value="linkedin">LinkedIn</option>
                    <option value="website">Website</option>
                    <option value="email">Email</option>
                    <option value="overig">Overig</option></select></div>
            <div style="min-width:110px;"><label class="form-label">Status</label>
                <select name="status" class="form-select">
                    <option value="idee">Idee</option>
                    <option value="gepland">Gepland</option>
                    <option value="klaar">Klaar</option></select></div>
            <div style="min-width:130px;"><label class="form-label">Publicatiedatum</label>
                <input type="date" name="publish_date" class="form-control"></div>
            <div style="min-width:120px;"><label class="form-label">Toegewezen aan</label>
                <select name="assigned_to" class="form-select">{member_options}</select></div>
            <div style="min-width:100px;"><label class="form-label">Tags</label>
                <input type="text" name="tags" class="form-control"></div>
            <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
        </form></div>'''

    column_style = 'flex:1;min-width:220px;background:#EDE8E3;border-radius:8px;padding:0.75rem;'
    body += '<div style="display:flex;gap:1rem;flex-wrap:wrap;align-items:flex-start;">'
    for status, label, color in [('idee', 'Idee', '#888'), ('gepland', 'Gepland', '#7A8FA6'),
                                 ('klaar', 'Klaar', '#B5916A'),
                                 ('gepubliceerd', 'Gepubliceerd', '#5C7A5A')]:
        column = by_status[status]
        body += (f'<div style="{column_style}"><div style="font-weight:bold;margin-bottom:0.75rem;">'
                 f'{label} <span style="background:{color};color:#fff;border-radius:10px;'
                 f'padding:0.1rem 0.5rem;font-size:0.78rem;">{len(column)}</span></div>')
        body += (''.join(_content_item_card(ctx, item, today) for item in column)
                 or '<div style="color:#B0A49A;font-size:0.85rem;">Leeg</div>')
        body += '</div>'
    body += '</div>'
    body += page_footer()
    ctx.html(body)


def _content_item_card(ctx, item, today) -> str:
    icon, color, bg = PLATFORM_CFG.get(item['platform'], ('map-pin', '#7A6E66', '#F2EEE9'))
    late = (item['publish_date'] and item['publish_date'] < today
            and item['status'] != 'gepubliceerd')
    date = (f'<div style="font-size:0.75rem;color:{"#C0392B" if late else "#555"};">'
            f'<i data-lucide=calendar class=icon></i> '
            f'{html.escape(str(item["publish_date"]))}</div>') if item['publish_date'] else ''
    who = (f'<div style="font-size:0.75rem;color:#B0A49A;"><i data-lucide=user class=icon></i> '
           f'{html.escape(item["assigned_to_name"])}</div>') if item['assigned_to_name'] else ''
    tags = ''.join(f'<span style="font-size:0.7rem;background:#EDF3EC;color:#7A8FA6;'
                   f'border-radius:3px;padding:0.05rem 0.3rem;">{html.escape(t.strip())}</span>'
                   for t in (item['tags'] or '').split(',') if t.strip())

    moves = ' '.join(
        post_button('/comm/content/move', ctx, label, css='btn btn-sm btn-secondary',
                    style='font-size:0.68rem;', fields={'id': item['id'], 'status': status})
        for status, label in [('idee', 'Idee'), ('gepland', 'Gepland'),
                              ('klaar', 'Klaar'), ('gepubliceerd', 'Publiceer')]
        if status != item['status'])

    edit = (f'<a href="/comm/content/edit?id={item["id"]}" style="color:#7A8FA6;'
            f'font-size:0.78rem;margin-right:0.4rem;"><i data-lucide=pencil class=icon></i></a>')
    remove = post_button('/comm/content/delete', ctx, '<i data-lucide=x class=icon></i>',
                         confirm='Verwijderen?', css='btn-link',
                         style='color:#C0392B;font-size:0.78rem;', fields={'id': item['id']})
    to_task = post_button('/comm/content/to-task', ctx, '→ Taak',
                          confirm='Als taak toevoegen aan board?',
                          css='btn btn-sm btn-secondary', style='font-size:0.68rem;',
                          fields={'id': item['id']})

    board_options = ''.join(
        f'<option value="{v}"{" selected" if (item["board_status"] or "") == v else ""}>{l}</option>'
        for v, l in [('', '— Niet in board'), ('backlog', 'Board: Backlog'),
                     ('bezig', 'Board: Bezig'), ('klaar', 'Board: Klaar')])

    return f'''<div style="background:#fff;border-radius:6px;padding:0.6rem 0.7rem;
            margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.1);border-left:3px solid {color};">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;">
            <div style="font-size:0.88rem;font-weight:bold;flex:1;">{html.escape(item['title'])}</div>
            <div style="display:flex;align-items:center;">{edit}{remove}</div>
        </div>
        <div style="margin:0.2rem 0;">
            <span style="background:{bg};color:{color};border-radius:3px;padding:0.05rem 0.35rem;
                         font-size:0.73rem;"><i data-lucide={icon} class=icon></i>
                {html.escape(item['platform'].capitalize())}</span> {tags}</div>
        {who}{date}
        <div style="margin-top:0.4rem;display:flex;gap:0.3rem;flex-wrap:wrap;align-items:center;">
            {moves} {to_task}
            <form method="POST" action="/comm/content/board-status" class="inline-form">
                {ctx.csrf_input()}
                <input type="hidden" name="id" value="{item['id']}">
                <select name="status" class="form-control" style="width:auto;display:inline;
                        font-size:0.72rem;padding:0.1rem 0.25rem;"
                        onchange="this.form.submit()">{board_options}</select></form>
        </div></div>'''


def _content_fields(ctx):
    return {
        'title': ctx.f('title'),
        'description': ctx.f('description') or None,
        'platform': ctx.choice(ctx.f('platform', 'overig'), config.COMM_PLATFORMS, 'overig'),
        'publish_date': ctx.f('publish_date') or None,
        'status': ctx.choice(ctx.f('status', 'idee'), config.COMM_CONTENT_STATUSES, 'idee'),
        'assigned_to': ctx.fint('assigned_to'),
        'tags': ctx.f('tags') or None,
    }


def content_add(ctx) -> None:
    values = _content_fields(ctx)
    if values['title']:
        with connect() as conn:
            conn.execute(
                'INSERT INTO comm_content (title, description, platform, publish_date, status, '
                ' assigned_to, created_by, tags) VALUES (:title, :description, :platform, '
                ' :publish_date, :status, :assigned_to, :created_by, :tags)',
                {**values, 'created_by': ctx.user_id})
    ctx.redirect('/comm/content')


def content_edit(ctx) -> None:
    content_id = ctx.qint('id')
    if not content_id:
        ctx.redirect('/comm/content')
        return
    if ctx.method == 'POST':
        values = _content_fields(ctx)
        if values['title']:
            with connect() as conn:
                conn.execute(
                    'UPDATE comm_content SET title=:title, description=:description, '
                    ' platform=:platform, publish_date=:publish_date, status=:status, '
                    ' assigned_to=:assigned_to, tags=:tags WHERE id=:id',
                    {**values, 'id': content_id})
        ctx.redirect('/comm/content')
        return

    item = query_one('SELECT * FROM comm_content WHERE id = ?', (content_id,))
    if not item:
        ctx.redirect('/comm/content')
        return
    members = comm_members()
    platform_options = ''.join(
        f'<option value="{v}"{" selected" if item["platform"] == v else ""}>{v.capitalize()}</option>'
        for v in config.COMM_PLATFORMS)
    status_options = ''.join(
        f'<option value="{v}"{" selected" if item["status"] == v else ""}>{v.capitalize()}</option>'
        for v in config.COMM_CONTENT_STATUSES)
    member_options = '<option value="">Niet toegewezen</option>' + ''.join(
        f'<option value="{m["id"]}"{" selected" if item["assigned_to"] == m["id"] else ""}>'
        f'{html.escape(m["username"])}</option>' for m in members)

    body = page_header('Content bewerken', ctx)
    body += '<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Content item bewerken</h2>'
    body += f'''<div class="card" style="max-width:600px;">
        <form method="POST" action="/comm/content/edit?id={content_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Titel *</label>
                <input type="text" name="title" value="{html.escape(item['title'])}"
                       required class="form-control"></div>
            <div class="mb-3"><label class="form-label">Omschrijving</label>
                <textarea name="description" class="form-control" rows="3">{html.escape(item['description'] or '')}</textarea></div>
            <div style="display:flex;gap:0.5rem;flex-wrap:wrap;">
                <div style="flex:1;min-width:110px;"><label class="form-label">Platform</label>
                    <select name="platform" class="form-select">{platform_options}</select></div>
                <div style="flex:1;min-width:110px;"><label class="form-label">Status</label>
                    <select name="status" class="form-select">{status_options}</select></div>
                <div style="flex:1;min-width:130px;"><label class="form-label">Publicatiedatum</label>
                    <input type="date" name="publish_date" class="form-control"
                           value="{html.escape(str(item['publish_date'] or ''))}"></div>
                <div style="flex:1;min-width:130px;"><label class="form-label">Toegewezen aan</label>
                    <select name="assigned_to" class="form-select">{member_options}</select></div>
            </div>
            <div class="mb-3 mt-3"><label class="form-label">Tags</label>
                <input type="text" name="tags" value="{html.escape(item['tags'] or '')}"
                       class="form-control"></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/comm/content" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def content_move(ctx) -> None:
    content_id = ctx.fint('id')
    status = ctx.choice(ctx.f('status'), config.COMM_CONTENT_STATUSES, 'idee')
    if content_id:
        with connect() as conn:
            conn.execute('UPDATE comm_content SET status = ? WHERE id = ?', (status, content_id))
    ctx.redirect('/comm/content')


def content_delete(ctx) -> None:
    content_id = ctx.fint('id')
    if content_id:
        with connect() as conn:
            conn.execute('DELETE FROM comm_content WHERE id = ?', (content_id,))
    ctx.redirect('/comm/content')


def content_board_status(ctx) -> None:
    content_id = ctx.fint('id')
    status = ctx.f('status')
    if status not in config.COMM_TASK_STATUSES:
        status = ''
    if content_id:
        with connect() as conn:
            conn.execute('UPDATE comm_content SET board_status = ? WHERE id = ?',
                         (status or None, content_id))
    ctx.redirect('/comm/content')


def content_to_task(ctx) -> None:
    content_id = ctx.fint('id')
    if content_id:
        with connect() as conn:
            item = conn.execute('SELECT * FROM comm_content WHERE id = ?', (content_id,)).fetchone()
            if item:
                conn.execute(
                    'INSERT INTO comm_tasks (title, description, status, due_date, assigned_to, '
                    ' created_by, priority, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                    (f'[{(item["platform"] or "overig").capitalize()}] {item["title"]}',
                     item['description'], 'backlog', item['publish_date'], item['assigned_to'],
                     ctx.user_id, 'medium', item['tags']))
    ctx.redirect('/comm/board')


# ── Events governance ─────────────────────────────────────────────────────
def events_gov(ctx) -> None:
    today = _today()
    rows = query_all('''
        SELECT eg.*, u.username AS assigned_name
          FROM events_gov_tasks eg LEFT JOIN users u ON eg.assigned_to = u.id
         ORDER BY CASE eg.priority WHEN 'hoog' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,
                  COALESCE(eg.due_date,'9999-12-31') ASC, eg.created_at DESC''')
    users = query_all('SELECT id, username FROM users ORDER BY username ASC')

    body = page_header('Events Gov', ctx)
    body += '<h2 class="mt-4"><i data-lucide=flag class=icon></i> Events Gov</h2>'
    body += comm_nav('events-gov', ctx)

    user_options = '<option value="">-- Niemand --</option>' + ''.join(
        f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in users)
    body += f'''<details style="margin-bottom:1rem;">
        <summary style="cursor:pointer;font-weight:bold;padding:0.5rem 0.75rem;background:#fff;
                        border-radius:6px;border:1px solid #E4DDD6;">
            + Governance check toevoegen</summary>
        <div class="card" style="margin-top:0.35rem;">
            <form method="POST" action="/comm/events-gov/add"
                  style="display:flex;flex-direction:column;gap:0.5rem;">
                {ctx.csrf_input()}
                <input type="text" name="title" class="form-control" required
                       placeholder="Wat moet gecheckt worden? *">
                <input type="text" name="event_context" class="form-control"
                       placeholder="Event of context (bijv. HAN Goes Green 2026)">
                <textarea name="description" class="form-control" rows="2"
                          placeholder="Toelichting / norm"></textarea>
                <div style="display:flex;gap:0.5rem;flex-wrap:wrap;">
                    <select name="assigned_to" class="form-select" style="flex:1;">{user_options}</select>
                    <select name="priority" class="form-select" style="flex:1;">
                        <option value="hoog">Hoog</option>
                        <option value="medium" selected>Medium</option>
                        <option value="laag">Laag</option></select>
                    <input type="date" name="due_date" class="form-control" style="flex:1;">
                </div>
                <button type="submit" class="btn btn-primary" style="align-self:flex-start;">
                    Toevoegen</button>
            </form></div></details>'''

    body += '<div style="display:flex;gap:1rem;align-items:flex-start;flex-wrap:wrap;">'
    for key, label, bg, color in EVENT_STATUSES:
        column = [t for t in rows if t['status'] == key]
        body += (f'<div style="flex:1;min-width:260px;background:{bg};border-radius:8px;'
                 f'padding:0.75rem;border-top:4px solid {color};">'
                 f'<div style="font-weight:bold;color:{color};margin-bottom:0.6rem;">'
                 f'{label} ({len(column)})</div>')
        if not column:
            body += '<div style="color:#B0A49A;font-size:0.85rem;font-style:italic;">Leeg</div>'
        for task in column:
            late = task['due_date'] and task['due_date'] < today and key != 'klaar'
            date = (f'<span style="font-size:0.75rem;color:{"#C0392B" if late else "#888"};">'
                    f'<i data-lucide=calendar class=icon></i> '
                    f'{html.escape(str(task["due_date"]))}</span>') if task['due_date'] else ''
            dot_color = {'hoog': '#C0392B', 'medium': '#B5916A',
                         'laag': '#5C7A5A'}.get(task['priority'], '#aaa')
            context = (f'<div style="font-size:0.75rem;color:#7A6E66;">'
                       f'{html.escape(task["event_context"])}</div>') if task['event_context'] else ''
            desc = (f'<div style="font-size:0.78rem;color:#7A6E66;margin:0.2rem 0;">'
                    f'{html.escape(task["description"])}</div>') if task['description'] else ''
            who = (f'<span style="font-size:0.75rem;color:#7A8FA6;">'
                   f'<i data-lucide=user class=icon></i> {html.escape(task["assigned_name"])}</span>'
                   ) if task['assigned_name'] else '<span style="font-size:0.75rem;color:#B0A49A;">Niemand</span>'
            buttons = ' '.join(
                post_button('/comm/events-gov/status', ctx, other_label,
                            css='btn btn-sm btn-secondary', style='font-size:0.72rem;',
                            fields={'id': task['id'], 'status': other_key})
                for other_key, other_label, _bg, _c in EVENT_STATUSES if other_key != key)
            buttons += ' ' + post_button('/comm/events-gov/delete', ctx,
                                         '<i data-lucide=x class=icon></i>',
                                         confirm='Verwijderen?', css='btn btn-sm btn-danger',
                                         style='font-size:0.72rem;', fields={'id': task['id']})
            body += (f'<div style="background:#fff;border-radius:6px;padding:0.6rem;'
                     f'margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.08);">'
                     f'<div style="font-weight:bold;font-size:0.88rem;">'
                     f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
                     f'background:{dot_color};margin-right:4px;"></span>'
                     f'{html.escape(task["title"])}</div>{context}{desc}'
                     f'<div style="display:flex;justify-content:space-between;align-items:center;'
                     f'margin-top:0.4rem;flex-wrap:wrap;gap:0.2rem;">{who} {date}</div>'
                     f'<div style="display:flex;gap:0.25rem;margin-top:0.4rem;flex-wrap:wrap;">'
                     f'{buttons}</div></div>')
        body += '</div>'
    body += '</div>'
    body += page_footer()
    ctx.html(body)


def events_gov_add(ctx) -> None:
    title = ctx.f('title')
    if title:
        with connect() as conn:
            conn.execute(
                'INSERT INTO events_gov_tasks (title, description, event_context, assigned_to, '
                ' due_date, priority, created_by) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (title, ctx.f('description') or None, ctx.f('event_context') or None,
                 ctx.fint('assigned_to'), ctx.f('due_date') or None,
                 ctx.choice(ctx.f('priority', 'medium'), config.PRIORITIES, 'medium'), ctx.user_id))
    ctx.redirect('/comm/events-gov')


def events_gov_status(ctx) -> None:
    task_id = ctx.fint('id')
    status = ctx.f('status')
    if task_id and status in config.EVENTS_GOV_STATUSES:
        with connect() as conn:
            conn.execute('UPDATE events_gov_tasks SET status = ? WHERE id = ?', (status, task_id))
    ctx.redirect('/comm/events-gov')


def events_gov_delete(ctx) -> None:
    task_id = ctx.fint('id')
    if task_id:
        with connect() as conn:
            conn.execute('DELETE FROM events_gov_tasks WHERE id = ?', (task_id,))
    ctx.redirect('/comm/events-gov')
