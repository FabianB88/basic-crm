"""The main dashboard."""

from __future__ import annotations

import datetime
import html
import urllib.parse

from ..db import connect
from ..ui import page_footer, page_header, stat_card

VERBINDING_TABS = [('ambassadeur', '#5C7A5A'), ('betrokken', '#7A8FA6'), ('niet betrokken', '#B0A49A')]


def show(ctx) -> None:
    today = datetime.date.today()
    today_iso = today.isoformat()
    this_month = today.strftime('%Y-%m')

    with connect(readonly=True) as conn:
        totals = conn.execute('''
            SELECT (SELECT COUNT(*) FROM customers)                                  AS customers,
                   (SELECT COUNT(*) FROM tasks WHERE status='open')                  AS open_tasks,
                   (SELECT COUNT(*) FROM tasks
                     WHERE status='open' AND due_date < DATE('now'))                 AS overdue,
                   (SELECT COUNT(*) FROM interactions
                     WHERE strftime('%Y-%m', COALESCE(contact_date, created_at)) = ?) AS interactions
        ''', (this_month,)).fetchone()

        verbinding = {r['verbinding']: r['cnt'] for r in conn.execute(
            'SELECT verbinding, COUNT(*) AS cnt FROM customers '
            'WHERE verbinding IS NOT NULL GROUP BY verbinding'
        )}

        user_stats = conn.execute('''
            SELECT u.id, u.username,
                   SUM(CASE WHEN t.status='open' THEN 1 ELSE 0 END) AS open_tasks,
                   SUM(CASE WHEN t.status='open' AND t.due_date < DATE('now')
                            THEN 1 ELSE 0 END)                       AS overdue_tasks
              FROM users u
              LEFT JOIN tasks t ON t.user_id = u.id
             GROUP BY u.id ORDER BY u.username ASC
        ''').fetchall()

        inter_by_user = {r['id']: r['n'] for r in conn.execute('''
            SELECT u.id, COUNT(i.id) AS n
              FROM users u
              LEFT JOIN interactions i
                     ON i.user_id = u.id
                    AND strftime('%Y-%m', COALESCE(i.contact_date, i.created_at)) = ?
             GROUP BY u.id
        ''', (this_month,))}

        customers_by_user = {r['created_by']: r['cnt'] for r in conn.execute(
            'SELECT created_by, COUNT(*) AS cnt FROM customers '
            'WHERE created_by IS NOT NULL GROUP BY created_by'
        )}

        notes = conn.execute('''
            SELECT n.content, n.created_at, c.name AS customer_name
              FROM notes n JOIN customers c ON n.customer_id = c.id
             WHERE n.user_id = ?
             ORDER BY n.created_at DESC LIMIT 5
        ''', (ctx.user_id,)).fetchall()

        due_tasks = conn.execute('''
            SELECT t.id AS task_id, t.title, t.due_date,
                   c.name AS customer_name, c.id AS customer_id,
                   u.username AS assigned_to
              FROM tasks t
              JOIN customers c ON t.customer_id = c.id
              JOIN users u     ON t.user_id = u.id
             WHERE t.status = 'open'
               AND t.due_date IS NOT NULL
               AND DATE(t.due_date) <= DATE('now', '+14 day')
             ORDER BY t.due_date ASC LIMIT 20
        ''').fetchall()

    body = page_header('Dashboard', ctx)
    body += '<h2 class="mt-4">Dashboard</h2>'

    # ── Stat row ──
    body += '<div style="display:flex;gap:0.6rem;flex-wrap:wrap;margin-bottom:0.6rem;">'
    body += stat_card(totals['customers'], 'Klanten')
    body += stat_card(totals['open_tasks'], 'Open taken',
                      '#B5916A' if totals['open_tasks'] else '#1C1713')
    body += stat_card(totals['overdue'], 'Verlopen',
                      '#C0392B' if totals['overdue'] else '#1C1713')
    body += stat_card(totals['interactions'], 'Interacties')
    for key, color in VERBINDING_TABS:
        count = verbinding.get(key, 0)
        body += (f'<a href="/customers?verbinding={urllib.parse.quote(key)}" '
                 f'style="flex:1;min-width:100px;text-decoration:none;">'
                 f'<div class="card stat-card" style="border-top:3px solid {color};">'
                 f'<div class="stat-val">{count}</div>'
                 f'<div class="stat-label">{html.escape(key.capitalize())}</div>'
                 f'</div></a>')
    body += '</div>'

    # ── Per-user stats ──
    body += ('<details style="margin-bottom:0.75rem;">'
             '<summary style="cursor:pointer;font-size:0.85rem;font-weight:600;padding:0.55rem 0.85rem;'
             'background:#fff;border-radius:8px;border:1px solid #E4DDD6;color:#7A6E66;">'
             f'<i data-lucide=trending-up class=icon></i> Statistieken per gebruiker ({this_month})'
             '</summary><div class="card" style="margin-top:0.25rem;"><div class="table-wrap"><table>'
             '<thead><tr><th>Gebruiker</th><th>Open</th><th>Verlopen</th>'
             '<th>Interacties</th><th>Klanten</th></tr></thead><tbody>')
    for row in user_stats:
        overdue = row['overdue_tasks'] or 0
        color = '#C0392B' if overdue else '#1C1713'
        body += (f'<tr><td><a href="/users/profile?id={row["id"]}">'
                 f'{html.escape(row["username"])}</a></td>'
                 f'<td>{row["open_tasks"] or 0}</td>'
                 f'<td style="color:{color};font-weight:{"600" if overdue else "400"};">{overdue}</td>'
                 f'<td>{inter_by_user.get(row["id"], 0)}</td>'
                 f'<td>{customers_by_user.get(row["id"], 0)}</td></tr>')
    body += '</tbody></table></div></div></details>'

    # ── Open tasks ──
    if due_tasks:
        rows = ''
        for task in due_tasks:
            date_str = task['due_date'] or ''
            overdue = date_str < today_iso
            date_color = '#C0392B' if overdue else '#B0A49A'
            label = ('<span class="badge badge-danger">verlopen</span>' if overdue else '')
            rows += (
                f'<div class="task-row">'
                f'<a href="/tasks/resolve?id={task["task_id"]}&from=dashboard" '
                f'style="float:right;background:#F2EEE9;color:#7A6E66;border:1px solid #E4DDD6;'
                f'border-radius:5px;padding:0.15rem 0.55rem;font-size:0.78rem;text-decoration:none;">'
                f'<i data-lucide=check class=icon></i> Resolve</a>'
                f'{html.escape(task["title"])} {label}<br>'
                f'<a href="/customers/view?id={task["customer_id"]}" style="font-weight:600;">'
                f'{html.escape(task["customer_name"])}</a> &middot; '
                f'<small style="color:#B0A49A;">{html.escape(task["assigned_to"] or "")}</small> &middot; '
                f'<small style="color:{date_color};">'
                f'<i data-lucide=calendar class=icon></i> {html.escape(date_str)}</small></div>')
    else:
        rows = '<p style="color:#B0A49A;font-size:0.875rem;">Geen openstaande taken.</p>'
    body += (f'<div class="card"><div class="section-title">Openstaande taken '
             f'<a href="/tasks/archive" style="float:right;font-size:0.82rem;color:#B0A49A;'
             f'font-weight:normal;"><i data-lucide=archive class=icon></i> Archief</a></div>'
             f'{rows}</div>')

    # ── Recent notes ──
    if notes:
        note_html = ''
        for note in notes:
            content = note['content'] or ''
            snippet = (content[:100] + '…') if len(content) > 100 else content
            note_html += (f'<div class="task-row"><strong>'
                          f'{html.escape(note["customer_name"])}</strong><br>'
                          f'{html.escape(snippet)}'
                          f'<div style="font-size:0.8rem;color:#7A6E66;">{note["created_at"]}</div></div>')
    else:
        note_html = '<p style="color:#B0A49A;">Er zijn nog geen notities.</p>'
    body += f'<div class="card"><div class="section-title">Recente notities</div>{note_html}</div>'

    body += page_footer()
    ctx.html(body)
