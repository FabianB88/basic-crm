"""Internal messaging.

Every route here raised "no such table: messages" until the schema was
restored — including the poller that runs on every page.
"""

from __future__ import annotations

import html

from ..db import connect, log_action, query_all, query_one
from ..ui import page_footer, page_header

CONVERSATIONS_SQL = '''
    SELECT other_id,
           u.username AS other_name,
           MAX(m.created_at) AS last_at,
           SUM(CASE WHEN m.recipient_id = :me AND m.is_read = 0 THEN 1 ELSE 0 END) AS unread,
           (SELECT content FROM messages m2
             WHERE (m2.sender_id = m.other_id AND m2.recipient_id = :me)
                OR (m2.sender_id = :me AND m2.recipient_id = m.other_id)
             ORDER BY m2.created_at DESC, m2.id DESC LIMIT 1) AS last_content
      FROM (SELECT CASE WHEN sender_id = :me THEN recipient_id ELSE sender_id END AS other_id,
                   created_at, recipient_id, is_read
              FROM messages
             WHERE sender_id = :me OR recipient_id = :me) m
      JOIN users u ON u.id = m.other_id
     GROUP BY other_id
     ORDER BY last_at DESC
'''


def poll(ctx) -> None:
    if not ctx.logged_in:
        ctx.json({'unread': 0, 'latest': None})
        return
    with connect(readonly=True) as conn:
        unread = conn.execute(
            'SELECT COUNT(*) FROM messages WHERE recipient_id = ? AND is_read = 0',
            (ctx.user_id,)).fetchone()[0]
        row = conn.execute('''
            SELECT m.sender_id, m.content, u.username AS from_user
              FROM messages m JOIN users u ON m.sender_id = u.id
             WHERE m.recipient_id = ? AND m.is_read = 0
             ORDER BY m.created_at DESC, m.id DESC LIMIT 1''', (ctx.user_id,)).fetchone()
    latest = ({'sender_id': row['sender_id'], 'from': row['from_user'], 'content': row['content']}
              if row else None)
    ctx.json({'unread': unread, 'latest': latest})


def conversations(ctx) -> None:
    rows = query_all(CONVERSATIONS_SQL, {'me': ctx.user_id})
    others = query_all('SELECT id, username FROM users WHERE id != ? ORDER BY username',
                       (ctx.user_id,))

    body = page_header('Berichten', ctx)
    body += '<h2 class="mt-4"><i data-lucide=message-circle class=icon></i> Berichten</h2>'
    options = ''.join(f'<option value="{u["id"]}">{html.escape(u["username"])}</option>'
                      for u in others)
    body += f'''<div style="margin-bottom:1rem;">
        <form method="GET" action="/messages/conversation"
              style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;">
            <select name="with" class="form-select" style="max-width:240px;" required>
                <option value="">— Nieuw gesprek met... —</option>{options}</select>
            <button type="submit" class="btn btn-primary">+ Start gesprek</button>
        </form></div>'''

    if rows:
        body += '<div class="card" style="padding:0;">'
        for conv in rows:
            unread = conv['unread'] or 0
            snippet = html.escape((conv['last_content'] or '')[:80])
            badge = (f'<span style="background:#5C7A5A;color:#fff;border-radius:50%;'
                     f'font-size:0.75rem;font-weight:bold;min-width:20px;height:20px;'
                     f'line-height:20px;text-align:center;display:inline-block;'
                     f'margin-left:0.4rem;">{unread}</span>') if unread else ''
            initial = html.escape((conv['other_name'] or '?')[0].upper())
            body += (
                f'<a href="/messages/conversation?with={conv["other_id"]}" '
                f'style="display:flex;align-items:center;padding:0.85rem 1.1rem;'
                f'border-bottom:1px solid #EDE8E3;text-decoration:none;color:inherit;'
                f'{"background:#EDF3EC;" if unread else ""}">'
                f'<div style="width:38px;height:38px;border-radius:50%;background:#5C7A5A;'
                f'color:#fff;display:flex;align-items:center;justify-content:center;'
                f'font-weight:bold;margin-right:0.85rem;flex-shrink:0;">{initial}</div>'
                f'<div style="flex:1;min-width:0;">'
                f'<div style="font-weight:{"bold" if unread else "normal"};">'
                f'{html.escape(conv["other_name"])}{badge}</div>'
                f'<div style="font-size:0.85rem;color:#B0A49A;overflow:hidden;'
                f'text-overflow:ellipsis;white-space:nowrap;">{snippet}</div></div>'
                f'<div style="font-size:0.78rem;color:#B0A49A;flex-shrink:0;margin-left:0.5rem;">'
                f'{html.escape(str(conv["last_at"] or "")[:16])}</div></a>')
        body += '</div>'
    else:
        body += '<p style="color:#B0A49A;">Nog geen berichten. Start hierboven een gesprek.</p>'
    body += page_footer()
    ctx.html(body)


def conversation(ctx) -> None:
    other_id = ctx.qint('with')
    if not other_id or other_id == ctx.user_id:
        ctx.redirect('/messages')
        return
    other = query_one('SELECT id, username FROM users WHERE id = ?', (other_id,))
    if not other:
        ctx.redirect('/messages')
        return

    if ctx.method == 'POST':
        content = ctx.f('content')
        if content:
            reply_to = ctx.fint('reply_to')
            if reply_to and not query_one(
                'SELECT 1 FROM messages WHERE id = ? AND (sender_id IN (?, ?)) '
                'AND (recipient_id IN (?, ?))',
                (reply_to, ctx.user_id, other_id, ctx.user_id, other_id)
            ):
                reply_to = None
            with connect() as conn:
                cur = conn.execute(
                    'INSERT INTO messages (sender_id, recipient_id, content, reply_to) '
                    'VALUES (?, ?, ?, ?)', (ctx.user_id, other_id, content, reply_to))
                log_action(ctx.user_id, 'create', 'messages', cur.lastrowid,
                           f'aan gebruiker {other_id}')
        ctx.redirect(f'/messages/conversation?with={other_id}')
        return

    with connect() as conn:
        conn.execute('UPDATE messages SET is_read = 1 '
                     'WHERE sender_id = ? AND recipient_id = ? AND is_read = 0',
                     (other_id, ctx.user_id))
        rows = conn.execute('''
            SELECT m.id, m.sender_id, m.content, m.created_at,
                   r.content AS reply_content, ru.username AS reply_from
              FROM messages m
              LEFT JOIN messages r  ON m.reply_to = r.id
              LEFT JOIN users ru    ON r.sender_id = ru.id
             WHERE (m.sender_id = ? AND m.recipient_id = ?)
                OR (m.sender_id = ? AND m.recipient_id = ?)
             ORDER BY m.created_at ASC, m.id ASC''',
            (ctx.user_id, other_id, other_id, ctx.user_id)).fetchall()

    name = other['username']
    body = page_header(f'Gesprek met {name}', ctx)
    body += (f'<div style="display:flex;align-items:center;gap:0.75rem;margin:1.5rem 0 1rem;">'
             f'<a href="/messages"><i data-lucide=arrow-left class=icon></i> Terug</a>'
             f'<h2 style="margin:0;"><i data-lucide=message-circle class=icon></i> '
             f'{html.escape(name)}</h2></div>')
    body += ('<div id="chat-box" style="display:flex;flex-direction:column;gap:0.5rem;'
             'margin-bottom:1.2rem;max-height:65vh;overflow-y:auto;padding:0.5rem;">')
    for msg in rows:
        mine = msg['sender_id'] == ctx.user_id
        align = 'flex-end' if mine else 'flex-start'
        bg = '#5C7A5A' if mine else '#EDE8E3'
        fg = '#fff' if mine else '#333'
        quoted = ''
        if msg['reply_content']:
            border = 'rgba(255,255,255,0.5)' if mine else '#5C7A5A'
            quoted = (f'<div style="font-size:0.78rem;border-left:3px solid {border};'
                      f'padding-left:0.4rem;margin-bottom:0.3rem;opacity:0.85;">'
                      f'{html.escape(msg["reply_from"] or "")}: '
                      f'{html.escape((msg["reply_content"] or "")[:60])}</div>')
        preview = html.escape((msg['content'] or '')[:60])
        body += (
            f'<div style="display:flex;flex-direction:column;align-items:{align};">'
            f'<div style="background:{bg};color:{fg};border-radius:12px;padding:0.55rem 0.85rem;'
            f'max-width:70%;word-break:break-word;">{quoted}{html.escape(msg["content"] or "")}</div>'
            f'<div style="font-size:0.75rem;color:#B0A49A;margin-top:0.15rem;display:flex;'
            f'gap:0.5rem;align-items:center;">{html.escape(str(msg["created_at"] or "")[:16])}'
            f'<button type="button" class="btn-link" style="font-size:0.75rem;padding:0;"'
            f' data-reply-id="{msg["id"]}" data-reply-from="{html.escape(msg["reply_from"] or name)}"'
            f' data-reply-text="{preview}" onclick="setReply(this)">'
            f'<i data-lucide=corner-down-left class=icon></i> Reply</button></div></div>')
    body += '</div>'

    body += f'''<div id="reply-preview" style="display:none;background:#EDF3EC;
            border-left:4px solid #5C7A5A;padding:0.4rem 0.8rem;border-radius:4px;
            margin-bottom:0.5rem;font-size:0.85rem;">
        <span id="reply-preview-text"></span>
        <button type="button" onclick="clearReply()" class="btn-link"
                style="float:right;">✕</button>
    </div>
    <form method="POST" action="/messages/conversation?with={other_id}"
          style="display:flex;gap:0.5rem;align-items:flex-end;">
        {ctx.csrf_input()}
        <input type="hidden" name="reply_to" id="reply-to-input" value="">
        <textarea name="content" class="form-control" rows="2" required id="msg-input"
                  placeholder="Schrijf een bericht..." style="flex:1;resize:none;"></textarea>
        <button type="submit" class="btn btn-primary">Verstuur</button>
    </form>
    <script>
    function setReply(btn) {{
        document.getElementById('reply-to-input').value = btn.dataset.replyId;
        document.getElementById('reply-preview-text').textContent =
            btn.dataset.replyFrom + ': ' + btn.dataset.replyText;
        document.getElementById('reply-preview').style.display = 'block';
        document.getElementById('msg-input').focus();
    }}
    function clearReply() {{
        document.getElementById('reply-to-input').value = '';
        document.getElementById('reply-preview').style.display = 'none';
    }}
    document.getElementById('msg-input').addEventListener('keydown', function(e) {{
        if (e.key === 'Enter' && !e.shiftKey) {{ e.preventDefault(); this.form.submit(); }}
    }});
    var box = document.getElementById('chat-box');
    if (box) box.scrollTop = box.scrollHeight;
    </script>'''
    body += page_footer()
    ctx.html(body)
