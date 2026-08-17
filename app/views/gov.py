"""Governance: fase-board, personen, projectkaarten en voortgang."""

from __future__ import annotations

import html

from .. import config
from ..db import connect, log_action, query_all, query_one
from ..ui import (gov_nav, gov_phase_color, gov_phase_label, gov_tag_pills,
                  page_footer, page_header, post_button, stat_card)

PHASES = config.GOV_PHASES
NOTE_TYPES = ['coaching', 'intervisie', 'aandachtspunt']


# ── Shared progress helpers ───────────────────────────────────────────────
def _load_structure(conn):
    cards = conn.execute('SELECT * FROM governance_card_templates '
                         'ORDER BY order_index ASC, id ASC').fetchall()
    items = conn.execute('SELECT * FROM governance_card_items '
                         'ORDER BY order_index ASC, id ASC').fetchall()
    items_by_card = {}
    for item in items:
        items_by_card.setdefault(item['card_id'], []).append(item)
    cards_by_phase = {}
    for card in cards:
        cards_by_phase.setdefault(card['phase'], []).append(card)
    return cards_by_phase, items_by_card


def _relevant_cards(project_type: str, cards_by_phase, phase: str = None):
    """Cards that apply to a person, honouring their project type."""
    phases = [phase] if phase else PHASES
    result = []
    for ph in phases:
        for card in cards_by_phase.get(ph, []):
            if project_type and card['project_type'] and card['project_type'] != project_type:
                continue
            result.append(card)
    return result


def _relevant_item_ids(project_type: str, cards_by_phase, items_by_card) -> set:
    ids = set()
    for card in _relevant_cards(project_type, cards_by_phase):
        for item in items_by_card.get(card['id'], []):
            ids.add(item['id'])
    return ids


# ── Board ─────────────────────────────────────────────────────────────────
def board(ctx) -> None:
    with connect(readonly=True) as conn:
        persons = conn.execute('SELECT * FROM governance_persons ORDER BY name ASC').fetchall()
        cards_by_phase, items_by_card = _load_structure(conn)
        completed = {}
        for row in conn.execute('SELECT person_id, item_id FROM governance_progress'):
            completed.setdefault(row['person_id'], set()).add(row['item_id'])

    progress, totals = {}, {}
    for person in persons:
        relevant = _relevant_item_ids((person['project_type'] or '').lower(),
                                      cards_by_phase, items_by_card)
        totals[person['id']] = len(relevant)
        progress[person['id']] = len(completed.get(person['id'], set()) & relevant)

    by_phase = {phase: [] for phase in PHASES}
    for person in persons:
        by_phase[person['phase'] if person['phase'] in by_phase else 'startpunt'].append(person)

    body = page_header('Governance Board', ctx)
    body += '<h2 class="mt-4"><i data-lucide=shield class=icon></i> Governance Dashboard</h2>'
    body += gov_nav('board', ctx)

    body += '<div class="stat-row">'
    body += stat_card(len(persons), 'Totaal personen')
    for phase in PHASES:
        if by_phase[phase]:
            body += stat_card(len(by_phase[phase]), gov_phase_label(phase), gov_phase_color(phase))
    body += '</div>'

    phase_options = ''.join(f'<option value="{p}">{gov_phase_label(p)}</option>' for p in PHASES)
    type_options = ''.join(f'<option value="{t}">{t.capitalize()}</option>'
                           for t in config.GOV_PROJECT_TYPES)
    body += f'''<div class="card" style="margin-bottom:0.75rem;">
        <div class="section-title">+ Persoon toevoegen</div>
        <form method="POST" action="/gov/persons/add"
              style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
            {ctx.csrf_input()}
            <div><label class="form-label">Naam</label>
                <input type="text" name="name" class="form-control" required style="min-width:160px;"></div>
            <div><label class="form-label">Tags</label>
                <input type="text" name="tags" class="form-control" placeholder="komma-gescheiden"
                       style="min-width:140px;"></div>
            <div><label class="form-label">Fase</label>
                <select name="phase" class="form-select">{phase_options}</select></div>
            <div><label class="form-label">Projecttype</label>
                <select name="project_type" class="form-select">{type_options}</select></div>
            <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
        </form></div>'''

    body += ('<div style="overflow-x:auto;margin:0 -1rem;padding:0 1rem 1rem;">'
             '<div style="display:flex;gap:1rem;width:max-content;padding-bottom:0.5rem;">')
    type_colors = {'communicatie': '#5C7A5A', 'werkveld': '#5C7A5A',
                   'evenementen': '#7A6E66', 'onderwijs': '#7A8FA6'}
    for phase in PHASES:
        color = gov_phase_color(phase)
        people = by_phase[phase]
        body += (f'<div class="gov-column" data-phase="{phase}" style="width:250px;flex:0 0 250px;'
                 f'background:#F7F4F0;border-radius:8px;padding:0.75rem;border-top:4px solid {color};">'
                 f'<div style="font-weight:bold;color:{color};font-size:0.95rem;margin-bottom:0.65rem;">'
                 f'{gov_phase_label(phase)} <span style="background:{color};color:#fff;'
                 f'border-radius:10px;padding:0.05rem 0.5rem;font-size:0.75rem;">{len(people)}</span></div>')
        for person in people:
            done = progress.get(person['id'], 0)
            total = totals.get(person['id'], 0)
            pct = round(done / total * 100) if total else 0
            ptype = person['project_type'] or ''
            type_badge = (f'<span style="font-size:0.68rem;background:{type_colors.get(ptype, "#888")};'
                          f'color:#fff;border-radius:3px;padding:0.05rem 0.3rem;margin-right:0.2rem;">'
                          f'{html.escape(ptype.capitalize())}</span>') if ptype else ''
            move = ''
            if phase != PHASES[-1]:
                nxt = PHASES[PHASES.index(phase) + 1]
                move += post_button('/gov/persons/move', ctx, '→', css='btn btn-sm btn-secondary',
                                    style='font-size:0.7rem;', title=f'Naar {gov_phase_label(nxt)}',
                                    fields={'id': person['id'], 'phase': nxt})
            if phase != PHASES[0]:
                prv = PHASES[PHASES.index(phase) - 1]
                move = post_button('/gov/persons/move', ctx, '←', css='btn btn-sm btn-secondary',
                                   style='font-size:0.7rem;', title=f'Naar {gov_phase_label(prv)}',
                                   fields={'id': person['id'], 'phase': prv}) + ' ' + move
            remove = post_button('/gov/persons/delete', ctx,
                                 '<i data-lucide=trash-2 class=icon></i>',
                                 confirm='Persoon verwijderen?', css='btn-link',
                                 style='color:#C0392B;font-size:0.75rem;',
                                 fields={'id': person['id']})
            body += (f'<div style="background:#fff;border-radius:6px;padding:0.5rem 0.6rem;'
                     f'margin-bottom:0.5rem;box-shadow:0 1px 3px rgba(0,0,0,0.1);">'
                     f'<div style="display:flex;justify-content:space-between;align-items:flex-start;">'
                     f'<a href="/gov/person?id={person["id"]}" style="font-weight:bold;'
                     f'color:#7A8FA6;font-size:0.9rem;">{html.escape(person["name"])}</a></div>'
                     f'<div style="margin-top:0.2rem;">{type_badge}'
                     f'{gov_tag_pills(person["tags"] or "")}</div>'
                     f'<div style="margin-top:0.35rem;">'
                     f'<div style="height:5px;background:#EDE8E3;border-radius:3px;overflow:hidden;">'
                     f'<div style="height:100%;width:{pct}%;background:{color};"></div></div>'
                     f'<div style="font-size:0.7rem;color:#B0A49A;margin-top:0.1rem;">'
                     f'{done}/{total} items ({pct}%)</div></div>'
                     f'<div style="margin-top:0.35rem;display:flex;gap:0.3rem;align-items:center;">'
                     f'<a href="/gov/persons/edit?id={person["id"]}" '
                     f'style="font-size:0.75rem;color:#7A6E66;">'
                     f'<i data-lucide=pencil class=icon></i></a>{move}{remove}</div></div>')
        body += '</div>'
    body += '</div></div>'
    body += page_footer()
    ctx.html(body)


# ── Person detail ─────────────────────────────────────────────────────────
def person(ctx) -> None:
    person_id = ctx.qint('id')
    if not person_id:
        ctx.redirect('/gov/board')
        return
    with connect(readonly=True) as conn:
        row = conn.execute('SELECT * FROM governance_persons WHERE id = ?', (person_id,)).fetchone()
        if not row:
            ctx.not_found()
            return
        cards_by_phase, items_by_card = _load_structure(conn)
        notes = {r['item_id']: (r['note'] or '') for r in conn.execute(
            'SELECT item_id, note FROM governance_progress WHERE person_id = ?', (person_id,))}

    completed = set(notes)
    project_type = (row['project_type'] or '').lower()
    relevant = _relevant_item_ids(project_type, cards_by_phase, items_by_card)
    total, done = len(relevant), len(completed & relevant)
    pct = round(done / total * 100) if total else 0

    body = page_header(f'Gov: {row["name"]}', ctx)
    body += gov_nav('profiles', ctx)
    body += f'<h2 class="mt-4"><i data-lucide=user class=icon></i> {html.escape(row["name"])}</h2>'
    body += (f'<div class="card"><div style="display:flex;align-items:center;gap:1rem;'
             f'flex-wrap:wrap;">'
             f'<span style="background:{gov_phase_color(row["phase"])};color:#fff;'
             f'border-radius:12px;padding:0.2rem 0.7rem;font-size:0.9rem;">'
             f'{gov_phase_label(row["phase"])}</span>'
             f'<div>{gov_tag_pills(row["tags"] or "")}</div>'
             f'<a href="/gov/persons/edit?id={person_id}" class="btn btn-sm btn-secondary">'
             f'<i data-lucide=pencil class=icon></i> Bewerken</a></div>')
    if row['notes']:
        body += (f'<div style="margin-top:0.5rem;color:#7A6E66;font-size:0.9rem;">'
                 f'{html.escape(row["notes"])}</div>')
    body += (f'<div style="margin-top:0.75rem;"><div style="font-size:0.85rem;color:#7A6E66;'
             f'margin-bottom:0.25rem;">Totale voortgang: {done}/{total} items ({pct}%)</div>'
             f'<div style="height:8px;background:#EDE8E3;border-radius:4px;overflow:hidden;">'
             f'<div style="height:100%;width:{pct}%;background:#7A8FA6;"></div></div></div></div>')

    for phase in PHASES:
        cards = _relevant_cards(project_type, cards_by_phase, phase)
        if not cards:
            continue
        color = gov_phase_color(phase)
        body += (f'<h3 style="color:{color};margin-top:1rem;margin-bottom:0.5rem;">'
                 f'{gov_phase_label(phase)}</h3>')
        for card in cards:
            items = items_by_card.get(card['id'], [])
            card_done = sum(1 for i in items if i['id'] in completed)
            card_pct = round(card_done / len(items) * 100) if items else 0
            body += (f'<div class="card" style="margin-bottom:0.5rem;">'
                     f'<div style="display:flex;justify-content:space-between;align-items:center;'
                     f'margin-bottom:0.35rem;"><strong>{html.escape(card["title"])}</strong>'
                     f'<span style="font-size:0.8rem;color:#B0A49A;">{card_done}/{len(items)}</span></div>')
            if card['description']:
                body += (f'<div style="font-size:0.8rem;color:#7A6E66;margin-bottom:0.4rem;">'
                         f'{html.escape(card["description"])}</div>')
            body += (f'<div style="height:5px;background:#EDE8E3;border-radius:3px;'
                     f'overflow:hidden;margin-bottom:0.5rem;">'
                     f'<div style="height:100%;width:{card_pct}%;background:{color};"></div></div>')
            for item in items:
                body += _item_row(ctx, item, person_id, item['id'] in completed,
                                  notes.get(item['id'], ''), color)
            body += '</div>'

    body += '''<script>
    function govToggle(id, prefix) {
        var el = document.getElementById(prefix + '-' + id);
        el.style.display = el.style.display === 'none' ? 'block' : 'none';
    }
    </script>'''
    body += page_footer()
    ctx.html(body)


def _item_row(ctx, item, person_id: int, checked: bool, note: str, color: str) -> str:
    info = ''
    if item['norm'] or item['middelen']:
        inner = ''
        if item['norm']:
            inner += f'<div style="margin-bottom:0.3rem;"><strong>Norm:</strong> {html.escape(item["norm"])}</div>'
        if item['middelen']:
            inner += f'<div><strong>Middelen:</strong> {html.escape(item["middelen"])}</div>'
        info = (f'<details style="display:inline-block;margin-left:0.3rem;">'
                f'<summary style="cursor:pointer;font-size:0.72rem;color:#7A8FA6;">info</summary>'
                f'<div style="background:#EDF3EC;border-radius:4px;padding:0.4rem 0.6rem;'
                f'margin-top:0.3rem;font-size:0.78rem;max-width:480px;">{inner}</div></details>')

    if checked:
        box = post_button('/gov/progress/toggle', ctx,
                          '<i data-lucide=check class=icon></i>', css='btn-link',
                          style=(f'width:18px;height:18px;border:2px solid {color};border-radius:3px;'
                                 f'background:{color};color:#fff;padding:0;line-height:1;'),
                          fields={'person_id': person_id, 'item_id': item['id'],
                                  'redirect': f'/gov/person?id={person_id}'})
    else:
        box = (f'<span onclick="govToggle({item["id"]}, \'gnote\')" '
               f'style="flex-shrink:0;cursor:pointer;display:inline-block;width:18px;height:18px;'
               f'border:2px solid {color};border-radius:3px;background:#fff;"></span>')

    strike = 'text-decoration:line-through;color:#B0A49A;' if checked else ''
    note_html = (f'<div style="font-size:0.75rem;color:#7A8FA6;background:#EDF3EC;'
                 f'border-radius:3px;padding:0.15rem 0.4rem;margin-top:0.15rem;'
                 f'display:inline-block;">{html.escape(note)}</div>') if note else ''

    note_form = ''
    if not checked:
        note_form = f'''<div id="gnote-{item['id']}" style="display:none;background:#FEF8F0;
                border-radius:4px;padding:0.5rem;margin-top:0.25rem;">
            <form method="POST" action="/gov/progress/complete">
                {ctx.csrf_input()}
                <input type="hidden" name="person_id" value="{person_id}">
                <input type="hidden" name="item_id" value="{item['id']}">
                <input type="hidden" name="redirect" value="/gov/person?id={person_id}">
                <textarea name="note" class="form-control" rows="2"
                          placeholder="Notitie bij afronding (optioneel)..."></textarea>
                <button type="submit" class="btn btn-sm btn-primary mt-2">
                    <i data-lucide=check class=icon></i> Afronden</button>
                <button type="button" class="btn btn-sm btn-secondary mt-2"
                        onclick="govToggle({item['id']}, 'gnote')">Annuleren</button>
            </form></div>'''

    edit_form = f'''<div id="gedit-{item['id']}" style="display:none;background:#F7F4F0;
            border-radius:4px;padding:0.5rem;margin-top:0.3rem;">
        <form method="POST" action="/gov/items/quick-edit">
            {ctx.csrf_input()}
            <input type="hidden" name="item_id" value="{item['id']}">
            <input type="hidden" name="person_id" value="{person_id}">
            <input type="text" name="title" value="{html.escape(item['title'])}"
                   class="form-control mb-3" required placeholder="Titel">
            <input type="text" name="description" value="{html.escape(item['description'] or '')}"
                   class="form-control mb-3" placeholder="Beschrijving">
            <textarea name="norm" class="form-control mb-3" rows="2"
                      placeholder="Norm">{html.escape(item['norm'] or '')}</textarea>
            <textarea name="middelen" class="form-control mb-3" rows="2"
                      placeholder="Middelen">{html.escape(item['middelen'] or '')}</textarea>
            <button type="submit" class="btn btn-sm btn-primary">Opslaan</button>
            <button type="button" class="btn btn-sm btn-secondary"
                    onclick="govToggle({item['id']}, 'gedit')">Annuleren</button>
        </form></div>'''

    return (f'<div style="margin-bottom:0.45rem;">'
            f'<div style="display:flex;align-items:flex-start;gap:0.4rem;">{box}'
            f'<div style="flex:1;"><span style="font-size:0.9rem;{strike}">'
            f'{html.escape(item["title"])}</span>{info}'
            f'<button type="button" onclick="govToggle({item["id"]}, \'gedit\')" '
            f'style="background:none;border:none;cursor:pointer;font-size:0.72rem;color:#bbb;">'
            f'<i data-lucide=pencil class=icon></i></button>'
            + (f'<div style="font-size:0.75rem;color:#B0A49A;">'
               f'{html.escape(item["description"])}</div>' if item['description'] else '')
            + f'{note_html}{edit_form}{note_form}</div></div></div>')


# ── Person CRUD ───────────────────────────────────────────────────────────
def person_add(ctx) -> None:
    name = ctx.f('name')
    if name:
        with connect() as conn:
            cur = conn.execute(
                'INSERT INTO governance_persons (name, phase, tags, notes, created_by, project_type) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (name, ctx.choice(ctx.f('phase', 'startpunt'), PHASES, 'startpunt'),
                 ctx.f('tags') or None, ctx.f('notes') or None, ctx.user_id,
                 ctx.choice(ctx.f('project_type'), config.GOV_PROJECT_TYPES, None)))
            log_action(ctx.user_id, 'create', 'governance_persons', cur.lastrowid, name)
    ctx.redirect('/gov/board')


def person_edit(ctx) -> None:
    person_id = ctx.qint('id')
    if not person_id:
        ctx.redirect('/gov/board')
        return
    if ctx.method == 'POST':
        name = ctx.f('name')
        if name:
            with connect() as conn:
                conn.execute(
                    'UPDATE governance_persons SET name=?, phase=?, tags=?, notes=?, '
                    ' project_type=? WHERE id=?',
                    (name, ctx.choice(ctx.f('phase', 'startpunt'), PHASES, 'startpunt'),
                     ctx.f('tags') or None, ctx.f('notes') or None,
                     ctx.choice(ctx.f('project_type'), config.GOV_PROJECT_TYPES, None), person_id))
        ctx.redirect('/gov/board')
        return

    row = query_one('SELECT * FROM governance_persons WHERE id = ?', (person_id,))
    if not row:
        ctx.not_found()
        return
    phase_options = ''.join(
        f'<option value="{p}"{" selected" if row["phase"] == p else ""}>{gov_phase_label(p)}</option>'
        for p in PHASES)
    current_type = (row['project_type'] or '').lower()
    type_options = '<option value="">— Geen —</option>' + ''.join(
        f'<option value="{t}"{" selected" if current_type == t else ""}>{t.capitalize()}</option>'
        for t in config.GOV_PROJECT_TYPES)

    body = page_header('Persoon bewerken', ctx)
    body += gov_nav('board', ctx)
    body += '<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Persoon bewerken</h2>'
    body += f'''<div class="card" style="max-width:560px;">
        <form method="POST" action="/gov/persons/edit?id={person_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Naam</label>
                <input type="text" name="name" value="{html.escape(row['name'])}"
                       class="form-control" required></div>
            <div class="mb-3"><label class="form-label">Fase</label>
                <select name="phase" class="form-select">{phase_options}</select></div>
            <div class="mb-3"><label class="form-label">Projecttype</label>
                <select name="project_type" class="form-select">{type_options}</select></div>
            <div class="mb-3"><label class="form-label">Tags</label>
                <input type="text" name="tags" value="{html.escape(row['tags'] or '')}"
                       class="form-control" placeholder="komma-gescheiden"></div>
            <div class="mb-3"><label class="form-label">Notities</label>
                <textarea name="notes" class="form-control" rows="3">{html.escape(row['notes'] or '')}</textarea></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/gov/board" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def person_delete(ctx) -> None:
    person_id = ctx.fint('id')
    if person_id:
        with connect() as conn:
            conn.execute('DELETE FROM governance_persons WHERE id = ?', (person_id,))
        log_action(ctx.user_id, 'delete', 'governance_persons', person_id)
    ctx.redirect('/gov/board')


def person_move(ctx) -> None:
    person_id = ctx.fint('id')
    phase = ctx.choice(ctx.f('phase'), PHASES, 'startpunt')
    if person_id:
        with connect() as conn:
            conn.execute('UPDATE governance_persons SET phase = ? WHERE id = ?', (phase, person_id))
    ctx.redirect('/gov/board')


# ── Progress ──────────────────────────────────────────────────────────────
def _safe_redirect(ctx, fallback: str) -> str:
    target = ctx.f('redirect') or fallback
    return target if target.startswith('/') and not target.startswith('//') else fallback


def progress_toggle(ctx) -> None:
    person_id = ctx.fint('person_id')
    item_id = ctx.fint('item_id')
    if person_id and item_id:
        with connect() as conn:
            existing = conn.execute(
                'SELECT id FROM governance_progress WHERE person_id = ? AND item_id = ?',
                (person_id, item_id)).fetchone()
            if existing:
                conn.execute('DELETE FROM governance_progress WHERE id = ?', (existing['id'],))
            else:
                conn.execute('INSERT INTO governance_progress (person_id, item_id, completed_by) '
                             'VALUES (?, ?, ?)', (person_id, item_id, ctx.user_id))
    ctx.redirect(_safe_redirect(ctx, f'/gov/person?id={person_id}' if person_id else '/gov/board'))


def progress_complete(ctx) -> None:
    person_id = ctx.fint('person_id')
    item_id = ctx.fint('item_id')
    if person_id and item_id:
        with connect() as conn:
            conn.execute(
                'INSERT OR IGNORE INTO governance_progress '
                '(person_id, item_id, completed_by, note) VALUES (?, ?, ?, ?)',
                (person_id, item_id, ctx.user_id, ctx.f('note') or None))
    ctx.redirect(_safe_redirect(ctx, f'/gov/person?id={person_id}' if person_id else '/gov/board'))


def item_quick_edit(ctx) -> None:
    item_id = ctx.fint('item_id')
    person_id = ctx.fint('person_id')
    title = ctx.f('title')
    if item_id and title:
        with connect() as conn:
            conn.execute('UPDATE governance_card_items SET title=?, description=?, norm=?, '
                         'middelen=? WHERE id=?',
                         (title, ctx.f('description') or None, ctx.f('norm') or None,
                          ctx.f('middelen') or None, item_id))
    ctx.redirect(f'/gov/person?id={person_id}' if person_id else '/gov/board')


# ── Profiles & notes ──────────────────────────────────────────────────────
def profiles(ctx) -> None:
    persons = query_all('SELECT * FROM governance_persons ORDER BY name ASC')
    notes = query_all('SELECT gn.*, u.username AS author FROM governance_notes gn '
                      'LEFT JOIN users u ON gn.created_by = u.id '
                      'ORDER BY gn.created_at DESC')
    by_person = {}
    for note in notes:
        by_person.setdefault(note['person_id'], []).append(note)

    labels = {'coaching': 'Coaching', 'intervisie': 'Intervisie', 'aandachtspunt': 'Aandachtspunt'}
    colors = {'coaching': '#7A8FA6', 'intervisie': '#7A6E66', 'aandachtspunt': '#B5916A'}
    type_colors = {'communicatie': '#5C7A5A', 'werkveld': '#5C7A5A',
                   'evenementen': '#7A6E66', 'onderwijs': '#7A8FA6'}

    body = page_header('Governance Personen', ctx)
    body += '<h2 class="mt-4"><i data-lucide=users class=icon></i> Personen &amp; Profiel</h2>'
    body += gov_nav('profiles', ctx)

    if not persons:
        body += '<div class="card"><p style="color:#B0A49A;">Nog geen personen toegevoegd.</p></div>'

    for row in persons:
        pid = row['id']
        color = gov_phase_color(row['phase'])
        ptype = row['project_type'] or ''
        type_badge = (f'<span style="font-size:0.75rem;background:{type_colors.get(ptype, "#888")};'
                      f'color:#fff;border-radius:3px;padding:0.1rem 0.4rem;margin-left:0.4rem;">'
                      f'{html.escape(ptype.capitalize())}</span>') if ptype else ''
        consent = row['consent_given']
        consent_button = post_button(
            '/gov/profiles/consent', ctx,
            ('<i data-lucide=check-circle class=icon></i> Akkoord gegeven' if consent
             else '<i data-lucide=square class=icon></i> Nog geen akkoord'),
            css='btn btn-sm btn-secondary',
            style=f'color:{"#5C7A5A" if consent else "#888"};', fields={'id': pid})

        person_notes = by_person.get(pid, [])
        attention = [n for n in person_notes if n['note_type'] == 'aandachtspunt']
        others = [n for n in person_notes if n['note_type'] != 'aandachtspunt']

        body += (f'<div class="card" style="border-left:4px solid {color};">'
                 f'<div style="display:flex;justify-content:space-between;align-items:flex-start;'
                 f'flex-wrap:wrap;gap:0.5rem;"><div>'
                 f'<a href="/gov/person?id={pid}" style="font-size:1.1rem;font-weight:bold;'
                 f'color:#7A8FA6;">{html.escape(row["name"])}</a>{type_badge}'
                 f'<span style="font-size:0.8rem;background:{color};color:#fff;border-radius:10px;'
                 f'padding:0.1rem 0.5rem;margin-left:0.4rem;">{gov_phase_label(row["phase"])}</span>'
                 f'</div>{consent_button}</div>')
        if row['notes']:
            body += (f'<div style="font-size:0.85rem;color:#7A6E66;margin-top:0.4rem;'
                     f'font-style:italic;">{html.escape(row["notes"])}</div>')

        if attention:
            body += ('<div style="margin-top:0.6rem;"><div style="font-size:0.85rem;'
                     'font-weight:bold;color:#B5916A;margin-bottom:0.3rem;">'
                     '<i data-lucide=target class=icon></i> Persoonlijke aandachtspunten</div>')
            for note in attention:
                body += _note_block(ctx, note, '#B5916A', '#FEF8F0')
            body += '</div>'

        if others:
            body += (f'<details style="margin-top:0.5rem;"><summary style="cursor:pointer;'
                     f'font-size:0.85rem;color:#7A6E66;">{len(others)} notitie(s)</summary>'
                     f'<div style="margin-top:0.4rem;">')
            for note in others:
                body += _note_block(ctx, note, colors.get(note['note_type'], '#888'), '#fff',
                                    labels.get(note['note_type'], note['note_type']))
            body += '</div></details>'

        type_options = ''.join(f'<option value="{t}">{labels[t]}</option>' for t in NOTE_TYPES)
        body += f'''<details style="margin-top:0.6rem;">
            <summary style="cursor:pointer;font-size:0.85rem;color:#7A8FA6;">
                + Notitie / aandachtspunt toevoegen</summary>
            <div style="margin-top:0.4rem;background:#F7F4F0;border-radius:4px;padding:0.6rem;">
                <form method="POST" action="/gov/notes/add">
                    {ctx.csrf_input()}
                    <input type="hidden" name="person_id" value="{pid}">
                    <div style="display:flex;gap:0.4rem;flex-wrap:wrap;align-items:flex-end;">
                        <div><label class="form-label">Type</label>
                            <select name="note_type" class="form-select">{type_options}</select></div>
                        <div style="flex:1;min-width:220px;"><label class="form-label">Notitie</label>
                            <textarea name="content" class="form-control" rows="2" required></textarea></div>
                        <div><button type="submit" class="btn btn-primary">Opslaan</button></div>
                    </div>
                </form></div></details>'''
        body += '</div>'

    body += page_footer()
    ctx.html(body)


def _note_block(ctx, note, color: str, bg: str, label: str = '') -> str:
    tag = (f'<span style="font-size:0.72rem;background:{color};color:#fff;border-radius:3px;'
           f'padding:0.05rem 0.3rem;margin-right:0.3rem;">{html.escape(label)}</span>'
           ) if label else ''
    remove = post_button('/gov/notes/delete', ctx, '<i data-lucide=trash-2 class=icon></i>',
                         confirm='Verwijderen?', css='btn-link',
                         style='color:#C0392B;font-size:0.75rem;', fields={'id': note['id']})
    return (f'<div style="background:{bg};border-left:3px solid {color};padding:0.4rem 0.6rem;'
            f'margin-bottom:0.3rem;border-radius:0 4px 4px 0;font-size:0.88rem;display:flex;'
            f'justify-content:space-between;align-items:flex-start;gap:0.5rem;">'
            f'<div>{tag}<span>{html.escape(note["content"])}</span>'
            f'<div style="font-size:0.72rem;color:#B0A49A;margin-top:0.15rem;">'
            f'{html.escape(str(note["created_at"] or "")[:10])} — '
            f'{html.escape(note["author"] or "?")}</div></div>{remove}</div>')


def profile_consent(ctx) -> None:
    person_id = ctx.fint('id')
    if person_id:
        with connect() as conn:
            row = conn.execute('SELECT consent_given FROM governance_persons WHERE id = ?',
                               (person_id,)).fetchone()
            if row:
                value = 0 if row['consent_given'] else 1
                conn.execute('UPDATE governance_persons SET consent_given = ? WHERE id = ?',
                             (value, person_id))
                log_action(ctx.user_id, 'update', 'governance_persons', person_id,
                           f'consent_given={value}')
    ctx.redirect('/gov/profiles')


def note_add(ctx) -> None:
    person_id = ctx.fint('person_id')
    content = ctx.f('content')
    if person_id and content:
        with connect() as conn:
            conn.execute('INSERT INTO governance_notes (person_id, note_type, content, created_by) '
                         'VALUES (?, ?, ?, ?)',
                         (person_id, ctx.choice(ctx.f('note_type', 'coaching'),
                                                NOTE_TYPES, 'coaching'), content, ctx.user_id))
    ctx.redirect('/gov/profiles')


def note_delete(ctx) -> None:
    note_id = ctx.fint('id')
    if note_id:
        with connect() as conn:
            conn.execute('DELETE FROM governance_notes WHERE id = ?', (note_id,))
        log_action(ctx.user_id, 'delete', 'governance_notes', note_id)
    ctx.redirect('/gov/profiles')


# ── Overview ──────────────────────────────────────────────────────────────
def overview(ctx) -> None:
    with connect(readonly=True) as conn:
        persons = conn.execute('SELECT * FROM governance_persons ORDER BY name ASC').fetchall()
        cards_by_phase, items_by_card = _load_structure(conn)
        completed = {}
        for row in conn.execute('SELECT person_id, item_id FROM governance_progress'):
            completed.setdefault(row['person_id'], set()).add(row['item_id'])

    # The old version divided every person's completed count by the total number
    # of card items in the database, while the board and detail pages divided by
    # the items that actually apply to that person's project type. The overview
    # therefore under-reported everyone. Same basis everywhere now.
    stats = []
    for row in persons:
        relevant = _relevant_item_ids((row['project_type'] or '').lower(),
                                      cards_by_phase, items_by_card)
        done = len(completed.get(row['id'], set()) & relevant)
        total = len(relevant)
        stats.append((row, done, total, round(done / total * 100) if total else 0))

    phase_counts = {phase: 0 for phase in PHASES}
    for row in persons:
        phase_counts[row['phase'] if row['phase'] in phase_counts else 'startpunt'] += 1
    average = round(sum(s[3] for s in stats) / len(stats)) if stats else 0

    body = page_header('Governance Overzicht', ctx)
    body += '<h2 class="mt-4"><i data-lucide=trending-up class=icon></i> Governance Overzicht</h2>'
    body += gov_nav('overview', ctx)

    body += '<div class="card"><div class="section-title">Faseverdeling</div>'
    body += '<div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:0.5rem;">'
    for phase in PHASES:
        count = phase_counts[phase]
        color = gov_phase_color(phase)
        pct = round(count / len(persons) * 100) if persons else 0
        body += (f'<div style="flex:1;min-width:90px;text-align:center;">'
                 f'<div style="font-size:1.2rem;font-weight:bold;color:{color};">{count}</div>'
                 f'<div style="font-size:0.75rem;color:#7A6E66;">{gov_phase_label(phase)}</div>'
                 f'<div style="height:6px;background:#EDE8E3;border-radius:3px;margin-top:0.2rem;'
                 f'overflow:hidden;"><div style="height:100%;width:{pct}%;background:{color};">'
                 f'</div></div></div>')
    body += (f'</div><div style="font-size:0.85rem;color:#7A6E66;">Gemiddelde afronding: '
             f'<strong>{average}%</strong></div></div>')

    body += ('<div class="card"><div class="section-title">Alle personen</div>'
             '<div class="table-wrap"><table><thead><tr><th>Naam</th><th>Fase</th>'
             '<th>Voortgang</th><th>% Klaar</th><th>Tags</th></tr></thead><tbody>')
    for row, done, total, pct in stats:
        color = gov_phase_color(row['phase'])
        body += (f'<tr><td><a href="/gov/person?id={row["id"]}">{html.escape(row["name"])}</a></td>'
                 f'<td><span style="background:{color};color:#fff;border-radius:10px;'
                 f'padding:0.1rem 0.5rem;font-size:0.8rem;">{gov_phase_label(row["phase"])}</span></td>'
                 f'<td style="min-width:120px;">'
                 f'<div style="height:8px;background:#EDE8E3;border-radius:4px;overflow:hidden;">'
                 f'<div style="height:100%;width:{pct}%;background:{color};"></div></div>'
                 f'<small style="color:#B0A49A;">{done}/{total}</small></td>'
                 f'<td><strong>{pct}%</strong></td>'
                 f'<td>{gov_tag_pills(row["tags"] or "")}</td></tr>')
    body += '</tbody></table></div></div>'
    body += page_footer()
    ctx.html(body)


# ── Card management (admin) ───────────────────────────────────────────────
def cards(ctx) -> None:
    with connect(readonly=True) as conn:
        cards_by_phase, items_by_card = _load_structure(conn)

    body = page_header('Governance Kaartbeheer', ctx)
    body += '<h2 class="mt-4"><i data-lucide=settings class=icon></i> Kaartbeheer</h2>'
    body += gov_nav('cards', ctx)

    phase_options = ''.join(f'<option value="{p}">{gov_phase_label(p)}</option>' for p in PHASES)
    type_options = '<option value="">Alle typen</option>' + ''.join(
        f'<option value="{t}">{t.capitalize()}</option>' for t in config.GOV_PROJECT_TYPES)
    body += f'''<div class="card">
        <div class="section-title">Kaart toevoegen</div>
        <form method="POST" action="/gov/cards/add"
              style="display:flex;gap:0.5rem;flex-wrap:wrap;align-items:flex-end;">
            {ctx.csrf_input()}
            <div><label class="form-label">Titel</label>
                <input type="text" name="title" class="form-control" required style="min-width:180px;"></div>
            <div><label class="form-label">Fase</label>
                <select name="phase" class="form-select">{phase_options}</select></div>
            <div><label class="form-label">Projecttype</label>
                <select name="project_type" class="form-select">{type_options}</select></div>
            <div><label class="form-label">Beschrijving</label>
                <input type="text" name="description" class="form-control" style="min-width:200px;"></div>
            <div><label class="form-label">Volgorde</label>
                <input type="number" name="order_index" value="0" class="form-control" style="width:80px;"></div>
            <div><button type="submit" class="btn btn-primary">Toevoegen</button></div>
        </form></div>'''

    type_colors = {'communicatie': '#5C7A5A', 'werkveld': '#5C7A5A',
                   'evenementen': '#7A6E66', 'onderwijs': '#7A8FA6'}
    for phase in PHASES:
        phase_cards = cards_by_phase.get(phase, [])
        if not phase_cards:
            continue
        color = gov_phase_color(phase)
        body += f'<h3 style="color:{color};margin-top:1rem;">{gov_phase_label(phase)}</h3>'
        for card in phase_cards:
            items = items_by_card.get(card['id'], [])
            ptype = card['project_type'] or ''
            badge = (f'<span style="font-size:0.72rem;background:{type_colors.get(ptype, "#888")};'
                     f'color:#fff;border-radius:3px;padding:0.05rem 0.35rem;margin-left:0.4rem;">'
                     f'{html.escape(ptype.capitalize()) if ptype else "Alle typen"}</span>')
            body += (f'<div class="card" style="border-left:4px solid {color};">'
                     f'<div style="display:flex;justify-content:space-between;align-items:center;'
                     f'flex-wrap:wrap;gap:0.5rem;"><div><strong>{html.escape(card["title"])}</strong>'
                     f'{badge}</div><div style="display:flex;gap:0.3rem;align-items:center;">'
                     f'<a href="/gov/cards/edit?id={card["id"]}" class="btn btn-sm btn-secondary">'
                     f'<i data-lucide=pencil class=icon></i> Bewerken</a>'
                     + post_button('/gov/cards/delete', ctx,
                                   '<i data-lucide=trash-2 class=icon></i>',
                                   confirm='Kaart verwijderen? Alle items verdwijnen mee.',
                                   fields={'id': card['id']}) + '</div></div>')
            if card['description']:
                body += (f'<div style="font-size:0.85rem;color:#7A6E66;margin-top:0.2rem;">'
                         f'{html.escape(card["description"])}</div>')
            body += (f'<div style="font-size:0.75rem;color:#B0A49A;">Volgorde: '
                     f'{card["order_index"]}</div>')
            if items:
                body += '<ul style="margin:0.5rem 0 0.3rem 1.2rem;padding:0;">'
                for item in items:
                    desc = (f'<span style="font-size:0.8rem;color:#B0A49A;"> — '
                            f'{html.escape(item["description"])}</span>'
                            ) if item['description'] else ''
                    body += (f'<li style="margin-bottom:0.2rem;font-size:0.9rem;'
                             f'display:flex;align-items:center;gap:0.4rem;">'
                             f'<span style="flex:1;">{html.escape(item["title"])}{desc}</span>'
                             + post_button('/gov/items/delete', ctx,
                                           '<i data-lucide=trash-2 class=icon></i>',
                                           confirm='Item verwijderen?', css='btn-link',
                                           style='color:#C0392B;font-size:0.75rem;',
                                           fields={'id': item['id']}) + '</li>')
                body += '</ul>'
            body += f'''<form method="POST" action="/gov/items/add"
                    style="display:flex;gap:0.4rem;flex-wrap:wrap;align-items:flex-end;margin-top:0.5rem;">
                {ctx.csrf_input()}
                <input type="hidden" name="card_id" value="{card['id']}">
                <div><input type="text" name="title" class="form-control" placeholder="Item titel"
                            required style="min-width:160px;"></div>
                <div><input type="text" name="description" class="form-control"
                            placeholder="Beschrijving (optioneel)" style="min-width:160px;"></div>
                <div><input type="number" name="order_index" value="0" class="form-control"
                            style="width:75px;"></div>
                <div><button type="submit" class="btn btn-sm btn-primary">+ Item</button></div>
            </form></div>'''

    body += page_footer()
    ctx.html(body)


def card_add(ctx) -> None:
    title = ctx.f('title')
    if title:
        with connect() as conn:
            conn.execute(
                'INSERT INTO governance_card_templates (phase, title, description, order_index, '
                ' project_type) VALUES (?, ?, ?, ?, ?)',
                (ctx.choice(ctx.f('phase', 'startpunt'), PHASES, 'startpunt'), title,
                 ctx.f('description') or None, ctx.fint('order_index', 0) or 0,
                 ctx.choice(ctx.f('project_type').lower(), config.GOV_PROJECT_TYPES, None)))
    ctx.redirect('/gov/cards')


def card_edit(ctx) -> None:
    card_id = ctx.qint('id')
    if not card_id:
        ctx.redirect('/gov/cards')
        return
    if ctx.method == 'POST':
        title = ctx.f('title')
        if title:
            with connect() as conn:
                conn.execute(
                    'UPDATE governance_card_templates SET phase=?, title=?, description=?, '
                    ' order_index=?, project_type=? WHERE id=?',
                    (ctx.choice(ctx.f('phase', 'startpunt'), PHASES, 'startpunt'), title,
                     ctx.f('description') or None, ctx.fint('order_index', 0) or 0,
                     ctx.choice(ctx.f('project_type').lower(), config.GOV_PROJECT_TYPES, None),
                     card_id))
        ctx.redirect('/gov/cards')
        return

    card = query_one('SELECT * FROM governance_card_templates WHERE id = ?', (card_id,))
    if not card:
        ctx.redirect('/gov/cards')
        return
    phase_options = ''.join(
        f'<option value="{p}"{" selected" if card["phase"] == p else ""}>{gov_phase_label(p)}</option>'
        for p in PHASES)
    current = card['project_type'] or ''
    type_options = (f'<option value=""{" selected" if not current else ""}>Alle typen</option>'
                    + ''.join(f'<option value="{t}"{" selected" if current == t else ""}>'
                              f'{t.capitalize()}</option>' for t in config.GOV_PROJECT_TYPES))

    body = page_header('Kaart bewerken', ctx)
    body += gov_nav('cards', ctx)
    body += '<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Kaart bewerken</h2>'
    body += f'''<div class="card" style="max-width:560px;">
        <form method="POST" action="/gov/cards/edit?id={card_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Titel</label>
                <input type="text" name="title" value="{html.escape(card['title'])}"
                       class="form-control" required></div>
            <div class="mb-3"><label class="form-label">Fase</label>
                <select name="phase" class="form-select">{phase_options}</select></div>
            <div class="mb-3"><label class="form-label">Projecttype</label>
                <select name="project_type" class="form-select">{type_options}</select></div>
            <div class="mb-3"><label class="form-label">Beschrijving</label>
                <textarea name="description" class="form-control" rows="2">{html.escape(card['description'] or '')}</textarea></div>
            <div class="mb-3"><label class="form-label">Volgorde</label>
                <input type="number" name="order_index" value="{card['order_index']}"
                       class="form-control"></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/gov/cards" class="btn btn-secondary">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def card_delete(ctx) -> None:
    card_id = ctx.fint('id')
    if card_id:
        with connect() as conn:
            conn.execute('DELETE FROM governance_card_templates WHERE id = ?', (card_id,))
        log_action(ctx.user_id, 'delete', 'governance_card_templates', card_id)
    ctx.redirect('/gov/cards')


def item_add(ctx) -> None:
    card_id = ctx.fint('card_id')
    title = ctx.f('title')
    if card_id and title:
        with connect() as conn:
            conn.execute('INSERT INTO governance_card_items (card_id, title, description, '
                         'order_index) VALUES (?, ?, ?, ?)',
                         (card_id, title, ctx.f('description') or None,
                          ctx.fint('order_index', 0) or 0))
    ctx.redirect('/gov/cards')


def item_delete(ctx) -> None:
    item_id = ctx.fint('id')
    if item_id:
        with connect() as conn:
            conn.execute('DELETE FROM governance_card_items WHERE id = ?', (item_id,))
        log_action(ctx.user_id, 'delete', 'governance_card_items', item_id)
    ctx.redirect('/gov/cards')
