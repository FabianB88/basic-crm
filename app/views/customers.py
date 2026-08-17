"""Customers, their notes and their interactions."""

from __future__ import annotations

import csv
import datetime
import html
import io
import json
import urllib.parse
from typing import Any, Dict, Optional

from .. import config, reminders
from ..db import (all_users, connect, get_custom_field_definitions, get_customer,
                  get_linked_user_ids, get_user_by_id, log_action, query_all)
from ..permissions import (can_delete_customer, can_edit_customer, can_manage_interaction,
                           can_manage_note)
from ..ui import alert, page_footer, page_header, post_button

SORT_COLUMNS = ('name', 'company', 'category', 'relation_type', 'created_at', 'role', 'verbinding')
INTERNAL_ROLES = ['Docent', 'Onderzoeker', 'Manager', 'Werknemer', 'Ondersteuner',
                  'Partnerdesk', 'Verbindingspersoon']
INTERACTION_LABELS = {'call': 'Bellen', 'email': 'E-mail', 'message': 'Bericht', 'meeting': 'Meeting'}


# ── List ──────────────────────────────────────────────────────────────────
def index(ctx) -> None:
    search = ctx.q('q')
    relation = ctx.choice(ctx.q('relatie'), config.RELATION_TYPES, '')
    verbinding = ctx.choice(ctx.q('verbinding'), config.VERBINDING_VALUES, '')
    sort_col = ctx.choice(ctx.q('sort', 'name'), SORT_COLUMNS, 'name')
    sort_dir = ctx.choice(ctx.q('dir', 'asc'), ('asc', 'desc'), 'asc')

    conditions, args = [], []
    if search:
        like = f'%{search}%'
        conditions.append('(name LIKE ? OR email LIKE ? OR company LIKE ? '
                          'OR tags LIKE ? OR role LIKE ? OR verbinding LIKE ?)')
        args.extend([like] * 6)
    if relation:
        conditions.append('relation_type = ?')
        args.append(relation)
    if verbinding:
        conditions.append('verbinding = ?')
        args.append(verbinding)

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    order = sort_col if sort_col == 'created_at' else f"LOWER(COALESCE({sort_col},''))"
    direction = 'ASC' if sort_dir == 'asc' else 'DESC'

    with connect(readonly=True) as conn:
        rows = conn.execute(
            f'SELECT * FROM customers {where} ORDER BY {order} {direction}', args
        ).fetchall()
        users = conn.execute('SELECT id, username FROM users ORDER BY username ASC').fetchall()
        managers: Dict[int, list] = {}
        for link in conn.execute('SELECT cu.customer_id, u.username FROM customer_users cu '
                                 'JOIN users u ON cu.user_id = u.id'):
            managers.setdefault(link['customer_id'], []).append(link['username'])

    heading = {'extern': 'Klanten Extern', 'intern': 'Klanten Intern'}.get(relation, 'Klanten')
    body = page_header(heading, ctx)
    body += f'<h2 class="mt-4">{heading}</h2>'
    body += _filter_bar(search, relation, verbinding, sort_col, sort_dir)

    if ctx.is_admin:
        options = '<option value="">-- Kies gebruiker --</option>' + ''.join(
            f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in users)
        body += f'''<div class="card" style="padding:0.5rem 1rem;display:flex;align-items:center;
                        gap:0.5rem;flex-wrap:wrap;">
            <span style="font-size:0.85rem;color:#7A6E66;"><i data-lucide=link-2 class=icon></i>
                Koppel relaties zonder accountmanager aan:</span>
            <form method="POST" action="/customers/bulk-link-empty"
                  style="display:flex;gap:0.5rem;align-items:center;"
                  onsubmit="return confirm('Alle relaties zonder accountmanager koppelen?');">
                {ctx.csrf_input()}
                <select name="user_id" class="form-control" style="width:auto;" required>{options}</select>
                <button type="submit" class="btn btn-sm btn-primary">Koppelen</button>
            </form></div>'''

    body += _bulk_bar(ctx, users)
    body += _table(ctx, rows, managers, search, relation, sort_col, sort_dir)
    body += _bulk_script()
    body += page_footer()
    ctx.html(body)


def _filter_bar(search, relation, verbinding, sort_col, sort_dir) -> str:
    def keep(**overrides):
        params = {}
        if search:
            params['q'] = search
        if relation:
            params['relatie'] = relation
        if verbinding:
            params['verbinding'] = verbinding
        if sort_col != 'name' or sort_dir != 'asc':
            params['sort'], params['dir'] = sort_col, sort_dir
        params.update({k: v for k, v in overrides.items() if v})
        for key in [k for k, v in overrides.items() if not v]:
            params.pop(key, None)
        return '/customers' + ('?' + urllib.parse.urlencode(params) if params else '')

    def pill(label, href, active, color='#5C7A5A', small=False):
        pad = '0.25rem 0.9rem' if small else '0.35rem 1.1rem'
        size = '0.85rem' if small else '0.9rem'
        style = (f'display:inline-block;padding:{pad};border-radius:20px;text-decoration:none;'
                 f'font-size:{size};margin-right:0.35rem;border:2px solid {color};')
        style += f'background:{color};color:#fff;font-weight:bold;' if active else f'color:{color};'
        return f'<a href="{href}" style="{style}">{label}</a>'

    tabs = (pill('Alle', keep(relatie=''), not relation)
            + pill('Extern', keep(relatie='extern'), relation == 'extern')
            + pill('Intern', keep(relatie='intern'), relation == 'intern'))

    vb = pill('Alle', keep(verbinding=''), not verbinding, '#B0A49A', small=True)
    for value, color in [('ambassadeur', '#5C7A5A'), ('betrokken', '#7A8FA6'), ('niet betrokken', '#888')]:
        vb += pill(value.capitalize(), keep(verbinding=value), verbinding == value, color, small=True)

    # Toevoegen vanuit de interne lijst zet het formulier ook op intern.
    add_href = f'/customers/add?relatie={relation}' if relation else '/customers/add'

    hidden = ''
    if relation:
        hidden += f'<input type="hidden" name="relatie" value="{html.escape(relation)}">'
    if verbinding:
        hidden += f'<input type="hidden" name="verbinding" value="{html.escape(verbinding)}">'
    if sort_col != 'name' or sort_dir != 'asc':
        hidden += (f'<input type="hidden" name="sort" value="{html.escape(sort_col)}">'
                   f'<input type="hidden" name="dir" value="{html.escape(sort_dir)}">')

    return f'''
    <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;
                gap:0.75rem;margin:1rem 0 0.5rem;">
        <div>{tabs}</div>
        <div style="display:flex;gap:0.5rem;align-items:center;flex-wrap:wrap;">
            <form method="get" action="/customers" class="d-flex" style="margin:0;">
                {hidden}
                <input class="form-control me-2" type="search" name="q" placeholder="Zoeken"
                       value="{html.escape(search)}" style="min-width:180px;">
                <button class="btn btn-outline-success" type="submit">Zoek</button>
            </form>
            <a href="{add_href}" class="btn btn-primary">+ Toevoegen</a>
        </div>
    </div>
    <div style="margin-bottom:0.75rem;">{vb}</div>'''


def _bulk_bar(ctx, users) -> str:
    options = '<option value="">-- Kies gebruiker --</option>' + ''.join(
        f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in users)
    return f'''<div id="bulk-bar" style="display:none;background:#FEF8F0;border:1px solid #B5916A;
            border-radius:6px;padding:0.6rem 1rem;margin-bottom:0.5rem;gap:0.75rem;
            align-items:center;flex-wrap:wrap;">
        <strong id="bulk-count">0 geselecteerd</strong>
        <button type="button" onclick="bulkAction('intern')" class="btn btn-sm btn-secondary">Intern</button>
        <button type="button" onclick="bulkAction('extern')" class="btn btn-sm btn-secondary">Extern</button>
        <span style="color:#B0A49A;">|</span>
        <input type="text" id="bulk-tag-input" class="form-control" style="width:auto;"
               placeholder="Tag toevoegen...">
        <button type="button" onclick="bulkAction('add_tag')" class="btn btn-sm btn-secondary">+ Tag</button>
        <span style="color:#B0A49A;">|</span>
        <select id="bulk-user-select" class="form-control" style="width:auto;">{options}</select>
        <button type="button" onclick="bulkAction('link_user')" class="btn btn-sm btn-primary">Koppel</button>
    </div>
    <form method="post" action="/customers/bulk" id="bulk-form">
        {ctx.csrf_input()}
        <input type="hidden" name="bulk_action" id="bulk-action-input">
        <input type="hidden" name="bulk_tag" id="bulk-tag-hidden">
        <input type="hidden" name="bulk_user_id" id="bulk-user-hidden">'''


def _table(ctx, rows, managers, search, relation, sort_col, sort_dir) -> str:
    def th(label, col):
        arrow = ''
        new_dir = 'asc'
        if sort_col == col:
            arrow = ' &#9650;' if sort_dir == 'asc' else ' &#9660;'
            new_dir = 'desc' if sort_dir == 'asc' else 'asc'
        params = {'sort': col, 'dir': new_dir}
        if search:
            params['q'] = search
        if relation:
            params['relatie'] = relation
        href = '/customers?' + urllib.parse.urlencode(params)
        return f'<th><a href="{href}" style="color:inherit;text-decoration:none;">{label}{arrow}</a></th>'

    out = f'''<div class="table-wrap"><table class="mt-1"><thead><tr>
        <th style="width:32px;"><input type="checkbox" id="select-all" onclick="toggleAll(this)"></th>
        {th('Naam','name')}{th('Bedrijf','company')}{th('Type / Rol','role')}
        {th('Relatie','relation_type')}{th('Verbinding','verbinding')}
        <th>Tags</th><th>E‑mail</th><th>Telefoon</th><th>Accountmanager</th>
        {th('Datum','created_at')}<th class="text-end">Acties</th>
    </tr></thead><tbody>'''

    vb_colors = {'ambassadeur': ('#EDF3EC', '#5C7A5A'),
                 'betrokken': ('#EDF3EC', '#7A8FA6'),
                 'niet betrokken': ('#f5f5f5', '#888')}

    if not rows:
        out += '<tr><td colspan="12" class="text-center">Geen klanten gevonden.</td></tr>'
    for cust in rows:
        rel = cust['relation_type'] or 'extern'
        if rel == 'intern':
            type_cell = (html.escape(cust['role']) if cust['role']
                         else '<span style="color:#B0A49A;font-style:italic;">—</span>')
        else:
            type_cell = html.escape((cust['category'] or 'klant').capitalize())
        rel_color = '#7A8FA6' if rel == 'intern' else '#555'
        rel_bg = '#EDF3EC' if rel == 'intern' else '#EDE8E3'
        rel_badge = (f'<span style="background:{rel_bg};color:{rel_color};border-radius:12px;'
                     f'padding:0.15rem 0.6rem;font-size:0.82rem;font-weight:bold;">'
                     f'{html.escape(rel.capitalize())}</span>')

        vb = cust['verbinding']
        if vb in vb_colors:
            bg, fg = vb_colors[vb]
            vb_badge = (f'<span style="background:{bg};color:{fg};border-radius:12px;'
                        f'padding:0.15rem 0.6rem;font-size:0.82rem;font-weight:bold;">'
                        f'{html.escape(vb.capitalize())}</span>')
        else:
            vb_badge = '<span style="color:#B0A49A;font-size:0.82rem;">—</span>'

        tags = ', '.join(html.escape(t.strip()) for t in (cust['tags'] or '').split(',') if t.strip()) or '-'
        am = ', '.join(html.escape(n) for n in managers.get(cust['id'], [])) or '-'

        actions = f'<a href="/customers/edit?id={cust["id"]}" class="btn btn-sm btn-secondary">Bewerk</a> '
        actions += post_button(
            '/customers/delete', ctx, 'Verwijder',
            confirm='Weet je zeker dat je deze klant wilt verwijderen?',
            fields={'id': cust['id']})

        out += (f'<tr><td><input type="checkbox" name="selected_ids" value="{cust["id"]}" '
                f'class="row-cb" onchange="updateBulk()"></td>'
                f'<td><a href="/customers/view?id={cust["id"]}">{html.escape(cust["name"])}</a></td>'
                f'<td>{html.escape(cust["company"] or "-")}</td>'
                f'<td>{type_cell}</td><td>{rel_badge}</td><td>{vb_badge}</td>'
                f'<td>{tags}</td><td>{html.escape(cust["email"] or "")}</td>'
                f'<td>{html.escape(cust["phone"] or "-")}</td><td>{am}</td>'
                f'<td style="color:#B0A49A;font-size:0.85rem;">{(cust["created_at"] or "")[:10]}</td>'
                f'<td class="text-end" style="white-space:nowrap;">{actions}</td></tr>')
    return out + '</tbody></table></div></form>'


def _bulk_script() -> str:
    return '''<script>
function toggleAll(cb){document.querySelectorAll('.row-cb').forEach(function(c){c.checked=cb.checked;});updateBulk();}
function updateBulk(){
    var n=document.querySelectorAll('.row-cb:checked').length;
    document.getElementById('bulk-count').textContent=n+' geselecteerd';
    document.getElementById('bulk-bar').style.display=n>0?'flex':'none';
}
function bulkAction(action){
    if(!document.querySelectorAll('.row-cb:checked').length){alert('Selecteer eerst klanten.');return;}
    document.getElementById('bulk-action-input').value=action;
    document.getElementById('bulk-tag-hidden').value=document.getElementById('bulk-tag-input').value;
    document.getElementById('bulk-user-hidden').value=document.getElementById('bulk-user-select').value;
    document.getElementById('bulk-form').submit();
}
</script>'''


# ── Create / edit ─────────────────────────────────────────────────────────
def _collect_custom_fields(ctx) -> Optional[str]:
    custom: Dict[str, Any] = {}
    for field in get_custom_field_definitions():
        value = ctx.f(f'cf_{field["name"]}')
        if value:
            custom[field['name']] = value
    raw = ctx.f('custom_fields')
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                custom.update({str(k): str(v) for k, v in parsed.items()})
        except json.JSONDecodeError:
            for line in raw.splitlines():
                if '=' in line:
                    key, value = line.split('=', 1)
                    custom[key.strip()] = value.strip()
    return json.dumps(custom) if custom else None


def _read_form(ctx) -> Dict[str, Any]:
    relation = ctx.choice(ctx.f('relation_type', 'extern'), config.RELATION_TYPES, 'extern')
    verbinding = ctx.f('verbinding')
    return {
        'name': ctx.f('name'),
        'email': ctx.f('email'),
        'phone': ctx.f('phone') or None,
        'address': ctx.f('address') or None,
        'company': ctx.f('company') or None,
        'website': ctx.f('website') or None,
        'industry': ctx.f('industry') or None,
        'company_size': ctx.f('company_size') or None,
        'region': ctx.f('region') or None,
        'tags': ctx.f('tags') or None,
        'category': ctx.choice(ctx.f('category', 'klant'), ('klant', 'netwerk'), 'klant'),
        'relation_type': relation,
        'role': ctx.f('role') or None,
        'verbinding': verbinding if verbinding in config.VERBINDING_VALUES else None,
        'custom_fields': _collect_custom_fields(ctx),
    }


def add(ctx) -> None:
    if ctx.method == 'GET':
        _customer_form(ctx, None)
        return

    values = _read_form(ctx)
    if not values['name'] or not values['email']:
        _customer_form(ctx, None, error='Naam en e‑mail zijn verplicht.')
        return

    with connect() as conn:
        if conn.execute('SELECT id FROM customers WHERE email = ?', (values['email'],)).fetchone():
            _customer_form(ctx, None, error='Er bestaat al een klant met dit e‑mailadres.')
            return
        cur = conn.execute(
            'INSERT INTO customers (name, email, phone, address, company, website, industry, '
            ' company_size, region, tags, category, relation_type, role, verbinding, '
            ' created_by, custom_fields) '
            'VALUES (:name, :email, :phone, :address, :company, :website, :industry, '
            ' :company_size, :region, :tags, :category, :relation_type, :role, :verbinding, '
            ' :created_by, :custom_fields)',
            {**values, 'created_by': ctx.user_id})
        customer_id = cur.lastrowid
        _save_links(conn, customer_id, ctx.flist('linked_users'))

    log_action(ctx.user_id, 'create', 'customers', customer_id, f"name={values['name']}")
    reminders.refresh_for_customer(customer_id)
    ctx.redirect(f'/customers/view?id={customer_id}')


def edit(ctx) -> None:
    customer_id = ctx.qint('id')
    if not customer_id:
        ctx.not_found()
        return
    customer = get_customer(customer_id)
    if not customer:
        ctx.not_found()
        return
    if not can_edit_customer(ctx, customer_id):
        ctx.forbidden('Je kunt alleen relaties bewerken waaraan je gekoppeld bent.')
        return

    if ctx.method == 'GET':
        _customer_form(ctx, customer)
        return

    values = _read_form(ctx)
    if not values['name'] or not values['email']:
        _customer_form(ctx, customer, error='Naam en e‑mail zijn verplicht.')
        return

    with connect() as conn:
        clash = conn.execute('SELECT id FROM customers WHERE email = ? AND id != ?',
                             (values['email'], customer_id)).fetchone()
        if clash:
            _customer_form(ctx, customer, error='Er bestaat al een andere klant met dit e‑mailadres.')
            return
        conn.execute(
            'UPDATE customers SET name=:name, email=:email, phone=:phone, address=:address, '
            ' company=:company, website=:website, industry=:industry, company_size=:company_size, '
            ' region=:region, tags=:tags, category=:category, relation_type=:relation_type, '
            ' role=:role, verbinding=:verbinding, custom_fields=:custom_fields, '
            ' updated_at=CURRENT_TIMESTAMP WHERE id=:id',
            {**values, 'id': customer_id})
        conn.execute('DELETE FROM customer_users WHERE customer_id = ?', (customer_id,))
        _save_links(conn, customer_id, ctx.flist('linked_users'))

    log_action(ctx.user_id, 'update', 'customers', customer_id, f"name={values['name']}")
    reminders.refresh_for_customer(customer_id)
    ctx.redirect(f'/customers/view?id={customer_id}')


def _save_links(conn, customer_id: int, raw_ids) -> None:
    for raw in raw_ids:
        try:
            conn.execute('INSERT OR IGNORE INTO customer_users (customer_id, user_id) VALUES (?, ?)',
                         (customer_id, int(raw)))
        except (ValueError, TypeError):
            continue


def delete(ctx) -> None:
    customer_id = ctx.fint('id')
    if not customer_id:
        ctx.not_found()
        return
    if not can_delete_customer(ctx, customer_id):
        ctx.forbidden('Alleen een beheerder of de aanmaker kan deze relatie verwijderen.')
        return
    # Foreign keys are on now, so notes, tasks, interactions, documents and
    # links all cascade instead of being left behind as orphans.
    with connect() as conn:
        row = conn.execute('SELECT relation_type FROM customers WHERE id = ?',
                           (customer_id,)).fetchone()
        relation = (row['relation_type'] if row else None) or 'extern'
        conn.execute('DELETE FROM customers WHERE id = ?', (customer_id,))
    log_action(ctx.user_id, 'delete', 'customers', customer_id)
    # Terug naar de lijst waar deze relatie in stond.
    ctx.redirect(f'/customers?relatie={relation}')


def _customer_form(ctx, customer: Optional[Dict[str, Any]], error: str = '') -> None:
    editing = customer is not None
    title = 'Klant bewerken' if editing else 'Klant toevoegen'
    linked = get_linked_user_ids(customer['id']) if editing else []
    users = all_users()

    def val(key, default=''):
        return html.escape(str((customer or {}).get(key) or default))

    # Nieuw record: neem de lijst over waar de gebruiker vandaan kwam.
    relation = ((customer or {}).get('relation_type')
                or ctx.choice(ctx.q('relatie'), config.RELATION_TYPES, 'extern'))
    role_val = (customer or {}).get('role') or ''
    verbinding = (customer or {}).get('verbinding') or ''
    category = (customer or {}).get('category') or 'klant'
    raw_custom = (customer or {}).get('custom_fields') or ''

    existing_custom: Dict[str, Any] = {}
    if raw_custom:
        try:
            parsed = json.loads(raw_custom)
            if isinstance(parsed, dict):
                existing_custom = parsed
        except json.JSONDecodeError:
            pass

    dynamic = ''
    for field in get_custom_field_definitions():
        key, label = field['name'], field['label']
        dynamic += (f'<div class="mb-3"><label class="form-label" for="cf_{html.escape(key)}">'
                    f'{html.escape(label)}</label>'
                    f'<input type="text" class="form-control" id="cf_{html.escape(key)}" '
                    f'name="cf_{html.escape(key)}" '
                    f'value="{html.escape(str(existing_custom.get(key, "")))}"></div>')

    pills = ''.join(
        f'<span class="user-pill">'
        f'<input type="checkbox" name="linked_users" value="{u["id"]}" id="upill_{u["id"]}"'
        f'{" checked" if u["id"] in linked else ""}>'
        f'<label for="upill_{u["id"]}">{html.escape(u["username"])}</label></span>'
        for u in users) or '<em>Geen gebruikers gevonden.</em>'

    role_options = ''.join(
        f'<option value="{r}"{" selected" if role_val == r else ""}>{r}</option>'
        for r in INTERNAL_ROLES)
    role_options += (f'<option value="anders"'
                     f'{" selected" if role_val and role_val not in INTERNAL_ROLES else ""}>'
                     f'Anders...</option>')

    action = f'/customers/edit?id={customer["id"]}' if editing else '/customers/add'

    body = page_header(title, ctx)
    body += f'<h2 class="mt-4">{title}</h2>'
    body += alert(error, 'danger')
    body += f'''<div class="card">
        <form method="post" action="{action}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label" for="name">Naam</label>
                <input type="text" class="form-control" id="name" name="name" value="{val('name')}" required></div>
            <div class="mb-3"><label class="form-label" for="email">E‑mail</label>
                <input type="email" class="form-control" id="email" name="email" value="{val('email')}" required></div>
            <div class="mb-3"><label class="form-label" for="phone">Telefoon</label>
                <input type="text" class="form-control" id="phone" name="phone" value="{val('phone')}"></div>
            <div class="mb-3"><label class="form-label" for="address">Adres</label>
                <input type="text" class="form-control" id="address" name="address" value="{val('address')}"></div>
            <div class="mb-3"><label class="form-label" for="company">Bedrijf</label>
                <input type="text" class="form-control" id="company" name="company" value="{val('company')}"></div>
            <div class="mb-3"><label class="form-label" for="website">Website</label>
                <input type="text" class="form-control" id="website" name="website" value="{val('website')}"></div>
            <div class="mb-3"><label class="form-label" for="industry">Branche</label>
                <input type="text" class="form-control" id="industry" name="industry" value="{val('industry')}"></div>
            <div class="mb-3"><label class="form-label" for="company_size">Grootte</label>
                <input type="text" class="form-control" id="company_size" name="company_size" value="{val('company_size')}"></div>
            <div class="mb-3"><label class="form-label" for="region">Regio</label>
                <input type="text" class="form-control" id="region" name="region" value="{val('region')}"></div>
            <div class="mb-3"><label class="form-label" for="tags">Tags (gescheiden door komma)</label>
                <input type="text" class="form-control" id="tags" name="tags" value="{val('tags')}"></div>

            <div class="mb-3"><label class="form-label">Relatie</label><br>
                <span class="user-pill">
                    <input type="radio" name="relation_type" value="extern" id="rel_extern"
                           {'checked' if relation != 'intern' else ''} onchange="toggleRelation()">
                    <label for="rel_extern">Extern</label></span>
                <span class="user-pill">
                    <input type="radio" name="relation_type" value="intern" id="rel_intern"
                           {'checked' if relation == 'intern' else ''} onchange="toggleRelation()">
                    <label for="rel_intern">Intern</label></span>
            </div>
            <div class="mb-3" id="field-category" style="{'display:none' if relation == 'intern' else ''}">
                <label class="form-label" for="category">Type</label>
                <select class="form-select" id="category" name="category">
                    <option value="klant" {'selected' if category == 'klant' else ''}>Klant</option>
                    <option value="netwerk" {'selected' if category == 'netwerk' else ''}>Netwerk</option>
                </select>
            </div>
            <div class="mb-3" id="field-role" style="{'display:none' if relation != 'intern' else ''}">
                <label class="form-label" for="role_select">Rol</label>
                <select class="form-select" id="role_select" onchange="toggleRoleCustom()">{role_options}</select>
                <input type="text" class="form-control mt-2" id="role_custom" name="role"
                       placeholder="Vul rol in..." value="{html.escape(role_val)}"
                       style="{'display:none' if not role_val or role_val in INTERNAL_ROLES else ''}">
            </div>
            <div class="mb-3"><label class="form-label" for="verbinding">Verbinding</label>
                <select class="form-select" id="verbinding" name="verbinding">
                    <option value="" {'selected' if not verbinding else ''}>— Kies —</option>
                    <option value="ambassadeur" {'selected' if verbinding == 'ambassadeur' else ''}>Ambassadeur</option>
                    <option value="betrokken" {'selected' if verbinding == 'betrokken' else ''}>Betrokken</option>
                    <option value="niet betrokken" {'selected' if verbinding == 'niet betrokken' else ''}>Niet betrokken</option>
                </select>
            </div>
            {dynamic}
            <div class="mb-3"><label class="form-label" for="custom_fields">Extra velden (JSON of key=value per regel)</label>
                <textarea class="form-control" id="custom_fields" name="custom_fields" rows="3">{html.escape(raw_custom)}</textarea>
            </div>
            <div class="mb-3"><label class="form-label"><strong>Accountmanagers</strong></label>
                <div style="margin-top:0.3rem;">{pills}</div>
                <small style="color:#B0A49A;">Gekoppelde managers krijgen automatisch een herinnering
                    (intern: {config.REMINDER_DAYS_INTERN // 30} maanden,
                     extern: {config.REMINDER_DAYS_EXTERN // 30} maanden).</small>
            </div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/customers" class="btn btn-link">Annuleren</a>
        </form>
    </div>
    <script>
    var PRESET_ROLES = {json.dumps(INTERNAL_ROLES)};
    function toggleRelation() {{
        var intern = document.getElementById('rel_intern').checked;
        document.getElementById('field-category').style.display = intern ? 'none' : '';
        document.getElementById('field-role').style.display = intern ? '' : 'none';
        if (intern) toggleRoleCustom();
    }}
    function toggleRoleCustom() {{
        var sel = document.getElementById('role_select');
        var custom = document.getElementById('role_custom');
        if (sel.value === 'anders') {{ custom.style.display = ''; }}
        else {{ custom.style.display = 'none'; custom.value = sel.value; }}
    }}
    document.querySelector('form[method=post]').addEventListener('submit', function() {{
        if (document.getElementById('rel_intern').checked) {{
            var sel = document.getElementById('role_select');
            if (sel.value !== 'anders') document.getElementById('role_custom').value = sel.value;
        }} else {{
            document.getElementById('role_custom').value = '';
        }}
    }});
    </script>'''
    body += page_footer()
    ctx.html(body)


# ── Detail ────────────────────────────────────────────────────────────────
def detail(ctx) -> None:
    customer_id = ctx.qint('id')
    if not customer_id:
        ctx.not_found()
        return
    customer = get_customer(customer_id)
    if not customer:
        ctx.not_found()
        return

    if ctx.method == 'POST':
        content = ctx.f('content')
        if content:
            note_id = None
            with connect() as conn:
                cur = conn.execute(
                    'INSERT INTO notes (content, customer_id, user_id) VALUES (?, ?, ?)',
                    (content, customer_id, ctx.user_id))
                note_id = cur.lastrowid
            log_action(ctx.user_id, 'create', 'notes', note_id)
            reminders.refresh_for_customer(customer_id)
        ctx.redirect(f'/customers/view?id={customer_id}')
        return

    _detail_page(ctx, customer)


def _detail_page(ctx, customer: Dict[str, Any], task_error: str = '') -> None:
    cid = customer['id']
    with connect(readonly=True) as conn:
        notes = conn.execute('''
            SELECT n.id AS note_id, n.content, n.created_at, n.user_id, u.username AS author
              FROM notes n LEFT JOIN users u ON n.user_id = u.id
             WHERE n.customer_id = ? ORDER BY n.created_at DESC''', (cid,)).fetchall()
        tasks_rows = conn.execute('''
            SELECT t.id AS task_id, t.title, t.description, t.due_date, t.status,
                   t.created_at, u.username AS author
              FROM tasks t JOIN users u ON t.user_id = u.id
             WHERE t.customer_id = ?
             ORDER BY CASE t.status WHEN 'open' THEN 0 ELSE 1 END,
                      COALESCE(t.due_date,'') ASC, t.created_at ASC''', (cid,)).fetchall()
        interactions = conn.execute('''
            SELECT i.id AS interaction_id, i.interaction_type, i.note, i.created_at,
                   i.contact_date, i.user_id, u.username AS author
              FROM interactions i JOIN users u ON i.user_id = u.id
             WHERE i.customer_id = ?
             ORDER BY COALESCE(i.contact_date, DATE(i.created_at)) DESC''', (cid,)).fetchall()
        users = conn.execute('SELECT id, username FROM users ORDER BY username ASC').fetchall()

    may_edit = can_edit_customer(ctx, cid)
    body = page_header(f'Klant: {customer["name"]}', ctx)

    # ── Profile card ──
    actions = ''
    if customer['phone']:
        actions += (f"<a href='tel:{html.escape(customer['phone'])}'>"
                    f"<i data-lucide=phone class=icon></i> Bel</a>")
    actions += (f"<a href='mailto:{html.escape(customer['email'] or '')}'>"
                f"<i data-lucide=mail class=icon></i> Email</a>")

    manage = ''
    if may_edit:
        manage += f'<a href="/customers/edit?id={cid}" class="btn btn-sm btn-secondary">Bewerk</a> '
    if can_delete_customer(ctx, cid):
        manage += post_button('/customers/delete', ctx, 'Verwijder',
                              confirm='Weet je zeker dat je deze klant wilt verwijderen?',
                              fields={'id': cid})

    subtitle = ''
    if customer.get('company') or customer.get('address'):
        subtitle = (f"<p>{html.escape(customer.get('company') or '')}<br>"
                    f"<small>{html.escape(customer.get('address') or '')}</small></p>")

    body += f'''<div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:0.5rem;">
            <div><h2>{html.escape(customer['name'])}</h2>{subtitle}</div>
            <div style="display:flex;gap:0.4rem;align-items:center;">{manage}</div>
        </div>
        <div class="action-buttons mt-2">{actions}</div>
    </div>'''

    # ── Contact details ──
    details = f"<p><i data-lucide=mail class=icon></i> {html.escape(customer['email'] or '')}</p>"
    for field, icon in [('phone', 'phone'), ('address', 'home'), ('company', 'briefcase'),
                        ('website', 'globe'), ('industry', 'factory'), ('region', 'map-pin')]:
        if customer.get(field):
            details += (f"<p><i data-lucide={icon} class=icon></i> "
                        f"{html.escape(str(customer[field]))}</p>")
    if customer.get('tags'):
        tags = ', '.join(html.escape(t.strip()) for t in customer['tags'].split(',') if t.strip())
        details += f'<p><i data-lucide=link-2 class=icon></i> {tags}</p>'

    if (customer.get('relation_type') or 'extern') == 'intern':
        type_line = 'Rol: ' + html.escape(customer.get('role') or '-')
    else:
        type_line = 'Type: ' + html.escape((customer.get('category') or 'klant').capitalize())
    details += (f"<p><i data-lucide=notebook-text class=icon></i> {type_line} &middot; "
                f"{html.escape((customer.get('relation_type') or 'extern').capitalize())}</p>")

    creator = get_user_by_id(customer['created_by']) if customer.get('created_by') else None
    details += (f"<p><i data-lucide=user class=icon></i> Toegevoegd door: "
                f"{html.escape(creator['username']) if creator else '-'}</p>")
    details += (f"<p><i data-lucide=calendar class=icon></i> Aangemaakt op "
                f"{html.escape(str(customer['created_at'] or ''))}</p>")

    manager_names = [html.escape(u['username']) for u in
                     (get_user_by_id(uid) for uid in get_linked_user_ids(cid)) if u]
    if manager_names:
        details += (f"<p><i data-lucide=users class=icon></i> Accountmanagers: "
                    f"{', '.join(manager_names)}</p>")

    if customer.get('custom_fields'):
        try:
            parsed = json.loads(customer['custom_fields'])
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    details += (f"<p><i data-lucide=file-text class=icon></i> "
                                f"{html.escape(str(key).capitalize())}: {html.escape(str(value))}</p>")
        except json.JSONDecodeError:
            details += f"<p>{html.escape(customer['custom_fields'])}</p>"

    body += f'<div class="card"><div class="section-title">Contactgegevens</div>{details}</div>'

    # ── Timeline ──
    body += _timeline(tasks_rows, notes, interactions)

    # ── Tasks ──
    body += _tasks_card(ctx, cid, tasks_rows, users, task_error)

    # ── Notes ──
    body += _notes_card(ctx, cid, notes)

    # ── Interactions ──
    body += _interactions_card(ctx, cid, interactions)

    body += page_footer()
    ctx.html(body)


def _timeline(tasks_rows, notes, interactions) -> str:
    icons = {'call': 'phone', 'email': 'mail', 'message': 'message-circle', 'meeting': 'handshake'}
    items = []
    for t in tasks_rows:
        date = t['due_date'] or (t['created_at'] or '')[:10]
        color = '#5C7A5A' if t['status'] == 'completed' else '#B5916A'
        items.append((date, f'<span style="color:{color};"><i data-lucide=clipboard-list class=icon>'
                            f'</i></span> <strong>{html.escape(t["title"])}</strong> '
                            f'<small style="color:#B0A49A;">(Taak · {html.escape(t["author"] or "")} '
                            f'· {html.escape(str(date))})</small>'))
    for n in notes:
        date = (n['created_at'] or '')[:10]
        content = n['content'] or ''
        snippet = (content[:80] + '…') if len(content) > 80 else content
        items.append((date, f'<i data-lucide=notebook-text class=icon></i> {html.escape(snippet)} '
                            f'<small style="color:#B0A49A;">(Notitie · '
                            f'{html.escape(n["author"] or "")} · {html.escape(date)})</small>'))
    for i in interactions:
        date = i['contact_date'] or (i['created_at'] or '')[:10]
        label = INTERACTION_LABELS.get(i['interaction_type'], i['interaction_type'] or '')
        icon = icons.get(i['interaction_type'], 'bell')
        note_part = f' — {html.escape(i["note"])}' if i['note'] else ''
        items.append((date, f'<i data-lucide={icon} class=icon></i> <strong>{html.escape(label)}'
                            f'</strong>{note_part} <small style="color:#B0A49A;">(Interactie · '
                            f'{html.escape(i["author"] or "")} · {html.escape(str(date))})</small>'))

    items.sort(key=lambda pair: str(pair[0]), reverse=True)
    inner = ''.join(f'<div style="border-bottom:1px solid #EDE8E3;padding:0.4rem 0;">{markup}</div>'
                    for _, markup in items) or '<p style="color:#B0A49A;">Nog geen activiteit.</p>'
    return (f'<details style="margin-bottom:0.75rem;">'
            f'<summary style="cursor:pointer;font-weight:bold;padding:0.6rem 1rem;background:#fff;'
            f'border-radius:8px;border:1px solid #E4DDD6;">'
            f'<i data-lucide=clipboard-list class=icon></i> Activiteitenoverzicht ({len(items)})'
            f'</summary><div class="card" style="margin-top:0.25rem;">{inner}</div></details>')


def _tasks_card(ctx, cid, tasks_rows, users, task_error) -> str:
    from ..permissions import can_manage_task
    options = '<option value="">Mezelf</option>' + ''.join(
        f'<option value="{u["id"]}">{html.escape(u["username"])}</option>' for u in users)
    section = alert(task_error, 'danger')
    section += f'''<form method="post" action="/tasks/add" style="margin-bottom:1rem;">
        {ctx.csrf_input()}
        <input type="hidden" name="customer_id" value="{cid}">
        <div class="mb-3"><label class="form-label">Titel</label>
            <input type="text" name="title" class="form-control" required></div>
        <div class="mb-3"><label class="form-label">Vervaldatum</label>
            <input type="date" name="due_date" class="form-control"></div>
        <div class="mb-3"><label class="form-label">Beschrijving</label>
            <input type="text" name="description" class="form-control"></div>
        <div class="mb-3"><label class="form-label">Toewijzen aan</label>
            <select name="assigned_user_id" class="form-select">{options}</select></div>
        <button type="submit" class="btn btn-primary">Taak toevoegen</button>
    </form>'''

    if not tasks_rows:
        section += '<p style="color:#B0A49A;">Er zijn nog geen taken.</p>'
    for task in tasks_rows:
        done = task['status'] == 'completed'
        label = 'Voltooid' if done else 'Open'
        color = '#5C7A5A' if done else '#B5916A'
        desc = f"<br><small>{html.escape(task['description'])}</small>" if task['description'] else ''
        buttons = ''
        if can_manage_task(ctx, task['task_id']):
            if not done:
                buttons += post_button('/tasks/complete', ctx, 'Markeer voltooid',
                                       css='btn btn-sm btn-secondary',
                                       fields={'id': task['task_id'], 'customer_id': cid}) + ' '
            buttons += post_button('/tasks/delete', ctx, 'Verwijder',
                                   confirm='Weet je zeker dat je deze taak wilt verwijderen?',
                                   fields={'id': task['task_id'], 'customer_id': cid})
        section += (f'<div class="task-row">'
                    f'<span style="color:{color};font-weight:bold;">{label}</span> '
                    f'<strong>{html.escape(task["title"])}</strong> '
                    f'(Vervaldatum: {html.escape(str(task["due_date"] or "-"))}){desc}'
                    f'<div style="font-size:0.8rem;color:#7A6E66;">Aangemaakt op '
                    f'{html.escape(str(task["created_at"] or ""))} &middot; Toegewezen aan: '
                    f'<strong>{html.escape(task["author"] or "")}</strong></div>'
                    f'<div style="margin-top:0.3rem;display:flex;gap:0.3rem;">{buttons}</div></div>')
    return f'<div class="card"><div class="section-title">Taken</div>{section}</div>'


def _notes_card(ctx, cid, notes) -> str:
    section = f'''<form method="post" action="/customers/view?id={cid}" style="margin-bottom:1rem;">
        {ctx.csrf_input()}
        <label class="form-label">Nieuwe notitie</label>
        <textarea name="content" rows="3" class="form-control" required></textarea>
        <button type="submit" class="btn btn-primary mt-2">Opslaan</button>
    </form>'''
    if not notes:
        section += '<p style="color:#B0A49A;">Er zijn nog geen notities.</p>'
    for note in notes:
        author = f"door {html.escape(note['author'])}" if note['author'] else ''
        remove = ''
        if ctx.is_admin or note['user_id'] == ctx.user_id:
            remove = post_button('/notes/delete', ctx, 'Verwijder',
                                 confirm='Weet je zeker dat je deze notitie wilt verwijderen?',
                                 css='btn btn-sm btn-danger',
                                 fields={'id': note['note_id'], 'customer_id': cid})
        section += (f'<div class="task-row">{html.escape(note["content"] or "")}'
                    f'<div style="font-size:0.8rem;color:#7A6E66;">'
                    f'{html.escape(str(note["created_at"] or ""))} {author}</div>'
                    f'<div style="margin-top:0.3rem;">{remove}</div></div>')
    return f'<div class="card"><div class="section-title">Notities</div>{section}</div>'


def _interactions_card(ctx, cid, interactions) -> str:
    today = datetime.date.today().isoformat()
    options = ''.join(f'<option value="{value}">{label}</option>'
                      for value, label in INTERACTION_LABELS.items())
    section = f'''<form method="post" action="/interactions/add" style="margin-bottom:1rem;">
        {ctx.csrf_input()}
        <input type="hidden" name="customer_id" value="{cid}">
        <div class="mb-3"><label class="form-label">Type interactie</label>
            <select name="interaction_type" class="form-select" required>
                <option value="">Selecteer...</option>{options}</select></div>
        <div class="mb-3"><label class="form-label">Datum contact</label>
            <input type="date" name="contact_date" class="form-control" value="{today}">
            <small style="color:#7A6E66;">Pas de datum aan als het contact eerder plaatsvond — de
                herinnering wordt dan vanaf die datum berekend.</small></div>
        <div class="mb-3"><label class="form-label">Notitie (optioneel)</label>
            <input type="text" name="note" class="form-control"></div>
        <button type="submit" class="btn btn-primary">Interactie toevoegen</button>
    </form>'''
    if not interactions:
        section += '<p style="color:#B0A49A;">Er zijn nog geen interacties.</p>'
    for item in interactions:
        label = INTERACTION_LABELS.get(item['interaction_type'], item['interaction_type'] or '')
        note_part = f"<br><small>{html.escape(item['note'])}</small>" if item['note'] else ''
        date = item['contact_date'] or (item['created_at'] or '')[:10]
        buttons = ''
        if ctx.is_admin or item['user_id'] == ctx.user_id:
            buttons = (f'<a href="/interactions/edit?id={item["interaction_id"]}&customer_id={cid}" '
                       f'class="btn btn-sm btn-secondary">Bewerk</a> ')
            buttons += post_button('/interactions/delete', ctx, 'Verwijder',
                                   confirm='Weet je zeker dat je deze interactie wilt verwijderen?',
                                   fields={'id': item['interaction_id'], 'customer_id': cid})
        section += (f'<div class="task-row"><strong>{html.escape(label)}</strong>{note_part}'
                    f'<div style="font-size:0.8rem;color:#7A6E66;">{html.escape(str(date))} door '
                    f'{html.escape(item["author"] or "")}</div>'
                    f'<div style="margin-top:0.3rem;display:flex;gap:0.3rem;">{buttons}</div></div>')
    return f'<div class="card"><div class="section-title">Interacties</div>{section}</div>'


# ── Notes ─────────────────────────────────────────────────────────────────
def delete_note(ctx) -> None:
    note_id = ctx.fint('id')
    customer_id = ctx.fint('customer_id')
    if not note_id or not customer_id:
        ctx.not_found()
        return
    if not can_manage_note(ctx, note_id):
        ctx.forbidden('Je kunt alleen je eigen notities verwijderen.')
        return
    with connect() as conn:
        conn.execute('DELETE FROM notes WHERE id = ?', (note_id,))
    log_action(ctx.user_id, 'delete', 'notes', note_id)
    ctx.redirect(f'/customers/view?id={customer_id}')


# ── Interactions ──────────────────────────────────────────────────────────
def add_interaction(ctx) -> None:
    customer_id = ctx.fint('customer_id')
    if not customer_id:
        ctx.not_found()
        return
    customer = get_customer(customer_id)
    if not customer:
        ctx.not_found()
        return

    interaction_type = ctx.f('interaction_type')
    if interaction_type not in config.INTERACTION_TYPES:
        _detail_page(ctx, customer, task_error='Kies een geldig interactietype.')
        return

    with connect() as conn:
        cur = conn.execute(
            'INSERT INTO interactions (interaction_type, note, contact_date, customer_id, user_id) '
            'VALUES (?, ?, ?, ?, ?)',
            (interaction_type, ctx.f('note') or None, ctx.f('contact_date') or None,
             customer_id, ctx.user_id))
        interaction_id = cur.lastrowid
    log_action(ctx.user_id, 'create', 'interactions', interaction_id, f'type={interaction_type}')
    reminders.refresh_for_customer(customer_id)
    ctx.redirect(f'/customers/view?id={customer_id}')


def edit_interaction(ctx) -> None:
    interaction_id = ctx.qint('id')
    customer_id = ctx.qint('customer_id')
    if not interaction_id or not customer_id:
        ctx.not_found()
        return
    if not can_manage_interaction(ctx, interaction_id):
        ctx.forbidden('Je kunt alleen je eigen interacties bewerken.')
        return

    if ctx.method == 'POST':
        interaction_type = ctx.f('interaction_type')
        if interaction_type not in config.INTERACTION_TYPES:
            ctx.redirect(f'/interactions/edit?id={interaction_id}&customer_id={customer_id}')
            return
        with connect() as conn:
            conn.execute(
                'UPDATE interactions SET interaction_type=?, note=?, contact_date=? WHERE id=?',
                (interaction_type, ctx.f('note') or None, ctx.f('contact_date') or None,
                 interaction_id))
        log_action(ctx.user_id, 'update', 'interactions', interaction_id, f'type={interaction_type}')
        reminders.refresh_for_customer(customer_id)
        ctx.redirect(f'/customers/view?id={customer_id}')
        return

    row = query_all('SELECT * FROM interactions WHERE id = ?', (interaction_id,))
    if not row:
        ctx.not_found()
        return
    item = row[0]
    customer = get_customer(customer_id)
    name = customer['name'] if customer else f'Klant {customer_id}'
    current_date = item['contact_date'] or (item['created_at'] or '')[:10]
    options = ''.join(
        f'<option value="{value}"{" selected" if item["interaction_type"] == value else ""}>'
        f'{label}</option>' for value, label in INTERACTION_LABELS.items())

    body = page_header('Interactie bewerken', ctx)
    body += (f'<h2 class="mt-4"><i data-lucide=pencil class=icon></i> Interactie bewerken — '
             f'{html.escape(name)}</h2>')
    body += f'''<div class="card" style="max-width:520px;">
        <form method="POST" action="/interactions/edit?id={interaction_id}&customer_id={customer_id}">
            {ctx.csrf_input()}
            <div class="mb-3"><label class="form-label">Type interactie</label>
                <select name="interaction_type" class="form-select" required>{options}</select></div>
            <div class="mb-3"><label class="form-label">Datum contact</label>
                <input type="date" name="contact_date" class="form-control"
                       value="{html.escape(str(current_date or ''))}"></div>
            <div class="mb-3"><label class="form-label">Notitie (optioneel)</label>
                <input type="text" name="note" class="form-control"
                       value="{html.escape(item['note'] or '')}"></div>
            <button type="submit" class="btn btn-primary">Opslaan</button>
            <a href="/customers/view?id={customer_id}" class="btn btn-link">Annuleren</a>
        </form></div>'''
    body += page_footer()
    ctx.html(body)


def delete_interaction(ctx) -> None:
    interaction_id = ctx.fint('id')
    customer_id = ctx.fint('customer_id')
    if not interaction_id or not customer_id:
        ctx.not_found()
        return
    if not can_manage_interaction(ctx, interaction_id):
        ctx.forbidden('Je kunt alleen je eigen interacties verwijderen.')
        return
    with connect() as conn:
        conn.execute('DELETE FROM interactions WHERE id = ?', (interaction_id,))
    log_action(ctx.user_id, 'delete', 'interactions', interaction_id)
    reminders.refresh_for_customer(customer_id)
    ctx.redirect(f'/customers/view?id={customer_id}')


# ── Bulk actions ──────────────────────────────────────────────────────────
def bulk(ctx) -> None:
    action = ctx.f('bulk_action')
    ids = []
    for raw in ctx.flist('selected_ids'):
        try:
            ids.append(int(raw))
        except (TypeError, ValueError):
            continue
    if not ids or not action:
        ctx.redirect('/customers')
        return

    allowed = [cid for cid in ids if can_edit_customer(ctx, cid)]
    skipped = len(ids) - len(allowed)
    if allowed:
        with connect() as conn:
            if action in config.RELATION_TYPES:
                conn.executemany('UPDATE customers SET relation_type = ? WHERE id = ?',
                                 [(action, cid) for cid in allowed])
            elif action == 'add_tag':
                tag = ctx.f('bulk_tag')
                if tag:
                    for cid in allowed:
                        row = conn.execute('SELECT tags FROM customers WHERE id = ?', (cid,)).fetchone()
                        tags = [t.strip() for t in ((row['tags'] if row else '') or '').split(',') if t.strip()]
                        if tag not in tags:
                            tags.append(tag)
                        conn.execute('UPDATE customers SET tags = ? WHERE id = ?',
                                     (','.join(tags), cid))
            elif action == 'link_user':
                target = ctx.fint('bulk_user_id')
                if target:
                    conn.executemany(
                        'INSERT OR IGNORE INTO customer_users (customer_id, user_id) VALUES (?, ?)',
                        [(cid, target) for cid in allowed])
        log_action(ctx.user_id, 'update', 'customers', None,
                   f'bulk {action} op {len(allowed)} klanten'
                   + (f' ({skipped} overgeslagen: geen rechten)' if skipped else ''))
    ctx.redirect('/customers')


def bulk_link_empty(ctx) -> None:
    target = ctx.fint('user_id')
    if not target:
        ctx.redirect('/customers')
        return
    with connect() as conn:
        conn.execute('''
            INSERT OR IGNORE INTO customer_users (customer_id, user_id)
            SELECT c.id, ? FROM customers c
             WHERE NOT EXISTS (SELECT 1 FROM customer_users cu WHERE cu.customer_id = c.id)
        ''', (target,))
    log_action(ctx.user_id, 'update', 'customer_users', None, f'bulk link lege AM aan user {target}')
    ctx.redirect('/customers')


# ── Export ────────────────────────────────────────────────────────────────
def export_csv(ctx) -> None:
    columns = ['id', 'name', 'email', 'phone', 'address', 'company', 'website', 'industry',
               'company_size', 'region', 'tags', 'category', 'relation_type', 'role',
               'verbinding', 'created_by', 'creator_name', 'custom_fields',
               'created_at', 'updated_at']
    rows = query_all('SELECT c.*, u.username AS creator_name FROM customers c '
                     'LEFT JOIN users u ON c.created_by = u.id ORDER BY c.id ASC')
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    for row in rows:
        keys = row.keys()
        writer.writerow([row[c] if c in keys and row[c] is not None else '' for c in columns])
    log_action(ctx.user_id, 'export', 'customers', None, f'{len(rows)} rijen')
    ctx.csv('customers_export.csv', buffer.getvalue().encode('utf-8'))
