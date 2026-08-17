"""Login, logout, first-run registration and password change."""

from __future__ import annotations

import html

from .. import auth, config
from ..db import get_user_by_id, log_action, users_exist
from ..ui import alert, page_footer, page_header


def _landing_for(ctx) -> str:
    """Where a user belongs after signing in."""
    if not ctx.is_admin and ctx.is_comm:
        return '/comm/board'
    if not ctx.is_admin and ctx.is_gov:
        return '/gov/board'
    return '/dashboard'


def root(ctx) -> None:
    ctx.redirect(_landing_for(ctx) if ctx.logged_in else '/login')


# ── Login ─────────────────────────────────────────────────────────────────
def _login_page(ctx, error: str = '', info: str = '', status: int = 200) -> None:
    body = page_header('Inloggen', ctx)
    body += '<h2 class="mt-4">Inloggen</h2>'
    body += alert(error, 'danger') + alert(info, 'success')
    body += '''
    <form method="post" class="mt-3">
        <div class="mb-3">
            <label for="username" class="form-label">Gebruikersnaam of e‑mail</label>
            <input type="text" class="form-control" id="username" name="username"
                   required autocomplete="username" autofocus>
        </div>
        <div class="mb-3">
            <label for="password" class="form-label">Wachtwoord</label>
            <input type="password" class="form-control" id="password" name="password"
                   required autocomplete="current-password">
        </div>
        <button type="submit" class="btn btn-primary">Inloggen</button>
    </form>'''
    # Only advertise registration while the very first account can still be made.
    if not users_exist():
        body += '<p class="mt-3">Nog geen account? <a href="/register">Maak het eerste account</a></p>'
    body += page_footer()
    ctx.html(body, status=status)


def login(ctx) -> None:
    if ctx.logged_in:
        ctx.redirect(_landing_for(ctx))
        return
    if ctx.method == 'GET':
        _login_page(ctx)
        return

    allowed, wait = auth.login_allowed(ctx.client_ip)
    if not allowed:
        _login_page(ctx, error=f'Te veel mislukte pogingen. Wacht {wait} seconden.', status=429)
        return

    user = auth.verify_user(ctx.f('username'), ctx.f('password'))
    if not user:
        auth.record_login_failure(ctx.client_ip)
        _login_page(ctx, error='Ongeldige inloggegevens.', status=401)
        return

    auth.record_login_success(ctx.client_ip)
    session_id, _ = auth.create_session(user['id'])
    log_action(user['id'], 'login', 'users', user['id'])

    is_admin = bool(user.get('is_admin')) or user['id'] == 1
    if not is_admin and user.get('is_comm'):
        destination = '/comm/board'
    elif not is_admin and user.get('is_governance'):
        destination = '/gov/board'
    else:
        destination = '/dashboard'
    ctx.set_session_cookie(session_id, destination)


def logout(ctx) -> None:
    auth.destroy_session(ctx.session_id)
    log_action(ctx.user_id, 'logout', 'users', ctx.user_id)
    ctx.clear_session_cookie('/login')


# ── Registration ──────────────────────────────────────────────────────────
def _register_page(ctx, error: str = '') -> None:
    body = page_header('Registreren', ctx)
    body += '<h2 class="mt-4">Eerste account aanmaken</h2>'
    body += alert(error, 'danger')
    body += f'''
    <form method="post" class="mt-3">
        {ctx.csrf_input() if ctx.logged_in else ''}
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
            <input type="password" class="form-control" id="password" name="password"
                   required minlength="{config.MIN_PASSWORD_LENGTH}" autocomplete="new-password">
            <small style="color:#B0A49A;">Minimaal {config.MIN_PASSWORD_LENGTH} tekens.</small>
        </div>
        <button type="submit" class="btn btn-primary">Account aanmaken</button>
        <p class="mt-3">Al een account? <a href="/login">Inloggen</a></p>
    </form>'''
    body += page_footer()
    ctx.html(body)


def register(ctx) -> None:
    """Open only until the first account exists; after that it is admin-only."""
    already_set_up = users_exist()
    if already_set_up and not ctx.is_admin:
        ctx.redirect('/login')
        return
    if already_set_up and ctx.method == 'POST' and not ctx.csrf_valid():
        ctx.forbidden('Ongeldig formulier. Ververs de pagina.')
        return

    if ctx.method == 'GET':
        _register_page(ctx)
        return

    # The very first account is the administrator.
    ok, message = auth.create_user(
        ctx.f('username'), ctx.f('email'), ctx.f('password'), is_admin=not already_set_up
    )
    if not ok:
        _register_page(ctx, error=message)
        return
    if ctx.logged_in:
        ctx.redirect('/users')
    else:
        _login_page(ctx, info=message)


# ── Password change ───────────────────────────────────────────────────────
def _password_form(ctx) -> str:
    return f'''<div class="card" style="max-width:420px;">
        <form method="POST" action="/account/password">
            {ctx.csrf_input()}
            <div class="mb-3">
                <label class="form-label">Huidig wachtwoord</label>
                <input type="password" name="current_password" class="form-control"
                       required autocomplete="current-password">
            </div>
            <div class="mb-3">
                <label class="form-label">Nieuw wachtwoord</label>
                <input type="password" name="new_password" class="form-control" required
                       autocomplete="new-password" minlength="{config.MIN_PASSWORD_LENGTH}">
            </div>
            <div class="mb-3">
                <label class="form-label">Bevestig nieuw wachtwoord</label>
                <input type="password" name="confirm_password" class="form-control" required
                       autocomplete="new-password" minlength="{config.MIN_PASSWORD_LENGTH}">
            </div>
            <button type="submit" class="btn btn-primary">Wachtwoord wijzigen</button>
        </form>
    </div>'''


def _password_page(ctx, error: str = '', success: str = '') -> None:
    body = page_header('Wachtwoord wijzigen', ctx)
    body += '<h2 class="mt-4"><i data-lucide=key-round class=icon></i> Wachtwoord wijzigen</h2>'
    body += alert(error, 'danger') + alert(success, 'success')
    body += _password_form(ctx)
    body += page_footer()
    ctx.html(body)


def change_password(ctx) -> None:
    if ctx.method == 'GET':
        changed = 'Wachtwoord succesvol gewijzigd.' if ctx.q('changed') else ''
        _password_page(ctx, success=changed)
        return

    current = ctx.f('current_password')
    new = ctx.f('new_password')
    confirm = ctx.f('confirm_password')

    if not current or not new or not confirm:
        _password_page(ctx, error='Vul alle velden in.')
        return
    if new != confirm:
        _password_page(ctx, error='Nieuw wachtwoord en bevestiging komen niet overeen.')
        return
    if len(new) < config.MIN_PASSWORD_LENGTH:
        _password_page(ctx, error=f'Nieuw wachtwoord moet minimaal '
                                  f'{config.MIN_PASSWORD_LENGTH} tekens zijn.')
        return

    user = get_user_by_id(ctx.user_id)
    if not user:
        _password_page(ctx, error='Gebruiker niet gevonden.')
        return
    ok, _ = auth.verify_password(current, user['password'])
    if not ok:
        _password_page(ctx, error='Huidig wachtwoord is onjuist.')
        return
    if new == current:
        _password_page(ctx, error='Kies een ander wachtwoord dan je huidige.')
        return

    auth.set_password(ctx.user_id, new)
    log_action(ctx.user_id, 'update', 'users', ctx.user_id, 'wachtwoord gewijzigd')

    # Drop every other session for this account, then re-issue one for this
    # browser so changing a password actually kicks out anyone else signed in.
    auth.destroy_user_sessions(ctx.user_id)
    session_id, _ = auth.create_session(ctx.user_id)
    ctx.set_session_cookie(session_id, '/account/password?changed=1')
