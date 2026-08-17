"""Route table and dispatch.

Replaces the ~2,800-line if/elif chain in the old handle_request().

Each route declares the methods it accepts, the role it requires and whether a
CSRF token is checked. Three classes of bug become structurally impossible:

* A mutating route reachable by GET. Deletes and role toggles were plain links,
  so a prefetcher, an <img> tag or a link scanner could delete records or grant
  admin. Anything that writes is POST-only here.
* A missing CSRF check. It is on by default for every POST.
* A handler that returns without sending a response — /interactions/add and
  /comm/tasks/comment answered a GET with nothing at all. Dispatch now notices
  and returns a real status.
"""

from __future__ import annotations

from typing import Callable, Dict, NamedTuple, Tuple

from .views import account, admin, comm, customers, dashboard, gov, messages, tasks

# Role required to reach a route.
PUBLIC, USER, ADMIN, COMM, GOV = 'public', 'user', 'admin', 'comm', 'gov'


class Route(NamedTuple):
    handler: Callable
    methods: Tuple[str, ...] = ('GET',)
    role: str = USER
    csrf: bool = True


def _r(handler, methods=('GET',), role=USER, csrf=True) -> Route:
    return Route(handler, methods, role, csrf)


ROUTES: Dict[str, Route] = {
    # ── Account ──────────────────────────────────────────────────────────
    '/':                        _r(account.root, ('GET',), PUBLIC),
    '/login':                   _r(account.login, ('GET', 'POST'), PUBLIC, csrf=False),
    '/logout':                  _r(account.logout, ('POST',), USER),
    '/register':                _r(account.register, ('GET', 'POST'), PUBLIC, csrf=False),
    '/account/password':        _r(account.change_password, ('GET', 'POST'), USER),

    # ── Dashboard ────────────────────────────────────────────────────────
    '/dashboard':               _r(dashboard.show),

    # ── Customers ────────────────────────────────────────────────────────
    '/customers':               _r(customers.index),
    '/customers/view':          _r(customers.detail, ('GET', 'POST')),
    '/customers/add':           _r(customers.add, ('GET', 'POST')),
    '/customers/edit':          _r(customers.edit, ('GET', 'POST')),
    '/customers/delete':        _r(customers.delete, ('POST',)),
    '/customers/bulk':          _r(customers.bulk, ('POST',)),
    '/customers/bulk-link-empty': _r(customers.bulk_link_empty, ('POST',), ADMIN),
    '/export':                  _r(customers.export_csv, ('GET',), ADMIN),

    # ── Notes ────────────────────────────────────────────────────────────
    '/notes/delete':            _r(customers.delete_note, ('POST',)),

    # ── Interactions ─────────────────────────────────────────────────────
    '/interactions/add':        _r(customers.add_interaction, ('POST',)),
    '/interactions/edit':       _r(customers.edit_interaction, ('GET', 'POST')),
    '/interactions/delete':     _r(customers.delete_interaction, ('POST',)),

    # ── Tasks ────────────────────────────────────────────────────────────
    '/tasks/add':               _r(tasks.add, ('POST',)),
    '/tasks/resolve':           _r(tasks.resolve, ('GET', 'POST')),
    '/tasks/complete':          _r(tasks.complete, ('POST',)),
    '/tasks/delete':            _r(tasks.delete, ('POST',)),
    '/tasks/search':            _r(tasks.search),
    '/tasks/archive':           _r(tasks.archive),
    '/tasks/reopen':            _r(tasks.reopen, ('POST',)),
    '/tasks/export':            _r(tasks.export_csv, ('GET',), ADMIN),
    '/tasks/delete-all-open':   _r(tasks.delete_all_open, ('POST',), ADMIN),
    '/tasks/delete-overdue':    _r(tasks.delete_overdue, ('POST',), ADMIN),

    # ── Messages ─────────────────────────────────────────────────────────
    '/messages':                _r(messages.conversations),
    '/messages/conversation':   _r(messages.conversation, ('GET', 'POST')),
    '/messages/poll':           _r(messages.poll),

    # ── Admin ────────────────────────────────────────────────────────────
    '/users':                   _r(admin.users_list, ('GET',), ADMIN),
    '/users/add':               _r(admin.user_add, ('GET', 'POST'), ADMIN),
    '/users/delete':            _r(admin.user_delete, ('POST',), ADMIN),
    '/users/toggle-admin':      _r(admin.toggle_admin, ('POST',), ADMIN),
    '/users/toggle-comm':       _r(admin.toggle_comm, ('POST',), ADMIN),
    '/users/toggle-governance': _r(admin.toggle_governance, ('POST',), ADMIN),
    '/users/profile':           _r(admin.user_profile),
    '/fields':                  _r(admin.fields_list, ('GET',), ADMIN),
    '/fields/add':              _r(admin.field_add, ('POST',), ADMIN),
    '/fields/delete':           _r(admin.field_delete, ('POST',), ADMIN),
    '/reports':                 _r(admin.reports, ('GET',), ADMIN),
    '/audit':                   _r(admin.audit_logs, ('GET',), ADMIN),
    '/import':                  _r(admin.import_page, ('GET', 'POST')),

    # ── Communicatie ─────────────────────────────────────────────────────
    '/comm':                    _r(comm.board, ('GET',), COMM),
    '/comm/board':              _r(comm.board, ('GET',), COMM),
    '/comm/week':               _r(comm.week, ('GET',), COMM),
    '/comm/overview':           _r(comm.overview, ('GET',), COMM),
    '/comm/archived':           _r(comm.archived, ('GET',), COMM),
    '/comm/search':             _r(comm.search, ('GET',), COMM),
    '/comm/profile':            _r(comm.profile, ('GET',), COMM),
    '/comm/profile/edit':       _r(comm.profile_edit, ('GET', 'POST'), COMM),

    '/comm/goals':              _r(comm.goals, ('GET',), COMM),
    '/comm/goals/add':          _r(comm.goal_add, ('POST',), COMM),
    '/comm/goals/edit':         _r(comm.goal_edit, ('GET', 'POST'), COMM),
    '/comm/goals/complete':     _r(comm.goal_complete, ('POST',), COMM),
    '/comm/goals/reopen':       _r(comm.goal_reopen, ('POST',), COMM),
    '/comm/goals/delete':       _r(comm.goal_delete, ('POST',), COMM),

    '/comm/tasks/add':          _r(comm.task_add, ('POST',), COMM),
    '/comm/tasks/edit':         _r(comm.task_edit, ('GET', 'POST'), COMM),
    '/comm/tasks/move':         _r(comm.task_move, ('POST',), COMM),
    '/comm/tasks/delete':       _r(comm.task_delete, ('POST',), COMM),
    '/comm/tasks/comment':      _r(comm.task_comment, ('POST',), COMM),
    '/comm/tasks/archive-done': _r(comm.tasks_archive_done, ('POST',), COMM),

    '/comm/dates':              _r(comm.dates, ('GET',), COMM),
    '/comm/dates/add':          _r(comm.date_add, ('POST',), COMM),
    '/comm/dates/edit':         _r(comm.date_edit, ('GET', 'POST'), COMM),
    '/comm/dates/delete':       _r(comm.date_delete, ('POST',), COMM),
    '/comm/dates/to-task':      _r(comm.date_to_task, ('POST',), COMM),

    '/comm/content':            _r(comm.content, ('GET',), COMM),
    '/comm/content/add':        _r(comm.content_add, ('POST',), COMM),
    '/comm/content/edit':       _r(comm.content_edit, ('GET', 'POST'), COMM),
    '/comm/content/move':       _r(comm.content_move, ('POST',), COMM),
    '/comm/content/delete':     _r(comm.content_delete, ('POST',), COMM),
    '/comm/content/to-task':    _r(comm.content_to_task, ('POST',), COMM),
    '/comm/content/board-status': _r(comm.content_board_status, ('POST',), COMM),

    '/comm/events-gov':         _r(comm.events_gov, ('GET',), COMM),
    '/comm/events-gov/add':     _r(comm.events_gov_add, ('POST',), COMM),
    '/comm/events-gov/status':  _r(comm.events_gov_status, ('POST',), COMM),
    '/comm/events-gov/delete':  _r(comm.events_gov_delete, ('POST',), COMM),

    # ── Governance ───────────────────────────────────────────────────────
    '/gov/board':               _r(gov.board, ('GET',), GOV),
    '/gov/overview':            _r(gov.overview, ('GET',), GOV),
    '/gov/person':              _r(gov.person, ('GET',), GOV),
    '/gov/persons/add':         _r(gov.person_add, ('POST',), GOV),
    '/gov/persons/edit':        _r(gov.person_edit, ('GET', 'POST'), GOV),
    '/gov/persons/delete':      _r(gov.person_delete, ('POST',), GOV),
    '/gov/persons/move':        _r(gov.person_move, ('POST',), GOV),
    '/gov/progress/toggle':     _r(gov.progress_toggle, ('POST',), GOV),
    '/gov/progress/complete':   _r(gov.progress_complete, ('POST',), GOV),
    '/gov/profiles':            _r(gov.profiles, ('GET',), GOV),
    '/gov/profiles/consent':    _r(gov.profile_consent, ('POST',), GOV),
    '/gov/notes/add':           _r(gov.note_add, ('POST',), GOV),
    '/gov/notes/delete':        _r(gov.note_delete, ('POST',), GOV),
    '/gov/items/quick-edit':    _r(gov.item_quick_edit, ('POST',), GOV),
    '/gov/cards':               _r(gov.cards, ('GET',), ADMIN),
    '/gov/cards/add':           _r(gov.card_add, ('POST',), ADMIN),
    '/gov/cards/edit':          _r(gov.card_edit, ('GET', 'POST'), ADMIN),
    '/gov/cards/delete':        _r(gov.card_delete, ('POST',), ADMIN),
    '/gov/items/add':           _r(gov.item_add, ('POST',), ADMIN),
    '/gov/items/delete':        _r(gov.item_delete, ('POST',), ADMIN),
}


def _favicon(ctx) -> None:
    """Browsers ask for this on every page; answer cheaply, not with a page."""
    ctx.no_content()


ROUTES['/favicon.ico'] = _r(_favicon, ('GET',), PUBLIC)


def _authorised(ctx, role: str) -> bool:
    if role == PUBLIC:
        return True
    if not ctx.logged_in:
        return False
    if role == USER:
        return True
    if role == ADMIN:
        return ctx.is_admin
    if role == COMM:
        return ctx.is_comm
    if role == GOV:
        return ctx.is_gov
    return False


def dispatch(ctx) -> None:
    route = ROUTES.get(ctx.path.rstrip('/') or '/')
    if route is None:
        ctx.not_found()
        return

    if ctx.method not in route.methods:
        # A mutating endpoint that used to be a GET link now lands here.
        ctx.html(
            '<!doctype html><meta charset="utf-8"><title>405</title>'
            '<p>Deze actie vereist een formulier (POST).</p>'
            '<p><a href="/">Terug naar start</a></p>',
            status=405,
        )
        return

    if not _authorised(ctx, route.role):
        if not ctx.logged_in:
            ctx.redirect('/login')
        else:
            ctx.forbidden()
        return

    if ctx.method == 'POST' and route.csrf and not ctx.csrf_valid():
        ctx.forbidden('Je sessie is verlopen of het formulier is ongeldig. '
                      'Ververs de pagina en probeer het opnieuw.')
        return

    route.handler(ctx)

    if not ctx.responded:
        # A handler fell through without writing anything. Previously the
        # connection was just left hanging with an empty reply.
        ctx.redirect('/dashboard')
