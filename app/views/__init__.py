"""Page handlers, grouped per area of the app.

Every handler has the same shape::

    def handler(ctx) -> None

and answers by calling one of ctx.html / ctx.redirect / ctx.json / ctx.csv /
ctx.not_found / ctx.forbidden.
"""

__all__ = ['account', 'admin', 'comm', 'customers', 'dashboard', 'gov', 'messages', 'tasks']
