"""Green Office CRM — application package.

Layout
------
config      constants and paths
db          connection helper, schema, migrations, indexes
auth        password hashing, sessions, CSRF, roles, login throttling
ui          shared HTML chrome (header, footer, nav, badges)
importer    multipart parsing and CSV/XLSX contact import
http        per-request context object (Ctx)
router      path -> handler table and dispatch
views/      the actual page handlers, grouped per module
"""

__all__ = ['config', 'db', 'auth', 'ui', 'importer', 'http', 'router', 'views']
