"""Configuration constants.

Every tunable lives here so nothing else has to reach for os.environ.
"""

from __future__ import annotations

import os

# ── Network ───────────────────────────────────────────────────────────────
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', '8000'))

# ── Database location ─────────────────────────────────────────────────────
# On Render a persistent disk is mounted at /var/data; use it when present so
# data survives deploys. Otherwise fall back to the project directory.
PERSISTENT_DIR = os.environ.get('PERSISTENT_DIR', '/var/data')
_PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if os.environ.get('CRM_DB_PATH'):
    DB_PATH = os.environ['CRM_DB_PATH']
elif os.path.isdir(PERSISTENT_DIR):
    DB_PATH = os.path.join(PERSISTENT_DIR, 'crm.db')
else:
    DB_PATH = os.path.join(_PROJECT_DIR, 'crm.db')

# ── Sessions ──────────────────────────────────────────────────────────────
SESSION_IDLE_SECONDS = int(os.environ.get('SESSION_IDLE_SECONDS', 12 * 3600))
SESSION_MAX_SECONDS = int(os.environ.get('SESSION_MAX_SECONDS', 30 * 24 * 3600))

# Only send the session cookie over HTTPS. Must be off when the app is served
# over plain HTTP on a LAN address, or the browser silently drops the cookie
# and login appears to do nothing. http://localhost is exempt in browsers.
COOKIE_SECURE = os.environ.get('COOKIE_SECURE', '1') not in ('0', 'false', 'False', '')

# ── Login throttling ──────────────────────────────────────────────────────
MAX_LOGIN_ATTEMPTS = 5
LOGIN_LOCKOUT_SECONDS = 60

# ── Uploads ───────────────────────────────────────────────────────────────
MAX_UPLOAD_BYTES = 5 * 1024 * 1024

# ── Reminders ─────────────────────────────────────────────────────────────
REMINDER_DAYS_INTERN = 180
REMINDER_DAYS_EXTERN = 60
REMINDER_INTERVAL_SECONDS = 24 * 3600

# ── Password policy ───────────────────────────────────────────────────────
MIN_PASSWORD_LENGTH = 8

# ── Domain vocabularies (single source of truth for validation) ───────────
GOV_PHASES = ['startpunt', 'empathize', 'define', 'ideate', 'prototype', 'test', 'uittreden']
GOV_PROJECT_TYPES = ['communicatie', 'werkveld', 'evenementen', 'onderwijs']
COMM_TASK_STATUSES = ['backlog', 'bezig', 'klaar']
COMM_CONTENT_STATUSES = ['idee', 'gepland', 'klaar', 'gepubliceerd']
COMM_PLATFORMS = ['instagram', 'linkedin', 'website', 'email', 'overig']
COMM_DATE_TYPES = ['event', 'deadline', 'mijlpaal']
PRIORITIES = ['hoog', 'medium', 'laag']
VERBINDING_VALUES = ['ambassadeur', 'betrokken', 'niet betrokken']
RELATION_TYPES = ['intern', 'extern']
INTERACTION_TYPES = ['call', 'email', 'message', 'meeting']
EVENTS_GOV_STATUSES = ['open', 'in_check', 'klaar']
