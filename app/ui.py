"""Shared HTML chrome: page shell, sidebar, navigation and small badges.

page_header() takes the request context rather than a loose
(title, logged_in, username, user_id) tuple. Two call sites used to pass a
hardcoded user_id of 1, so the sidebar rendered admin links and a "Mijn profiel"
link pointing at user 1 no matter who was looking at the page.
"""

from __future__ import annotations

import html
from typing import Optional

from . import config

# Pinned rather than @latest: an unpinned CDN means a breaking upstream release
# silently changes or removes every icon in the app. Verified to resolve before
# pinning — check a new version actually exists on unpkg before bumping this.
LUCIDE_VERSION = '1.31.0'
LUCIDE_URL = f'https://unpkg.com/lucide@{LUCIDE_VERSION}/dist/umd/lucide.min.js'

# System font stack instead of an @import from Google Fonts. Keeps an EU
# intranet from making a third-party request on every page load. Swap the first
# entry back to Inter plus the @import if the exact typeface matters more.
FONT_STACK = ("Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, "
              "'Helvetica Neue', Arial, sans-serif")

STYLES = f'''
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: {FONT_STACK}; background: #F7F4F0; color: #1C1713;
           font-size: 0.9rem; line-height: 1.5; }}

    /* ── Sidebar ── */
    .sidebar {{ position: fixed; top: 0; left: 0; height: 100vh; width: 220px; background: #EFEBE5;
              border-right: 1px solid #E4DDD6; display: flex; flex-direction: column; z-index: 100;
              overflow: hidden; }}
    .sidebar-header {{ padding: 1rem 1rem 0.6rem; flex-shrink: 0; }}
    .brand {{ color: #1C1713; font-weight: 700; font-size: 0.92rem; text-decoration: none;
            letter-spacing: -0.01em; display: block; }}
    .sidebar-search {{ padding: 0 0.75rem 0.6rem; flex-shrink: 0; }}
    .sidebar-search form {{ display: flex; }}
    .sidebar-search input {{ flex: 1; min-width: 0; padding: 0.32rem 0.6rem; border: 1px solid #E4DDD6;
                           border-right: none; border-radius: 6px 0 0 6px; font-size: 0.8rem;
                           background: #fff; color: #1C1713; outline: none; font-family: inherit; }}
    .sidebar-search input:focus {{ border-color: #5C7A5A; }}
    .sidebar-search button {{ padding: 0.32rem 0.6rem; background: #5C7A5A; color: #fff; border: none;
                            border-radius: 0 6px 6px 0; cursor: pointer; display: flex;
                            align-items: center; flex-shrink: 0; }}
    .sidebar-nav {{ flex: 1; overflow-y: auto; padding: 0.1rem 0.6rem 0.5rem; }}
    .nav-section-label {{ font-size: 0.67rem; font-weight: 700; color: #B0A49A; text-transform: uppercase;
                        letter-spacing: 0.09em; padding: 0.75rem 0.5rem 0.2rem; }}
    .nav-link {{ display: flex; align-items: center; gap: 0.55rem; padding: 0.42rem 0.65rem;
               border-radius: 7px; color: #7A6E66; text-decoration: none; font-size: 0.875rem;
               font-weight: 500; transition: background 0.1s, color 0.1s; margin-bottom: 1px; }}
    .nav-link:hover {{ background: rgba(0,0,0,0.06); color: #1C1713; }}
    .nav-link.active {{ background: #D0E3CF; color: #3d5c3b; font-weight: 600; }}
    .nav-badge {{ margin-left: auto; background: #5C7A5A; color: #fff; border-radius: 10px;
                font-size: 0.68rem; font-weight: 700; padding: 0.05rem 0.45rem; min-width: 18px;
                text-align: center; display: none; }}
    .sidebar-footer {{ flex-shrink: 0; padding: 0.5rem 0.6rem 0.75rem; border-top: 1px solid #E4DDD6; }}
    .sidebar-user {{ font-size: 0.75rem; color: #B0A49A; padding: 0.25rem 0.65rem 0.35rem;
                   white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}

    /* ── Main content ── */
    .main {{ margin-left: 220px; min-height: 100vh; }}
    .container {{ max-width: 960px; margin: 0 auto; padding: 1.5rem 1.25rem; }}

    /* ── Mobile topbar ── */
    .mobile-topbar {{ display: none; position: fixed; top: 0; left: 0; right: 0; height: 50px;
                    background: #fff; border-bottom: 1px solid #E4DDD6; align-items: center;
                    padding: 0 1rem; gap: 0.75rem; z-index: 200; }}
    .hamburger {{ background: none; border: none; cursor: pointer; color: #7A6E66; padding: 0.3rem;
                display: flex; align-items: center; }}
    .sidebar-backdrop {{ display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.25); z-index: 99; }}
    @media (max-width: 768px) {{
        .mobile-topbar {{ display: flex; }}
        .main {{ margin-left: 0; padding-top: 50px; }}
        .sidebar {{ transform: translateX(-100%); transition: transform 0.22s ease; z-index: 150; }}
        .sidebar.open {{ transform: translateX(0); box-shadow: 4px 0 20px rgba(0,0,0,0.15); }}
        .sidebar-backdrop.open {{ display: block; }}
    }}

    /* ── Cards ── */
    .card {{ background: #fff; border-radius: 10px; padding: 1.25rem; margin-bottom: 1rem;
           box-shadow: 0 1px 2px rgba(0,0,0,0.05); border: 1px solid #E4DDD6; }}
    .section-title {{ font-size: 0.8rem; font-weight: 700; margin-bottom: 0.75rem; color: #7A6E66;
                    text-transform: uppercase; letter-spacing: 0.06em; }}

    /* ── Action buttons ── */
    .action-buttons a {{ display: inline-flex; align-items: center; gap: 0.35rem; border: 1px solid #E4DDD6;
                       border-radius: 6px; padding: 0.3rem 0.75rem; color: #7A6E66; background: #F7F4F0;
                       text-decoration: none; margin-right: 0.4rem; font-size: 0.82rem; font-weight: 500; }}
    .action-buttons a:hover {{ border-color: #5C7A5A; color: #5C7A5A; background: #EDF3EC; }}

    /* ── Lucide icons ── */
    .icon {{ width: 14px; height: 14px; vertical-align: -2px; display: inline-block; flex-shrink: 0;
           stroke-width: 1.75; }}
    .icon-sm {{ width: 12px; height: 12px; vertical-align: -1px; }}
    .icon-nav {{ width: 16px; height: 16px; vertical-align: -3px; stroke-width: 2; flex-shrink: 0; }}

    /* ── Tables ── */
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 0.5rem; }}
    th, td {{ padding: 0.55rem 0.75rem; text-align: left; border-bottom: 1px solid #EDE8E3; }}
    th {{ background: #F7F4F0; font-weight: 600; color: #B0A49A; font-size: 0.75rem;
        text-transform: uppercase; letter-spacing: 0.06em; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #FAFAF9; }}
    .text-end {{ text-align: right; }}
    .text-center {{ text-align: center; }}

    /* ── Buttons ── */
    .btn {{ display: inline-flex; align-items: center; gap: 0.35rem; padding: 0.35rem 0.85rem; border: none;
          border-radius: 7px; font-size: 0.85rem; font-weight: 500; cursor: pointer;
          text-decoration: none; font-family: inherit; transition: opacity 0.12s; }}
    .btn:hover {{ opacity: 0.88; }}
    .btn-primary {{ background: #5C7A5A; color: #fff; }}
    .btn-secondary {{ background: #F2EEE9; color: #7A6E66; border: 1px solid #E4DDD6; }}
    .btn-danger {{ background: #fef2f2; color: #C0392B; border: 1px solid #fecaca; }}
    .btn-sm {{ font-size: 0.78rem; padding: 0.2rem 0.6rem; }}
    .btn-link {{ background: none; color: #5C7A5A; border: none; padding: 0.35rem 0.5rem;
               font-size: 0.85rem; cursor: pointer; text-decoration: none; font-family: inherit; }}

    /* Buttons used to submit one-row POST forms (deletes, toggles). */
    .inline-form {{ display: inline; margin: 0; }}

    /* ── Forms ── */
    .form-control, .form-select {{ padding: 0.42rem 0.7rem; border: 1px solid #E4DDD6; border-radius: 7px;
                                 width: 100%; font-family: inherit; font-size: 0.875rem; background: #fff;
                                 color: #1C1713; outline: none; }}
    .form-control:focus, .form-select:focus {{ border-color: #5C7A5A; box-shadow: 0 0 0 3px rgba(92,122,90,0.10); }}
    .form-label {{ display: block; font-size: 0.8rem; font-weight: 600; color: #7A6E66; margin-bottom: 0.3rem; }}
    .mb-3 {{ margin-bottom: 0.85rem; }}
    .mt-2 {{ margin-top: 0.5rem; }}
    .mt-3 {{ margin-top: 0.75rem; }}
    .mt-4 {{ margin-top: 1rem; }}

    /* ── Alerts ── */
    .alert {{ border-radius: 6px; padding: 0.6rem 1rem; margin-bottom: 0.75rem; }}
    .alert-danger {{ background: #fdecea; color: #c62828; }}
    .alert-success {{ background: #EDF3EC; color: #5C7A5A; }}

    /* ── Utilities ── */
    .d-flex {{ display: flex; }}
    .me-2 {{ margin-right: 0.5rem; }}
    a {{ color: #5C7A5A; text-decoration: none; }}
    a:hover {{ color: #5C7A5A; }}
    h2 {{ font-size: 1.15rem; font-weight: 700; color: #1C1713; margin: 0 0 0.5rem; letter-spacing: -0.01em; }}
    .btn-outline-success {{ border: 1px solid #E4DDD6; color: #5C7A5A; background: transparent;
                          border-radius: 0 7px 7px 0; padding: 0.3rem 0.6rem; cursor: pointer; font-size: 0.85rem; }}
    .btn-outline-success:hover {{ background: #EDF3EC; }}

    /* ── Stat cards ── */
    .stat-card {{ flex: 1; min-width: 100px; text-align: center; padding: 0.8rem 0.5rem; }}
    .stat-val {{ font-size: 1.5rem; font-weight: 700; color: #1C1713; line-height: 1.1; }}
    .stat-label {{ font-size: 0.72rem; color: #B0A49A; margin-top: 0.2rem; text-transform: uppercase;
                 letter-spacing: 0.05em; }}
    .stat-row {{ display: flex; gap: 0.75rem; flex-wrap: wrap; margin-bottom: 0.75rem; }}

    /* ── Badges ── */
    .badge {{ display: inline-block; font-size: 0.7rem; border-radius: 3px; padding: 0.05rem 0.35rem;
            font-weight: 500; vertical-align: middle; }}
    .badge-danger {{ color: #C0392B; border: 1px solid #fecaca; background: #fef2f2; }}
    .badge-warn   {{ color: #B5916A; border: 1px solid #f5d6b0; background: #FEF8F0; }}
    .badge-ok     {{ color: #5C7A5A; border: 1px solid #D0E3CF; background: #EDF3EC; }}
    .badge-muted  {{ color: #B0A49A; border: 1px solid #EDE8E3; background: #F7F4F0; }}

    /* ── Task rows ── */
    .task-row {{ border-bottom: 1px solid #EDE8E3; padding: 0.7rem 0; }}
    .task-row:last-child {{ border-bottom: none; }}

    /* ── Pill toggles (accountmanagers, relatie) ── */
    .user-pill input[type=checkbox], .user-pill input[type=radio] {{ display: none; }}
    .user-pill label {{ display: inline-block; padding: 0.35rem 1rem; border-radius: 20px;
                      border: 2px solid #5C7A5A; color: #5C7A5A; cursor: pointer;
                      margin: 0.25rem 0.25rem 0.25rem 0; font-size: 0.9rem;
                      transition: background 0.15s, color 0.15s; }}
    .user-pill label:hover {{ background: #EDF3EC; }}
    .user-pill input[type=checkbox]:checked + label,
    .user-pill input[type=radio]:checked + label {{ background: #5C7A5A; color: #fff; font-weight: bold; }}
'''

# Polls for unread messages. Was every 5 seconds on a single-threaded server;
# now backs off and stops entirely while the tab is hidden.
POLL_JS = '''<script>
(function() {
    var lastUnread = -1, timer = null, INTERVAL = 20000;
    function poll() {
        if (document.hidden) return;
        fetch('/messages/poll', {credentials: 'same-origin'})
            .then(function(r){ return r.ok ? r.json() : null; })
            .then(function(data){
                if (!data) return;
                var badge = document.getElementById('msg-badge');
                if (badge) {
                    if (data.unread > 0) { badge.textContent = data.unread; badge.style.display = 'inline-block'; }
                    else { badge.style.display = 'none'; }
                }
                if (lastUnread >= 0 && data.unread > lastUnread && data.latest) { showPopup(data.latest); }
                lastUnread = data.unread;
            }).catch(function(){});
    }
    function chime() {
        try {
            var ctx = new (window.AudioContext || window.webkitAudioContext)();
            var o = ctx.createOscillator(), g = ctx.createGain();
            o.connect(g); g.connect(ctx.destination);
            o.type = 'sine'; o.frequency.value = 880;
            g.gain.setValueAtTime(0.3, ctx.currentTime);
            g.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.4);
            o.start(ctx.currentTime); o.stop(ctx.currentTime + 0.4);
        } catch(e) {}
    }
    function showPopup(msg) {
        var popup = document.getElementById('msg-popup');
        if (!popup) return;
        document.getElementById('msg-popup-from').textContent = msg.from;
        document.getElementById('msg-popup-text').textContent =
            msg.content.length > 100 ? msg.content.substring(0,100) + '...' : msg.content;
        document.getElementById('msg-popup-link').href = '/messages/conversation?with=' + msg.sender_id;
        popup.style.display = 'block';
        chime();
        clearTimeout(window._msgPopupTimer);
        window._msgPopupTimer = setTimeout(function(){ popup.style.display = 'none'; }, 8000);
    }
    window.closeMsgPopup = function() {
        var p = document.getElementById('msg-popup');
        if (p) p.style.display = 'none';
        clearTimeout(window._msgPopupTimer);
    };
    document.addEventListener('visibilitychange', function(){ if (!document.hidden) poll(); });
    poll();
    timer = setInterval(poll, INTERVAL);
})();
</script>'''


def _sidebar(ctx) -> str:
    nav = "<div class='nav-section-label'>WERKRUIMTE</div>"
    nav += "<a href='/dashboard' class='nav-link'><i data-lucide=home class=icon></i> Dashboard</a>"
    nav += "<a href='/customers' class='nav-link'><i data-lucide=users class=icon></i> Klanten</a>"
    nav += ("<a href='/messages' class='nav-link'><i data-lucide=message-circle class=icon></i> "
            "Berichten <span class='nav-badge' id='msg-badge'></span></a>")
    nav += "<a href='/tasks/search' class='nav-link'><i data-lucide=check-square class=icon></i> Taken</a>"

    if ctx.is_admin:
        nav += "<div class='nav-section-label'>BEHEER</div>"
        nav += "<a href='/users' class='nav-link'><i data-lucide=user class=icon></i> Gebruikers</a>"
        nav += "<a href='/fields' class='nav-link'><i data-lucide=sliders class=icon></i> Velden</a>"
        nav += "<a href='/reports' class='nav-link'><i data-lucide=bar-chart-2 class=icon></i> Rapporten</a>"
        nav += "<a href='/audit' class='nav-link'><i data-lucide=scroll-text class=icon></i> Audit log</a>"
    nav += "<a href='/import' class='nav-link'><i data-lucide=upload class=icon></i> Importeren</a>"

    if ctx.is_comm or ctx.is_gov:
        nav += "<div class='nav-section-label'>MODULES</div>"
        if ctx.is_comm:
            nav += "<a href='/comm/board' class='nav-link'><i data-lucide=megaphone class=icon></i> Communicatie</a>"
        if ctx.is_gov:
            nav += "<a href='/gov/board' class='nav-link'><i data-lucide=shield class=icon></i> Governance</a>"
    return nav


def _footer_links(ctx) -> str:
    return (
        f"<div class='sidebar-user'>{html.escape(ctx.username or '')}</div>"
        f"<a href='/users/profile?id={ctx.user_id}' class='nav-link'>"
        f"<i data-lucide=circle-user class=icon></i> Mijn profiel</a>"
        "<a href='/account/password' class='nav-link'>"
        "<i data-lucide=key-round class=icon></i> Wachtwoord</a>"
        f"<form method='POST' action='/logout' class='inline-form'>{ctx.csrf_input()}"
        "<button type='submit' class='nav-link' "
        "style='width:100%;background:none;border:none;cursor:pointer;font-family:inherit;'>"
        "<i data-lucide=log-out class=icon></i> Uitloggen</button></form>"
    )


def page_header(title: str, ctx) -> str:
    """Open the page: <head>, sidebar and the container the body writes into."""
    if ctx.logged_in:
        sidebar_nav = _sidebar(ctx)
        sidebar_footer = _footer_links(ctx)
        sidebar_search = (
            "<div class='sidebar-search'><form method='get' action='/customers'>"
            "<input type='search' name='q' placeholder='Klant zoeken...'>"
            "<button type='submit'><i data-lucide=search class=icon></i></button>"
            "</form></div>"
        )
        popup = (
            "<div id='msg-popup' style='display:none;position:fixed;bottom:1.5rem;right:1.5rem;"
            "z-index:9999;background:#fff;border-radius:10px;box-shadow:0 4px 20px rgba(0,0,0,0.2);"
            "padding:1rem 1.2rem;min-width:280px;max-width:360px;border-left:4px solid #5C7A5A;'>"
            "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:0.3rem;'>"
            "<strong style='color:#5C7A5A;'><i data-lucide=message-circle class=icon></i> "
            "Nieuw bericht van <span id='msg-popup-from'></span></strong>"
            "<button onclick='closeMsgPopup()' style='background:none;border:none;cursor:pointer;"
            "color:#B0A49A;'><i data-lucide=x class=icon></i></button></div>"
            "<div id='msg-popup-text' style='font-size:0.9rem;color:#1C1713;margin-bottom:0.6rem;'></div>"
            "<a id='msg-popup-link' href='/messages' style='background:#5C7A5A;color:#fff;"
            "border-radius:4px;padding:0.25rem 0.8rem;text-decoration:none;font-size:0.85rem;'>Bekijken</a>"
            "</div>"
        )
        poll = POLL_JS
    else:
        sidebar_nav = "<a href='/login' class='nav-link'><i data-lucide=log-in class=icon></i> Inloggen</a>"
        sidebar_footer = sidebar_search = popup = poll = ''

    return f'''<!doctype html>
<html lang="nl">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{html.escape(title)}</title>
    <style>{STYLES}</style>
    <script src="{LUCIDE_URL}" defer></script>
    <script>
    function sidebarOpen(){{document.getElementById('sidebar').classList.add('open');
        document.getElementById('sidebar-backdrop').classList.add('open');}}
    function sidebarClose(){{document.getElementById('sidebar').classList.remove('open');
        document.getElementById('sidebar-backdrop').classList.remove('open');}}
    document.addEventListener('DOMContentLoaded',function(){{
        var path=window.location.pathname;
        document.querySelectorAll('.nav-link[href]').forEach(function(a){{
            var h=a.getAttribute('href');
            if(h&&h!=='/'&&path.startsWith(h))a.classList.add('active');
            else if(h==='/'&&path==='/')a.classList.add('active');
        }});
    }});
    </script>
</head>
<body>
{popup}
{poll}
<div class="sidebar-backdrop" id="sidebar-backdrop" onclick="sidebarClose()"></div>
<aside class="sidebar" id="sidebar">
  <div class="sidebar-header"><a href="/" class="brand">Green Office CRM</a></div>
  {sidebar_search}
  <nav class="sidebar-nav">{sidebar_nav}</nav>
  <div class="sidebar-footer">{sidebar_footer}</div>
</aside>
<div class="mobile-topbar">
  <button class="hamburger" onclick="sidebarOpen()"><i data-lucide=menu class=icon-nav></i></button>
  <a href="/" class="brand">Green Office CRM</a>
</div>
<main class="main">
<div class="container">
'''


def page_footer() -> str:
    return ('<script>if(window.lucide)lucide.createIcons();'
            'else window.addEventListener("load",function(){if(window.lucide)lucide.createIcons();});'
            '</script></div></main></body></html>')


# ── Small shared fragments ────────────────────────────────────────────────
def alert(message: str, kind: str = 'danger') -> str:
    if not message:
        return ''
    return f'<div class="alert alert-{kind}">{html.escape(message)}</div>'


def stat_card(value, label: str, color: str = '#1C1713') -> str:
    return (f'<div class="card stat-card">'
            f'<div class="stat-val" style="color:{color};">{value}</div>'
            f'<div class="stat-label">{html.escape(label)}</div></div>')


def post_button(action: str, ctx, label: str, *, confirm: str = '',
                css: str = 'btn btn-sm btn-danger', style: str = '',
                fields: Optional[dict] = None, title: str = '') -> str:
    """A POST form styled as a button.

    Deletes and role toggles used to be plain GET links, which meant a
    prefetcher, an <img> tag or a link scanner could destroy data or grant
    admin. Every mutating action goes through one of these now, with a CSRF
    token attached.
    """
    onsubmit = f' onsubmit="return confirm({confirm!r});"' if confirm else ''
    hidden = ctx.csrf_input()
    for key, value in (fields or {}).items():
        hidden += f'<input type="hidden" name="{html.escape(str(key))}" value="{html.escape(str(value))}">'
    title_attr = f' title="{html.escape(title)}"' if title else ''
    return (f'<form method="POST" action="{action}" class="inline-form"{onsubmit}>{hidden}'
            f'<button type="submit" class="{css}" style="{style}"{title_attr}>{label}</button></form>')


def priority_badge(priority: str) -> str:
    colors = {'hoog': ('#C0392B', '&#9650; Hoog'),
              'medium': ('#B5916A', '&#9654; Medium'),
              'laag': ('#5C7A5A', '&#9660; Laag')}
    color, label = colors.get(priority or 'medium', ('#B5916A', '&#9654; Medium'))
    return f'<span style="font-size:0.7rem;background:{color};color:#fff;border-radius:3px;padding:0.1rem 0.35rem;">{label}</span>'


def comm_nav(active: str, ctx) -> str:
    tabs = [
        ('/comm/board', '<i data-lucide=menu class=icon></i> Board', 'board'),
        ('/comm/goals', '<i data-lucide=target class=icon></i> Doelen', 'goals'),
        ('/comm/week', '<i data-lucide=calendar class=icon></i> Week', 'week'),
        ('/comm/overview', '<i data-lucide=clipboard-list class=icon></i> Overzicht', 'overview'),
        ('/comm/events-gov', '<i data-lucide=flag class=icon></i> Events Gov', 'events-gov'),
        (f'/comm/profile?id={ctx.user_id}', '<i data-lucide=user class=icon></i> Mijn profiel', 'profile'),
        ('/comm/search', '<i data-lucide=search class=icon></i> Zoeken', 'search'),
        ('/comm/dates', '<i data-lucide=calendar class=icon></i> Datums', 'dates'),
        ('/comm/content', '<i data-lucide=newspaper class=icon></i> Content', 'content'),
        ('/comm/archived', '<i data-lucide=archive class=icon></i> Archief', 'archived'),
    ]
    return _nav(tabs, active, '#5C7A5A')


def gov_nav(active: str, ctx) -> str:
    tabs = [
        ('/gov/board', '<i data-lucide=menu class=icon></i> Board', 'board'),
        ('/gov/overview', '<i data-lucide=trending-up class=icon></i> Overzicht', 'overview'),
        ('/gov/profiles', '<i data-lucide=users class=icon></i> Personen', 'profiles'),
    ]
    if ctx.is_admin:
        tabs.append(('/gov/cards', '<i data-lucide=settings class=icon></i> Kaartbeheer', 'cards'))
    return _nav(tabs, active, '#7A8FA6')


def _nav(tabs, active: str, color: str) -> str:
    parts = []
    for href, label, key in tabs:
        if key == active:
            parts.append(f'<a href="{href}" style="background:{color};color:#fff;padding:0.4rem 0.85rem;'
                         f'border-radius:4px;text-decoration:none;font-weight:bold;font-size:0.9rem;">{label}</a>')
        else:
            parts.append(f'<a href="{href}" style="background:#fff;color:{color};border:2px solid {color};'
                         f'padding:0.3rem 0.85rem;border-radius:4px;text-decoration:none;font-size:0.9rem;">{label}</a>')
    return '<div style="display:flex;gap:0.4rem;flex-wrap:wrap;margin-bottom:1rem;">' + ''.join(parts) + '</div>'


def gov_phase_color(phase: str) -> str:
    return {'startpunt': '#888', 'empathize': '#7A8FA6', 'define': '#7A6E66',
            'ideate': '#B5916A', 'prototype': '#B5916A', 'test': '#5C7A5A',
            'uittreden': '#5C7A5A'}.get(phase, '#888')


def gov_phase_label(phase: str) -> str:
    return {'startpunt': 'Startpunt', 'empathize': 'Empathize', 'define': 'Define',
            'ideate': 'Ideate', 'prototype': 'Prototype', 'test': 'Test',
            'uittreden': 'Uittreden'}.get(phase, (phase or '').capitalize())


def gov_tag_pills(tags: str) -> str:
    if not tags:
        return ''
    palette = ['#7A8FA6', '#7A6E66', '#5C7A5A', '#B5916A']
    parts = []
    for i, tag in enumerate(t.strip() for t in tags.split(',')):
        if tag:
            color = palette[i % len(palette)]
            parts.append(f'<span style="font-size:0.7rem;background:{color};color:#fff;'
                         f'border-radius:10px;padding:0.1rem 0.45rem;margin-right:0.2rem;">'
                         f'{html.escape(tag)}</span>')
    return ''.join(parts)
