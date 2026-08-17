"""Multipart upload handling and CSV/XLSX contact import.

The old multipart parser ended each part with ``content.rstrip(b'\\r\\n--')``.
rstrip takes a *set of characters*, not a suffix, so it chewed every trailing
'-', '\\r' and '\\n' byte off the payload: a CSV whose last cell ended in '-'
lost data, and .xlsx uploads could be truncated into an unreadable archive.
Parsing now goes through the stdlib email parser, which knows the actual
multipart grammar.
"""

from __future__ import annotations

import csv
import io
import json
import re
from email.parser import BytesParser
from email.policy import default as email_policy
from typing import Dict, List, Optional, Tuple

try:
    import openpyxl  # optioneel; alleen nodig voor .xlsx
except Exception:  # pragma: no cover
    openpyxl = None

ALLOWED_BASE_COLS = {
    'name', 'email', 'phone', 'address', 'company', 'tags', 'category',
    'custom_fields', 'website', 'industry', 'company_size', 'region',
}

HEADER_MAP_NL_EN = {
    'naam': 'name', 'bedrijf': 'company', 'e-mail': 'email', 'email': 'email',
    'mail': 'email', 'telefoon': 'phone', 'telefoonnr': 'phone',
    'telefoonnummer': 'phone', 'mobiel': 'phone', 'adres': 'address',
    'straat': 'address', 'tags': 'tags', 'label': 'tags', 'type': 'category',
    'categorie': 'category', 'custom_fields': 'custom_fields', 'website': 'website',
    'branche': 'industry', 'grootte': 'company_size', 'regio': 'region',
    'extra': 'custom_fields',
}


# ── Multipart ─────────────────────────────────────────────────────────────
def parse_multipart(body: bytes, content_type: str) -> Dict[str, Tuple[Optional[str], bytes]]:
    """Return {field_name: (filename_or_None, raw_bytes)}."""
    if not body or 'multipart/form-data' not in (content_type or ''):
        return {}
    prelude = f'Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n'.encode('utf-8')
    message = BytesParser(policy=email_policy).parsebytes(prelude + body)
    if not message.is_multipart():
        return {}

    out: Dict[str, Tuple[Optional[str], bytes]] = {}
    for part in message.iter_parts():
        name = part.get_param('name', header='content-disposition')
        if not name:
            continue
        filename = part.get_filename()
        payload = part.get_payload(decode=True)
        if payload is None:
            raw = part.get_payload()
            payload = raw.encode('utf-8') if isinstance(raw, str) else b''
        out[str(name)] = (filename, payload)
    return out


# ── Header normalisation ──────────────────────────────────────────────────
def _norm(value: object) -> str:
    return (str(value) if value is not None else '').strip()


def _norm_key(value: str) -> str:
    key = (value or '').strip().lower()
    key = re.sub(r'\s+', ' ', key)
    return key.replace(':', '').replace(';', '').replace('#', '').replace('­', '')


def _map_header(header: str, dynamic_fields_lc: set) -> Optional[str]:
    key = _norm_key(header)
    if key in HEADER_MAP_NL_EN:
        return HEADER_MAP_NL_EN[key]
    if key.startswith('cf_'):
        field = key[3:]
        return f'cf_{field}' if field in dynamic_fields_lc else None
    if key in ALLOWED_BASE_COLS:
        return key
    return None


def _read_table(file_bytes: bytes, filename: str) -> Tuple[List[str], List[list]]:
    if filename.lower().endswith('.xlsx'):
        if not openpyxl:
            raise RuntimeError('Excel-import vereist openpyxl op de server.')
        workbook = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        sheet = workbook.active
        rows_iter = sheet.iter_rows(values_only=True)
        try:
            headers = [_norm(h) for h in next(rows_iter)]
        except StopIteration:
            return [], []
        return headers, [list(r) for r in rows_iter]

    text = file_bytes.decode('utf-8-sig', errors='replace')
    sample = text[:2048]
    try:
        delimiter = csv.Sniffer().sniff(sample, delimiters=[',', ';', '\t']).delimiter
    except csv.Error:
        delimiter = ';' if sample.count(';') >= sample.count(',') else ','
    data = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    if not data:
        return [], []
    return [h.strip() for h in data[0]], data[1:]


def parse_import_file(file_bytes: bytes, filename: str,
                      dynamic_fields: List[str]) -> List[Dict[str, str]]:
    """Read a CSV or XLSX file into row dicts, dropping unknown columns."""
    dyn_lc = {d.lower().strip() for d in dynamic_fields}
    headers, rows = _read_table(file_bytes, filename)
    if not headers:
        return []

    mapped = [_map_header(h, dyn_lc) for h in headers]
    result: List[Dict[str, str]] = []

    for raw_row in rows:
        row: Dict[str, str] = {}
        for idx, key in enumerate(mapped):
            if key is None or idx >= len(raw_row) or raw_row[idx] is None:
                continue
            row[key] = _norm(raw_row[idx])

        custom: Dict[str, str] = {}
        for key in list(row):
            if key.startswith('cf_'):
                custom[key[3:]] = row.pop(key)

        raw_custom = row.pop('custom_fields', '')
        if raw_custom:
            if raw_custom.strip().startswith('{'):
                try:
                    parsed = json.loads(raw_custom)
                    if isinstance(parsed, dict):
                        custom.update({str(k): str(v) for k, v in parsed.items()})
                except json.JSONDecodeError:
                    pass
            else:
                for line in raw_custom.splitlines():
                    if '=' in line:
                        k, v = line.split('=', 1)
                        custom[_norm(k)] = _norm(v)

        category = (row.get('category') or '').lower()
        if category in ('klant', 'client', 'customer'):
            row['category'] = 'klant'
        elif category in ('netwerk', 'network', 'partner', 'relatie'):
            row['category'] = 'netwerk'
        else:
            row['category'] = 'klant'

        tags = row.get('tags')
        if tags:
            sep = ';' if tags.count(';') >= tags.count(',') else ','
            row['tags'] = ','.join(t.strip() for t in tags.split(sep) if t.strip())

        if custom:
            row['__custom_json'] = json.dumps(custom)

        if not row.get('name') and not row.get('email') and not row.get('company'):
            continue
        result.append(row)
    return result


def import_rows(rows: List[Dict[str, str]], user_id: int) -> Tuple[int, List[str]]:
    """Insert parsed rows. Returns (imported_count, error_messages).

    Name uniqueness used to be resolved with a SELECT COUNT(*) per candidate
    name per row plus a commit per row — quadratic, and 500 contacts into 500
    existing rows meant a quarter of a million queries. Existing names are read
    once into a set and the whole import runs in one transaction.
    """
    from .db import connect, log_action

    imported = 0
    errors: List[str] = []
    created_ids: List[int] = []

    with connect() as conn:
        taken = {r[0] for r in conn.execute('SELECT name FROM customers')}
        for row in rows:
            custom_json = row.pop('__custom_json', None)
            base = (row.get('name') or row.get('company') or '').strip() or 'Naam onbekend'
            name = base
            suffix = 1
            while name in taken:
                suffix += 1
                name = f'{base} {suffix}'
            taken.add(name)
            try:
                cur = conn.execute(
                    'INSERT INTO customers '
                    '(name, email, phone, address, company, tags, category, '
                    ' website, industry, company_size, region, created_by, custom_fields) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (name, row.get('email') or None, row.get('phone'), row.get('address'),
                     row.get('company'), row.get('tags'), row.get('category'),
                     row.get('website'), row.get('industry'), row.get('company_size'),
                     row.get('region'), user_id, custom_json),
                )
                created_ids.append(cur.lastrowid)
                imported += 1
            except Exception as exc:  # unique email, mostly
                label = row.get('email') or name
                errors.append(f'Overgeslagen ({label}): {exc}')

    if created_ids:
        log_action(user_id, 'create', 'customers', None,
                   f'import: {len(created_ids)} klanten toegevoegd')
    return imported, errors
