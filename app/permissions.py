"""Who may change what.

Before this, any signed-in account could edit or delete any customer, note,
task or interaction — there was no ownership check anywhere. The rules are
deliberately permissive (this is a small internal team) but no longer absent:

    customer  edit   admin, the creator, or a linked accountmanager
    customer  delete admin or the creator
    note      delete admin or the author
    interaction        admin or the author
    task      change admin, the assignee, or the accountmanager of the customer

Governance and communication content stays team-scoped: any member of that
team may manage it, which matches how those boards are actually used.
"""

from __future__ import annotations

from typing import Optional

from .db import query_one


def can_edit_customer(ctx, customer_id: int) -> bool:
    if ctx.is_admin:
        return True
    row = query_one('''
        SELECT c.created_by,
               (SELECT 1 FROM customer_users cu
                 WHERE cu.customer_id = c.id AND cu.user_id = ?) AS linked
          FROM customers c WHERE c.id = ?
    ''', (ctx.user_id, customer_id))
    if not row:
        return False
    return row['created_by'] == ctx.user_id or bool(row['linked'])


def can_delete_customer(ctx, customer_id: int) -> bool:
    if ctx.is_admin:
        return True
    row = query_one('SELECT created_by FROM customers WHERE id = ?', (customer_id,))
    return bool(row and row['created_by'] == ctx.user_id)


def can_manage_note(ctx, note_id: int) -> bool:
    if ctx.is_admin:
        return True
    row = query_one('SELECT user_id FROM notes WHERE id = ?', (note_id,))
    return bool(row and row['user_id'] == ctx.user_id)


def can_manage_interaction(ctx, interaction_id: int) -> bool:
    if ctx.is_admin:
        return True
    row = query_one('SELECT user_id FROM interactions WHERE id = ?', (interaction_id,))
    return bool(row and row['user_id'] == ctx.user_id)


def can_manage_task(ctx, task_id: int) -> bool:
    if ctx.is_admin:
        return True
    row = query_one('''
        SELECT t.user_id,
               (SELECT 1 FROM customer_users cu
                 WHERE cu.customer_id = t.customer_id AND cu.user_id = ?) AS linked
          FROM tasks t WHERE t.id = ?
    ''', (ctx.user_id, task_id))
    if not row:
        return False
    return row['user_id'] == ctx.user_id or bool(row['linked'])


def can_view_profile(ctx, profile_id: Optional[int]) -> bool:
    return ctx.is_admin or profile_id == ctx.user_id
