"""
Tracking database for the Branch Order Portal.

This is SEPARATE from Odoo/SWAG — it's our own small record of activity on
this tool: every login and every order placed through it. This is what
powers the analytics/graph dashboard (order counts over time, per-outlet
activity, etc.) without having to re-query Odoo for everything.

Uses SQLite by default (a single file, zero setup). On Railway this file
resets whenever the service redeploys unless you attach a persistent volume
or point DATABASE_URL at a real Postgres instance (Railway can add one with
one click — then set DATABASE_URL to its connection string and this module
will use it instead, no code changes needed).
"""

import os
from datetime import datetime, timezone

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./branch_portal.db")

# Railway/Heroku-style Postgres URLs sometimes start with postgres:// —
# SQLAlchemy 2.x needs postgresql://, and we explicitly want the psycopg3
# driver (postgresql+psycopg://) rather than the default psycopg2 dialect,
# since psycopg2-binary's native libpq dependency isn't reliably available
# on Railway's runtime image.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


class LoginEvent(Base):
    __tablename__ = "login_events"

    id = Column(Integer, primary_key=True)
    branch_key = Column(String, index=True)          # LAROUCHE stock.warehouse id, as string
    branch_name = Column(String)                      # e.g. "Outfit — Prince Sultan St"
    employee_name = Column(String)
    swag_partner_id = Column(Integer, nullable=True)
    swag_partner_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)


class OrderRecord(Base):
    __tablename__ = "order_records"

    id = Column(Integer, primary_key=True)
    swag_order_id = Column(Integer, index=True)
    swag_order_name = Column(String)
    branch_key = Column(String, index=True)
    branch_name = Column(String)
    employee_name = Column(String)
    swag_partner_id = Column(Integer, index=True)
    swag_partner_name = Column(String)
    warehouse_id = Column(Integer, nullable=True)
    warehouse_name = Column(String, nullable=True)
    item_count = Column(Integer)
    amount_total = Column(Float)
    items_json = Column(Text)  # JSON list of {product_id, name, qty, price}
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)


class BranchOrderLimit(Base):
    """Optional daily/monthly spending caps admin can set per branch."""
    __tablename__ = "branch_order_limits"

    branch_key = Column(String, primary_key=True)
    daily_limit = Column(Float, nullable=True)
    monthly_limit = Column(Float, nullable=True)


class HiddenProduct(Base):
    """A product hidden from a specific branch's catalog by admin."""
    __tablename__ = "hidden_products"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, index=True)
    branch_key = Column(String, index=True)


class AdminAuditLog(Base):
    """Every meaningful admin action, for a full accountability trail."""
    __tablename__ = "admin_audit_log"

    id = Column(Integer, primary_key=True)
    action = Column(String)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)


class BranchAccessControl(Base):
    """Admin can flip a branch's access to this portal on/off without
    touching their real LAROUCHE account."""
    __tablename__ = "branch_access_control"

    branch_key = Column(String, primary_key=True)
    is_disabled = Column(Integer, default=0)  # 0/1 (SQLite-friendly boolean)
    reason = Column(String, nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ProductMinOverride(Base):
    """Per-product minimum order quantity, set by admin — overrides the
    global default (4 pieces) for specific products."""
    __tablename__ = "product_min_overrides"

    product_id = Column(Integer, primary_key=True)
    min_qty = Column(Integer)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class SystemSetting(Base):
    """Simple key/value store for global toggles (e.g. maintenance mode)."""
    __tablename__ = "system_settings"

    key = Column(String, primary_key=True)
    value = Column(String)


class OrderTemplate(Base):
    """A saved cart a branch can name and reuse later (e.g. 'Weekly
    Standard', 'Eid Stock') — one click loads the whole thing back into
    the cart instead of re-searching everything."""
    __tablename__ = "order_templates"

    id = Column(Integer, primary_key=True)
    branch_key = Column(String, index=True)
    name = Column(String)
    items_json = Column(Text)  # JSON list of {product_id, default_code, name, qty, price, packaging_id, packaging_qty}
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)


class StockReservation(Base):
    """A short-lived hold on stock while a product sits in someone's cart —
    so a second branch searching the same product sees it as (partly)
    unavailable instead of both branches racing for the same last pieces.
    Expires automatically; converted orders/removed cart items delete it."""
    __tablename__ = "stock_reservations"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, index=True)
    warehouse_id = Column(Integer, index=True)
    branch_key = Column(String, index=True)
    qty = Column(Float)
    expires_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class PendingOrder(Base):
    """An order a staff member submitted for manager approval — sits here
    until someone at the branch approves (creating the real SWAG order) or
    rejects it. Nothing touches Odoo until approved."""
    __tablename__ = "pending_orders"

    id = Column(Integer, primary_key=True)
    branch_key = Column(String, index=True)
    branch_name = Column(String)
    swag_partner_id = Column(Integer)
    requested_by = Column(String)          # employee who submitted it
    warehouse_id = Column(Integer, nullable=True)
    warehouse_name = Column(String, nullable=True)
    note = Column(Text, nullable=True)
    items_json = Column(Text)              # JSON list of {product_id, name, default_code, qty, price}
    amount_total = Column(Float)
    status = Column(String, default="pending")  # pending | approved | rejected
    decided_by = Column(String, nullable=True)
    swag_order_id = Column(Integer, nullable=True)
    swag_order_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    decided_at = Column(DateTime, nullable=True)


Base.metadata.create_all(engine)


def get_session():
    return SessionLocal()


def log_login(branch_key, branch_name, employee_name, swag_partner_id=None, swag_partner_name=None):
    db = get_session()
    try:
        db.add(LoginEvent(
            branch_key=str(branch_key),
            branch_name=branch_name,
            employee_name=employee_name,
            swag_partner_id=swag_partner_id,
            swag_partner_name=swag_partner_name,
        ))
        db.commit()
    finally:
        db.close()


def log_order(**kwargs):
    db = get_session()
    try:
        db.add(OrderRecord(**kwargs))
        db.commit()
    finally:
        db.close()


def is_branch_disabled(branch_key: str) -> tuple[bool, str | None]:
    db = get_session()
    try:
        row = db.query(BranchAccessControl).filter(BranchAccessControl.branch_key == str(branch_key)).first()
        if row and row.is_disabled:
            return True, row.reason
        return False, None
    finally:
        db.close()


def set_branch_disabled(branch_key: str, disabled: bool, reason: str | None = None):
    db = get_session()
    try:
        row = db.query(BranchAccessControl).filter(BranchAccessControl.branch_key == str(branch_key)).first()
        if row:
            row.is_disabled = 1 if disabled else 0
            row.reason = reason
            row.updated_at = datetime.now(timezone.utc)
        else:
            db.add(BranchAccessControl(branch_key=str(branch_key), is_disabled=1 if disabled else 0, reason=reason))
        db.commit()
    finally:
        db.close()


def get_product_min_qty(product_id: int, default: int = 4) -> int:
    db = get_session()
    try:
        row = db.query(ProductMinOverride).filter(ProductMinOverride.product_id == product_id).first()
        return row.min_qty if row else default
    finally:
        db.close()


def get_all_min_overrides() -> dict:
    db = get_session()
    try:
        rows = db.query(ProductMinOverride).all()
        return {r.product_id: r.min_qty for r in rows}
    finally:
        db.close()


def set_product_min_qty(product_id: int, min_qty: int):
    db = get_session()
    try:
        row = db.query(ProductMinOverride).filter(ProductMinOverride.product_id == product_id).first()
        if row:
            row.min_qty = min_qty
            row.updated_at = datetime.now(timezone.utc)
        else:
            db.add(ProductMinOverride(product_id=product_id, min_qty=min_qty))
        db.commit()
    finally:
        db.close()


def delete_product_min_override(product_id: int):
    db = get_session()
    try:
        db.query(ProductMinOverride).filter(ProductMinOverride.product_id == product_id).delete()
        db.commit()
    finally:
        db.close()


def get_setting(key: str, default: str | None = None) -> str | None:
    db = get_session()
    try:
        row = db.query(SystemSetting).filter(SystemSetting.key == key).first()
        return row.value if row else default
    finally:
        db.close()


def set_setting(key: str, value: str):
    db = get_session()
    try:
        row = db.query(SystemSetting).filter(SystemSetting.key == key).first()
        if row:
            row.value = value
        else:
            db.add(SystemSetting(key=key, value=value))
        db.commit()
    finally:
        db.close()


def log_admin_action(action: str, details: str = ""):
    db = get_session()
    try:
        db.add(AdminAuditLog(action=action, details=details))
        db.commit()
    finally:
        db.close()


def get_branch_limits(branch_key: str):
    db = get_session()
    try:
        row = db.query(BranchOrderLimit).filter(BranchOrderLimit.branch_key == str(branch_key)).first()
        if not row:
            return None, None
        return row.daily_limit, row.monthly_limit
    finally:
        db.close()


def set_branch_limits(branch_key: str, daily_limit, monthly_limit):
    db = get_session()
    try:
        row = db.query(BranchOrderLimit).filter(BranchOrderLimit.branch_key == str(branch_key)).first()
        if row:
            row.daily_limit = daily_limit
            row.monthly_limit = monthly_limit
        else:
            db.add(BranchOrderLimit(branch_key=str(branch_key), daily_limit=daily_limit, monthly_limit=monthly_limit))
        db.commit()
    finally:
        db.close()


def is_product_hidden(product_id: int, branch_key: str) -> bool:
    db = get_session()
    try:
        row = (
            db.query(HiddenProduct)
            .filter(HiddenProduct.product_id == product_id, HiddenProduct.branch_key == str(branch_key))
            .first()
        )
        return row is not None
    finally:
        db.close()


def get_hidden_product_ids_for_branch(branch_key: str) -> set:
    db = get_session()
    try:
        rows = db.query(HiddenProduct).filter(HiddenProduct.branch_key == str(branch_key)).all()
        return {r.product_id for r in rows}
    finally:
        db.close()


def hide_product(product_id: int, branch_key: str):
    db = get_session()
    try:
        exists = (
            db.query(HiddenProduct)
            .filter(HiddenProduct.product_id == product_id, HiddenProduct.branch_key == str(branch_key))
            .first()
        )
        if not exists:
            db.add(HiddenProduct(product_id=product_id, branch_key=str(branch_key)))
            db.commit()
    finally:
        db.close()


def unhide_product(product_id: int, branch_key: str):
    db = get_session()
    try:
        db.query(HiddenProduct).filter(
            HiddenProduct.product_id == product_id, HiddenProduct.branch_key == str(branch_key)
        ).delete()
        db.commit()
    finally:
        db.close()


def list_hidden_products():
    db = get_session()
    try:
        rows = db.query(HiddenProduct).all()
        return [{"product_id": r.product_id, "branch_key": r.branch_key} for r in rows]
    finally:
        db.close()
