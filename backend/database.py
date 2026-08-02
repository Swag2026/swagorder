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


class BranchMapping(Base):
    """Branch -> SWAG customer mapping — stored in the database (not a local
    file) specifically so it SURVIVES redeploys. A local JSON file gets
    wiped every time Railway rebuilds the container, which was silently
    losing every human-confirmed mapping and forcing re-confirmation on
    every single deploy."""
    __tablename__ = "branch_mappings"

    branch_key = Column(String, primary_key=True)
    swag_partner_id = Column(Integer, nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


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


def get_all_branch_mappings() -> dict:
    """{ "<branch_key>": swag_partner_id } for every branch ever confirmed."""
    db = get_session()
    try:
        rows = db.query(BranchMapping).all()
        return {r.branch_key: r.swag_partner_id for r in rows}
    finally:
        db.close()


def set_branch_mapping(branch_key: str, swag_partner_id: int) -> None:
    db = get_session()
    try:
        row = db.query(BranchMapping).filter(BranchMapping.branch_key == str(branch_key)).first()
        if row:
            row.swag_partner_id = swag_partner_id
        else:
            db.add(BranchMapping(branch_key=str(branch_key), swag_partner_id=swag_partner_id))
        db.commit()
    finally:
        db.close()


def delete_branch_mapping(branch_key: str) -> None:
    db = get_session()
    try:
        db.query(BranchMapping).filter(BranchMapping.branch_key == str(branch_key)).delete()
        db.commit()
    finally:
        db.close()
