"""
SWAG Branch Order Portal — Backend  (Performance-Optimised Build)
-----------------------------------
Each branch employee logs in with their OWN LAROUCHE Odoo credentials (for
accountability — every order can be traced to who placed it). Their login
also tells us which company/brand they belong to, so they only ever see
their own company's outlets (`stock.warehouse`) — never another brand's.

A separate LAROUCHE "admin" service account (server-side only) is used just
to reliably list warehouses (avoids per-user Odoo access-right edge cases);
it never determines identity or is exposed to the browser.

Two Odoo systems:
  1. LAROUCHE Odoo — employee login (identity) + warehouse listing (outlets).
  2. SWAG Odoo     — the product catalog lives here and the draft Sales
     Order gets created here, using SWAG's own service account (branches
     never have their own SWAG login). SWAG's own warehouses are also listed
     here so the branch can pick which SWAG warehouse fulfills the order.

Because a LAROUCHE outlet (res.company + stock.warehouse) isn't the same
record as its SWAG customer (res.partner), the backend needs a mapping
between the two — kept in `branch_partner_map.json` (keyed by LAROUCHE
stock.warehouse id). It's built automatically where possible (VAT/email/
phone/location-keyword matching) and falls back to a one-time human pick
(with full search) otherwise.

Deploy: Railway (this folder as the service root).

Env vars required (Railway → Variables):
    LAROUCHE_URL            e.g. https://outfit.larache.sa  (NO trailing /odoo)
    LAROUCHE_DB             e.g. db3
    LAROUCHE_ADMIN_USER     LAROUCHE admin-style account email (warehouse listing only)
    LAROUCHE_ADMIN_API_KEY  that account's password/API key
    SWAG_URL                e.g. https://db.swag.com.sa     (NO trailing /odoo)
    SWAG_DB                 e.g. db2
    SWAG_USER               SWAG service account email (e.g. sharaf@swag.com.sa)
    SWAG_API_KEY            that service account's API key
    JWT_SECRET              any long random string
    ALLOWED_ORIGINS         comma-separated list, e.g. https://swag-branch-orders.netlify.app
"""

import os
import json
import time
import xmlrpc.client
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any

import jwt
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

import database

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LAROUCHE_URL = os.environ.get("LAROUCHE_URL", "").rstrip("/")
LAROUCHE_DB  = os.environ.get("LAROUCHE_DB", "")
LAROUCHE_ADMIN_USER    = os.environ.get("LAROUCHE_ADMIN_USER", "")
LAROUCHE_ADMIN_API_KEY = os.environ.get("LAROUCHE_ADMIN_API_KEY", "")

SWAG_URL     = os.environ.get("SWAG_URL", "").rstrip("/")
SWAG_DB      = os.environ.get("SWAG_DB", "")
SWAG_USER    = os.environ.get("SWAG_USER", "")
SWAG_API_KEY = os.environ.get("SWAG_API_KEY", "")

JWT_SECRET      = os.environ.get("JWT_SECRET", "change-me-in-railway-variables")
ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
ADMIN_PASSWORD  = os.environ.get("ADMIN_PASSWORD", "")

MAP_FILE = Path(__file__).parent / "branch_partner_map.json"

# Thread pool shared across all requests — XML-RPC calls release the GIL so
# threads give real parallelism here.
_executor = ThreadPoolExecutor(max_workers=12)


# ─────────────────────────────────────────────────────────────────────────────
# SIMPLE TTL CACHE
# ─────────────────────────────────────────────────────────────────────────────
class _TTLCache:
    """Thread-safe in-memory cache with per-key TTL (seconds)."""

    def __init__(self):
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = Lock()

    def get(self, key: str):
        with self._lock:
            entry = self._store.get(key)
            if entry and time.monotonic() < entry[1]:
                return entry[0]
            return None  # missing or expired

    def set(self, key: str, value: Any, ttl: int):
        with self._lock:
            self._store[key] = (value, time.monotonic() + ttl)

    def delete(self, key: str):
        with self._lock:
            self._store.pop(key, None)

    def clear_prefix(self, prefix: str):
        with self._lock:
            keys = [k for k in self._store if k.startswith(prefix)]
            for k in keys:
                del self._store[k]


_cache = _TTLCache()

# Cache TTLs (seconds)
TTL_SWAG_WAREHOUSES   = 1800   # 30 min  — warehouse list rarely changes
TTL_PRODUCTS          = 300    # 5 min   — product list + prices
TTL_PACKAGINGS        = 1800   # 30 min  — UOM/packaging config rarely changes
TTL_PRODUCT_STOCK     = 120    # 2 min   — stock moves fast
TTL_MY_SHOP_STOCK     = 120    # 2 min


# ─────────────────────────────────────────────────────────────────────────────
# BRANCH-PARTNER MAP
# ─────────────────────────────────────────────────────────────────────────────
def load_branch_partner_map() -> dict:
    """{ "<larache_warehouse_id>": swag_partner_id }"""
    if not MAP_FILE.exists():
        return {}
    try:
        raw = json.loads(MAP_FILE.read_text())
        return {str(k): int(v) for k, v in raw.items()}
    except Exception:
        return {}


def save_branch_partner_map(mapping: dict) -> None:
    try:
        MAP_FILE.write_text(json.dumps(mapping, indent=2, ensure_ascii=False))
    except Exception:
        pass


_resolved_cache: dict[str, int] = {}


# ─────────────────────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(title="SWAG Branch Order Portal API")

app.add_middleware(GZipMiddleware, minimum_size=500)   # compress JSON responses
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# ODOO HELPERS  (each call creates its own proxy — xmlrpc proxies are not
# thread-safe when shared, so we create per-call instances)
# ─────────────────────────────────────────────────────────────────────────────
def _proxy(url: str, endpoint: str):
    return xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/{endpoint}", allow_none=True)


def laroche_authenticate(email: str, password: str) -> int:
    if not (LAROUCHE_URL and LAROUCHE_DB):
        raise HTTPException(500, "Server is missing LAROUCHE_URL / LAROUCHE_DB configuration.")
    try:
        uid = _proxy(LAROUCHE_URL, "common").authenticate(LAROUCHE_DB, email, password, {})
    except Exception as e:
        raise HTTPException(502, f"Could not reach LAROUCHE Odoo: {e}")
    if not uid:
        raise HTTPException(401, "Invalid email or password.")
    return uid


def laroche_execute_as(uid: int, password: str, model: str, method: str, args: list, kwargs: dict | None = None):
    try:
        return _proxy(LAROUCHE_URL, "object").execute_kw(
            LAROUCHE_DB, uid, password, model, method, args, kwargs or {}
        )
    except xmlrpc.client.Fault as e:
        raise HTTPException(400, f"LAROUCHE Odoo error: {e.faultString}")
    except Exception as e:
        raise HTTPException(502, f"Could not reach LAROUCHE Odoo: {e}")


_laroche_admin_uid_cache: int | None = None


def laroche_admin_uid() -> int:
    global _laroche_admin_uid_cache
    if _laroche_admin_uid_cache:
        return _laroche_admin_uid_cache
    if not (LAROUCHE_URL and LAROUCHE_DB and LAROUCHE_ADMIN_USER and LAROUCHE_ADMIN_API_KEY):
        raise HTTPException(500, "Server is missing LAROUCHE_ADMIN_USER / LAROUCHE_ADMIN_API_KEY.")
    try:
        uid = _proxy(LAROUCHE_URL, "common").authenticate(
            LAROUCHE_DB, LAROUCHE_ADMIN_USER, LAROUCHE_ADMIN_API_KEY, {}
        )
    except Exception as e:
        raise HTTPException(502, f"Could not reach LAROUCHE Odoo: {e}")
    if not uid:
        raise HTTPException(500, "LAROUCHE admin account credentials are invalid.")
    _laroche_admin_uid_cache = uid
    return uid


def laroche_admin_execute(model: str, method: str, args: list, kwargs: dict | None = None):
    try:
        return _proxy(LAROUCHE_URL, "object").execute_kw(
            LAROUCHE_DB, laroche_admin_uid(), LAROUCHE_ADMIN_API_KEY, model, method, args, kwargs or {}
        )
    except xmlrpc.client.Fault as e:
        raise HTTPException(400, f"LAROUCHE Odoo error: {e.faultString}")
    except Exception as e:
        raise HTTPException(502, f"Could not reach LAROUCHE Odoo: {e}")


_swag_uid_cache: int | None = None


def swag_uid() -> int:
    global _swag_uid_cache
    if _swag_uid_cache:
        return _swag_uid_cache
    if not (SWAG_URL and SWAG_DB and SWAG_USER and SWAG_API_KEY):
        raise HTTPException(500, "Server is missing SWAG_URL / SWAG_DB / SWAG_USER / SWAG_API_KEY.")
    try:
        uid = _proxy(SWAG_URL, "common").authenticate(SWAG_DB, SWAG_USER, SWAG_API_KEY, {})
    except Exception as e:
        raise HTTPException(502, f"Could not reach SWAG Odoo: {e}")
    if not uid:
        raise HTTPException(500, "SWAG service account credentials are invalid.")
    _swag_uid_cache = uid
    return uid


def swag_execute(model: str, method: str, args: list, kwargs: dict | None = None):
    try:
        return _proxy(SWAG_URL, "object").execute_kw(
            SWAG_DB, swag_uid(), SWAG_API_KEY, model, method, args, kwargs or {}
        )
    except xmlrpc.client.Fault as e:
        raise HTTPException(400, f"SWAG Odoo error: {e.faultString}")
    except Exception as e:
        raise HTTPException(502, f"Could not reach SWAG Odoo: {e}")


_field_label_cache: dict[str, str | None] = {}


def find_field_by_label(model: str, keywords: list[str]) -> str | None:
    cache_key = f"{model}:{'|'.join(keywords)}"
    if cache_key in _field_label_cache:
        return _field_label_cache[cache_key]
    try:
        meta = swag_execute(model, "fields_get", [], {"attributes": ["string"]})
    except Exception:
        _field_label_cache[cache_key] = None
        return None
    keywords_lower = [k.lower() for k in keywords]
    found = None
    for fname, finfo in (meta or {}).items():
        label = (finfo.get("string") or "").lower()
        if any(kw in label for kw in keywords_lower):
            found = fname
            break
    _field_label_cache[cache_key] = found
    return found


def _m2o_name(value):
    return value[1] if value else None


def _norm(s: str | None) -> str:
    return " ".join((s or "").strip().lower().split())


_GENERIC_NAME_WORDS = {
    "شركة", "التجارية", "فرع", "فروع", "مؤسسة", "المحدودة", "مجموعة", "متجر", "معرض",
    "مستودع", "للأزياء", "التجاري", "مصنع", "محل", "مكتب", "co", "company", "trading",
    "branch", "store", "shop", "warehouse", "ltd", "llc", "est", "establishment",
}


def _brand_keywords(name: str | None) -> set[str]:
    words = _norm(name).split()
    return {w for w in words if w not in _GENERIC_NAME_WORDS and len(w) >= 3}


def _clean_warehouse_name(name: str) -> str:
    if "/" in (name or ""):
        return name.split("/", 1)[1].strip()
    return (name or "").strip()


def auto_resolve_swag_partner(branch_info: dict):
    identifiers = []
    if branch_info.get("vat"):
        identifiers.append(("vat", branch_info["vat"]))
    if branch_info.get("email"):
        identifiers.append(("email", branch_info["email"]))
    if branch_info.get("mobile"):
        identifiers.append(("mobile", branch_info["mobile"]))
    if branch_info.get("phone"):
        identifiers.append(("phone", branch_info["phone"]))
    if branch_info.get("name"):
        identifiers.append(("name", branch_info["name"]))

    if not identifiers:
        return None, None, None, []

    or_terms = [[field, "=", value] for field, value in identifiers]
    combined_domain = (["|"] * (len(or_terms) - 1)) + or_terms

    all_matches = swag_execute(
        "res.partner", "search_read", [combined_domain],
        {"fields": ["id", "name", "city", "street", "email", "phone", "mobile", "vat"]},
    )

    best_candidates: list[dict] = []
    for label, value in identifiers:
        matches = [m for m in all_matches if m.get(label) == value]
        if len(matches) == 1:
            return matches[0]["id"], matches[0]["name"], label, []
        if len(matches) > 1:
            candidates = [{"id": m["id"], "name": m["name"], "city": m.get("city")} for m in matches]
            if not best_candidates or len(candidates) < len(best_candidates):
                best_candidates = candidates

    if best_candidates:
        brand_words = _brand_keywords(branch_info.get("company_name"))
        keywords = _brand_keywords(branch_info.get("name")) - brand_words
        if keywords:
            scored = [(c, len(keywords & set(_norm(c["name"]).split()))) for c in best_candidates]
            scored = [(c, score) for c, score in scored if score > 0]
            if scored:
                scored.sort(key=lambda cs: cs[1], reverse=True)
                top_score = scored[0][1]
                top_matches = [c for c, score in scored if score == top_score]
                if len(top_matches) == 1:
                    winner = top_matches[0]
                    return winner["id"], winner["name"], "location keywords", []
                best_candidates = [c for c, _ in scored]

    return None, None, None, best_candidates


# ─────────────────────────────────────────────────────────────────────────────
# PARALLEL HELPER
# ─────────────────────────────────────────────────────────────────────────────
def run_parallel(**fns) -> dict:
    """Run multiple zero-argument callables in parallel and return a dict of
    results keyed by the same names.  Any exception is re-raised from here.

    Usage:
        results = run_parallel(
            warehouses=lambda: swag_execute(...),
            partner=lambda: swag_execute(...),
        )
        warehouses = results["warehouses"]
    """
    futures = {_executor.submit(fn): name for name, fn in fns.items()}
    out = {}
    exc = None
    for future in as_completed(futures):
        name = futures[future]
        try:
            out[name] = future.result()
        except Exception as e:
            exc = e
    if exc:
        raise exc
    return out


# ─────────────────────────────────────────────────────────────────────────────
# AUTH TOKENS
# ─────────────────────────────────────────────────────────────────────────────
def make_login_token(email: str, employee_name: str, company_id: int, company_name: str) -> str:
    payload = {
        "type": "login",
        "email": email,
        "employee_name": employee_name,
        "company_id": company_id,
        "company_name": company_name,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=30),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def read_login_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Your session expired — please log in again.")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid login token.")
    if payload.get("type") != "login":
        raise HTTPException(401, "Invalid login token.")
    return payload


def make_token(
    branch_key: str, branch_name: str, swag_partner_id: int, swag_partner_name: str,
    employee_name: str, default_warehouse_id: int | None = None,
    default_warehouse_name: str | None = None,
) -> str:
    payload = {
        "type": "session",
        "branch_key": branch_key,
        "branch_name": branch_name,
        "swag_partner_id": swag_partner_id,
        "swag_partner_name": swag_partner_name,
        "employee_name": employee_name,
        "default_warehouse_id": default_warehouse_id,
        "default_warehouse_name": default_warehouse_name,
        "exp": datetime.now(timezone.utc) + timedelta(hours=10),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def make_select_token(branch_key: str, branch_name: str, candidate_ids: list[int], employee_name: str) -> str:
    payload = {
        "type": "branch_select",
        "branch_key": branch_key,
        "branch_name": branch_name,
        "candidate_ids": candidate_ids,
        "employee_name": employee_name,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=15),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def read_token(authorization: str | None) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or invalid Authorization header.")
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Session expired, please log in again.")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid session token.")
    if payload.get("type") != "session":
        raise HTTPException(401, "Invalid session token.")
    return payload


def make_admin_token() -> str:
    payload = {
        "type": "admin",
        "exp": datetime.now(timezone.utc) + timedelta(hours=12),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def read_admin_token(authorization: str | None) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing or invalid Authorization header.")
    token = authorization.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Admin session expired, please log in again.")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid admin session token.")
    if payload.get("type") != "admin":
        raise HTTPException(401, "Invalid admin session token.")
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────
class AdminLoginRequest(BaseModel):
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class SelectBranchRequest(BaseModel):
    login_token: str
    warehouse_id: int


class BranchSearchRequest(BaseModel):
    select_token: str
    q: str = ""


class ConfirmBranchRequest(BaseModel):
    select_token: str
    partner_id: int


class CartItem(BaseModel):
    product_id: int
    qty: float
    packaging_id: int | None = None
    packaging_qty: float | None = None


class OrderRequest(BaseModel):
    items: list[CartItem]
    warehouse_id: int | None = None
    note: str | None = None


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/login")
def login(body: LoginRequest):
    """Employee logs in with THEIR OWN LAROUCHE credentials.
    Optimised: fields_get + user read run in parallel after auth."""
    email = body.email.strip()
    uid = laroche_authenticate(email, body.password)

    # Check available fields and read user record in parallel
    def _get_fields():
        wanted_fields = ["name", "company_id", "property_warehouse_id"]
        try:
            meta = laroche_execute_as(uid, body.password, "res.users", "fields_get", [], {"attributes": []})
            return [f for f in wanted_fields if f in meta]
        except Exception:
            return ["name", "company_id"]

    def _read_user(fields):
        return laroche_execute_as(uid, body.password, "res.users", "read", [[uid]], {"fields": fields})

    existing_fields = _get_fields()
    user_recs = _read_user(existing_fields)

    if not user_recs:
        raise HTTPException(500, "Could not load your LAROUCHE user profile.")
    employee_name = user_recs[0]["name"]
    company = user_recs[0].get("company_id") or False
    if not company:
        raise HTTPException(500, "Your LAROUCHE account has no company/brand assigned — ask an admin to check it.")
    company_id, company_name = company[0], company[1]

    own_warehouse = user_recs[0].get("property_warehouse_id") or False
    if own_warehouse:
        result = _resolve_warehouse_to_session(own_warehouse[0], company_id, company_name, employee_name)
        result["single_warehouse"] = True
        return result

    login_token = make_login_token(email, employee_name, company_id, company_name)
    return {
        "login_token": login_token,
        "employee_name": employee_name,
        "company_id": company_id,
        "company_name": company_name,
        "single_warehouse": False,
    }


@app.get("/api/warehouses")
def list_warehouses(company_id: int, login_token: str):
    payload = read_login_token(login_token)
    if payload["company_id"] != company_id:
        raise HTTPException(403, "You can only view outlets for your own company.")
    warehouses = laroche_admin_execute(
        "stock.warehouse", "search_read", [[["company_id", "=", company_id]]],
        {"fields": ["id", "name", "code"], "order": "name asc"},
    )
    return {"warehouses": warehouses}


def auto_resolve_swag_warehouse(branch_info: dict, swag_partner_id: int | None = None):
    if swag_partner_id:
        try:
            direct = swag_execute(
                "stock.warehouse", "search_read", [[["partner_id", "=", swag_partner_id]]],
                {"fields": ["id", "name"]},
            )
            if len(direct) == 1:
                return direct[0]["id"], direct[0]["name"]
        except Exception:
            pass

    brand_words = _brand_keywords(branch_info.get("company_name"))
    keywords = _brand_keywords(branch_info.get("name")) - brand_words
    if not keywords:
        return None, None

    all_warehouses = swag_execute("stock.warehouse", "search_read", [[]], {"fields": ["id", "name"]})
    scored = [(w, len(keywords & set(_norm(_clean_warehouse_name(w["name"])).split()))) for w in all_warehouses]
    scored = [(w, s) for w, s in scored if s > 0]
    if not scored:
        return None, None
    scored.sort(key=lambda ws: ws[1], reverse=True)
    top = scored[0][1]
    winners = [w for w, s in scored if s == top]
    if len(winners) == 1:
        return winners[0]["id"], winners[0]["name"]
    return None, None


def _resolve_branch(branch_id: int, branch_name: str, branch_info: dict, employee_name: str):
    key = str(branch_id)
    mapping = load_branch_partner_map()
    swag_partner_id = mapping.get(key) or _resolved_cache.get(key)
    matched_on = "saved mapping" if swag_partner_id else None

    if not swag_partner_id:
        resolved_id, resolved_name, matched_on, candidates = auto_resolve_swag_partner(branch_info)
        if not resolved_id:
            select_token = make_select_token(key, branch_name, [c["id"] for c in candidates], employee_name)
            return {
                "selection_required": True,
                "select_token": select_token,
                "branch_name": branch_name,
                "candidates": candidates[:40],
            }
        swag_partner_id = resolved_id
        _resolved_cache[key] = swag_partner_id
        mapping[key] = swag_partner_id
        save_branch_partner_map(mapping)

    # Fetch partner name and default warehouse IN PARALLEL
    results = run_parallel(
        partner=lambda: swag_execute("res.partner", "read", [[swag_partner_id]], {"fields": ["name"]}),
        warehouse=lambda: auto_resolve_swag_warehouse(branch_info, swag_partner_id),
    )

    partner_recs = results["partner"]
    if not partner_recs:
        raise HTTPException(500, f"Mapped SWAG partner id {swag_partner_id} was not found in SWAG Odoo.")
    swag_partner_name = partner_recs[0]["name"]
    default_warehouse_id, default_warehouse_name = results["warehouse"]

    token = make_token(
        key, branch_name, swag_partner_id, swag_partner_name, employee_name,
        default_warehouse_id, default_warehouse_name,
    )
    database.log_login(key, branch_name, employee_name, swag_partner_id, swag_partner_name)
    return {
        "token": token,
        "partner_name": swag_partner_name,
        "branch_name": branch_name,
        "matched_on": matched_on,
        "default_warehouse_name": default_warehouse_name,
    }


def _resolve_warehouse_to_session(warehouse_id: int, company_id: int, company_name: str, employee_name: str):
    warehouses = laroche_admin_execute(
        "stock.warehouse", "read", [[warehouse_id]], {"fields": ["name", "company_id"]}
    )
    if not warehouses:
        raise HTTPException(404, "Outlet not found.")
    warehouse = warehouses[0]
    if not warehouse.get("company_id") or warehouse["company_id"][0] != company_id:
        raise HTTPException(403, "This outlet doesn't belong to your company.")

    outlet_name = _clean_warehouse_name(warehouse["name"])
    branch_name = f"{company_name} — {outlet_name}"

    # Fetch company partner info (for email/vat matching) in parallel with branch resolve prep
    branch_info: dict = {"name": outlet_name, "company_name": company_name}
    company = laroche_admin_execute("res.company", "read", [[company_id]], {"fields": ["partner_id"]})
    company_partner = company[0].get("partner_id") if company else False
    if company_partner:
        p = laroche_admin_execute(
            "res.partner", "read", [[company_partner[0]]],
            {"fields": ["email", "phone", "mobile", "vat"]},
        )
        if p:
            branch_info.update({k: p[0].get(k) for k in ("email", "phone", "mobile", "vat")})

    return _resolve_branch(warehouse_id, branch_name, branch_info, employee_name)


@app.post("/api/select-branch")
def select_branch(body: SelectBranchRequest):
    login_payload = read_login_token(body.login_token)
    return _resolve_warehouse_to_session(
        body.warehouse_id, login_payload["company_id"], login_payload["company_name"], login_payload["employee_name"]
    )


@app.post("/api/branch-select/search")
def branch_select_search(body: BranchSearchRequest):
    try:
        payload = jwt.decode(body.select_token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "This selection expired — please pick your outlet again.")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid selection token.")
    if payload.get("type") != "branch_select":
        raise HTTPException(401, "Invalid selection token.")

    q = body.q.strip()
    domain = [["name", "ilike", q]] if q else []
    results = swag_execute(
        "res.partner", "search_read", [domain],
        {"fields": ["id", "name", "city", "street"], "limit": 25, "order": "name asc"},
    )
    return {"results": results}


@app.post("/api/confirm-branch")
def confirm_branch(body: ConfirmBranchRequest):
    try:
        payload = jwt.decode(body.select_token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "This selection expired — please pick your outlet again.")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid selection token.")
    if payload.get("type") != "branch_select":
        raise HTTPException(401, "Invalid selection token.")

    partner_recs = swag_execute("res.partner", "read", [[body.partner_id]], {"fields": ["name"]})
    if not partner_recs:
        raise HTTPException(500, f"SWAG partner id {body.partner_id} was not found.")
    swag_partner_name = partner_recs[0]["name"]

    key = payload["branch_key"]
    mapping = load_branch_partner_map()
    mapping[key] = body.partner_id
    _resolved_cache[key] = body.partner_id
    save_branch_partner_map(mapping)

    branch_name = payload.get("branch_name", key)
    employee_name = payload.get("employee_name", "")

    company_part, _, outlet_part = branch_name.partition(" — ")
    default_warehouse_id, default_warehouse_name = auto_resolve_swag_warehouse(
        {"name": outlet_part or branch_name, "company_name": company_part}, body.partner_id
    )

    token = make_token(
        key, branch_name, body.partner_id, swag_partner_name, employee_name,
        default_warehouse_id, default_warehouse_name,
    )
    database.log_login(key, branch_name, employee_name, body.partner_id, swag_partner_name)
    return {
        "token": token,
        "partner_name": swag_partner_name,
        "branch_name": branch_name,
        "matched_on": "confirmed by human",
        "default_warehouse_name": default_warehouse_name,
    }


@app.get("/api/me")
def me(authorization: str | None = Header(None)):
    payload = read_token(authorization)
    return {
        "partner_name": payload["swag_partner_name"],
        "branch_name": payload["branch_name"],
        "employee_name": payload.get("employee_name"),
        "default_warehouse_id": payload.get("default_warehouse_id"),
        "default_warehouse_name": payload.get("default_warehouse_name"),
    }


@app.post("/api/report-wrong-customer")
def report_wrong_customer(authorization: str | None = Header(None)):
    payload = read_token(authorization)
    key = payload["branch_key"]
    mapping = load_branch_partner_map()
    if key in mapping:
        del mapping[key]
        save_branch_partner_map(mapping)
    _resolved_cache.pop(key, None)
    return {"ok": True, "message": "Mapping cleared — please log out and log back in to re-verify."}


@app.get("/api/products")
def search_products(q: str = "", limit: int = 40, authorization: str | None = Header(None)):
    read_token(authorization)
    q = (q or "").strip()

    # Cache keyed by (query, limit) — empty searches are cached, specific
    # queries are also cached for 5 min to speed up repeated typing.
    cache_key = f"products:{q}:{limit}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    domain = [["sale_ok", "=", True]]
    if q:
        domain = ["&", domain[0], "|", ["default_code", "ilike", q], ["name", "ilike", q]]
    fields = ["id", "default_code", "name", "list_price", "qty_available", "uom_id"]
    products = swag_execute(
        "product.product", "search_read",
        [domain], {"fields": fields, "limit": limit, "order": "default_code asc"},
    )
    result = {"products": products}
    _cache.set(cache_key, result, TTL_PRODUCTS)
    return result


@app.get("/api/products/{product_id}/image")
def product_image(product_id: int, authorization: str | None = Header(None)):
    read_token(authorization)
    cache_key = f"product_image:{product_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    recs = swag_execute("product.product", "read", [[product_id]], {"fields": ["image_128"]})
    image_b64 = recs[0].get("image_128") if recs else None
    result = {"image_base64": image_b64}
    # Images are static — cache for 1 hour
    _cache.set(cache_key, result, 3600)
    return result


@app.get("/api/products/{product_id}/stock")
def product_stock_by_warehouse(product_id: int, authorization: str | None = Header(None)):
    """Per-warehouse on-hand stock for a product.  Cached 2 min."""
    read_token(authorization)
    cache_key = f"stock:{product_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    quants = swag_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    if not quants:
        result = {"stock_by_warehouse": []}
        _cache.set(cache_key, result, TTL_PRODUCT_STOCK)
        return result

    location_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
    locations = swag_execute("stock.location", "read", [location_ids], {"fields": ["warehouse_id"]})
    loc_to_wh = {loc["id"]: loc.get("warehouse_id") for loc in locations}

    totals: dict[tuple, float] = {}
    for q in quants:
        loc = q.get("location_id")
        if not loc:
            continue
        wh = loc_to_wh.get(loc[0])
        key = (wh[0], wh[1]) if wh else (None, "Other / Unassigned")
        totals[key] = totals.get(key, 0) + q["quantity"]

    result_list = [
        {"warehouse_id": wh_id, "warehouse_name": wh_name, "quantity": qty}
        for (wh_id, wh_name), qty in totals.items()
        if qty
    ]
    result_list.sort(key=lambda r: r["warehouse_name"] or "")
    result = {"stock_by_warehouse": result_list}
    _cache.set(cache_key, result, TTL_PRODUCT_STOCK)
    return result


@app.get("/api/products/{product_id}/packagings")
def product_packagings(product_id: int, authorization: str | None = Header(None)):
    """Product.packaging records for a product.  Cached 30 min.

    Reads from product.packaging (not uom.uom).  Each packaging record has:
      - id   : the product.packaging record id
      - name : e.g. "P12"
      - qty  : how many base-UOM units are in one pack (e.g. 12 for P12)
    """
    read_token(authorization)
    cache_key = f"packagings:{product_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    # product.packaging is linked to product.product via product_id field.
    # We need the product.template id from the product.product record first,
    # because product.packaging links to product_id on the template level
    # in Odoo 16+. We try both approaches for compatibility.
    packagings = _fetch_product_packagings(product_id)
    result = {"packagings": packagings}
    _cache.set(cache_key, result, TTL_PACKAGINGS)
    return result


def _fetch_product_packagings(product_id: int) -> list:
    """Read product.packaging records for the given product.product id.

    Odoo stores packagings on product.template, but product.packaging
    has a `product_id` field that points to product.template in most
    versions.  We resolve the template id from the product.product record
    and then search product.packaging by that template id.

    Falls back gracefully if the model or fields are unavailable.
    """
    # Step 1: Get product.template id from product.product
    try:
        prod_recs = swag_execute(
            "product.product", "read", [[product_id]],
            {"fields": ["product_tmpl_id", "uom_id"]}
        )
    except Exception:
        return []

    if not prod_recs:
        return []

    tmpl = prod_recs[0].get("product_tmpl_id")
    tmpl_id = tmpl[0] if tmpl else None

    # Step 2: Determine available fields on product.packaging dynamically
    try:
        fields_meta = swag_execute("product.packaging", "fields_get", [], {"attributes": ["string", "type"]})
    except Exception:
        return []

    if not fields_meta:
        return []

    # Build the field list to request — always id+name+qty, plus barcode if present
    available = set(fields_meta.keys())
    req_fields = [f for f in ["id", "name", "qty", "product_id", "barcode"] if f in available]
    if "id" not in req_fields:
        req_fields.insert(0, "id")
    if "name" not in req_fields or "qty" not in req_fields:
        # Model doesn't have expected fields — not a standard product.packaging
        return []

    # Step 3: Search product.packaging records.
    # Try by product_tmpl_id first (Odoo 14+), fall back to product_id (template)
    pkg_records = []

    # Try filtering by product_id (which in product.packaging is the template)
    if tmpl_id:
        try:
            pkg_records = swag_execute(
                "product.packaging", "search_read",
                [[[("product_id", "=", tmpl_id)]]],
                {"fields": req_fields}
            )
        except Exception:
            pkg_records = []

    # If nothing found via template, try direct search_read with no filter
    # and match by product code (less precise but safe fallback)
    if not pkg_records:
        try:
            # Some Odoo versions store packaging on product.product directly
            pkg_records = swag_execute(
                "product.packaging", "search_read",
                [[]],
                {"fields": req_fields, "limit": 500}
            )
            # Filter client-side for this product's template
            if tmpl_id:
                pkg_records = [
                    r for r in pkg_records
                    if r.get("product_id") and (
                        (isinstance(r["product_id"], list) and r["product_id"][0] == tmpl_id)
                        or r["product_id"] == tmpl_id
                    )
                ]
        except Exception:
            return []

    if not pkg_records:
        return []

    packagings = []
    for r in pkg_records:
        raw_qty = r.get("qty")
        try:
            qty = float(raw_qty) if raw_qty is not None else 1.0
        except (TypeError, ValueError):
            qty = 1.0
        packagings.append({
            "id": r["id"],
            "name": r.get("name") or f"Pack {r['id']}",
            "qty": qty,
        })

    packagings.sort(key=lambda p: p["qty"])
    return packagings


@app.get("/api/products/{product_id}/my-shop-stock")
def product_my_shop_stock(product_id: int, authorization: str | None = Header(None)):
    """Branch's own on-hand stock for this product (from LAROUCHE).  Cached 2 min."""
    payload = read_token(authorization)
    branch_key = payload["branch_key"]
    cache_key = f"mystock:{product_id}:{branch_key}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached

    recs = swag_execute("product.product", "read", [[product_id]], {"fields": ["default_code", "name"]})
    code = recs[0].get("default_code") if recs else None
    if not code:
        result = {"matched": False, "quantity": None, "reason": "This product has no code to match by."}
        _cache.set(cache_key, result, TTL_MY_SHOP_STOCK)
        return result

    laroche_products = laroche_admin_execute(
        "product.product", "search_read", [[["default_code", "=", code]]], {"fields": ["id", "name"]}
    )
    if len(laroche_products) != 1:
        reason = "not found in LAROUCHE" if not laroche_products else "multiple LAROUCHE products share this code"
        result = {"matched": False, "quantity": None, "reason": reason}
        _cache.set(cache_key, result, TTL_MY_SHOP_STOCK)
        return result
    laroche_product_id = laroche_products[0]["id"]

    warehouse_id = int(branch_key)
    quants = laroche_admin_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", laroche_product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    if not quants:
        result = {"matched": True, "quantity": 0, "laroche_product_name": laroche_products[0]["name"]}
        _cache.set(cache_key, result, TTL_MY_SHOP_STOCK)
        return result

    location_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
    locations = laroche_admin_execute("stock.location", "read", [location_ids], {"fields": ["warehouse_id"]})
    loc_to_wh_id = {loc["id"]: (loc["warehouse_id"][0] if loc.get("warehouse_id") else None) for loc in locations}

    total = sum(
        q["quantity"]
        for q in quants
        if (loc := q.get("location_id")) and loc_to_wh_id.get(loc[0]) == warehouse_id
    )

    result = {"matched": True, "quantity": total, "laroche_product_name": laroche_products[0]["name"]}
    _cache.set(cache_key, result, TTL_MY_SHOP_STOCK)
    return result


@app.get("/api/products/{product_id}/full-details")
def product_full_details(product_id: int, authorization: str | None = Header(None)):
    """NEW: fetch stock + packagings + my-shop-stock all in ONE request, in
    parallel.  Cuts the product-detail page from 3 serial round-trips to 1.
    Frontend can call this single endpoint instead of 3 separate ones."""
    payload = read_token(authorization)
    branch_key = payload["branch_key"]

    results = run_parallel(
        stock=lambda: _fetch_stock(product_id),
        packagings=lambda: _fetch_packagings(product_id),
        my_shop_stock=lambda: _fetch_my_shop_stock(product_id, branch_key),
    )
    return {
        "stock_by_warehouse": results["stock"]["stock_by_warehouse"],
        "packagings":         results["packagings"]["packagings"],
        "my_shop_stock":      results["my_shop_stock"],
    }


# ── internal helpers used by the parallel full-details endpoint ──────────────
def _fetch_stock(product_id: int) -> dict:
    cache_key = f"stock:{product_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    quants = swag_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    if not quants:
        r = {"stock_by_warehouse": []}
        _cache.set(cache_key, r, TTL_PRODUCT_STOCK)
        return r
    location_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
    locations = swag_execute("stock.location", "read", [location_ids], {"fields": ["warehouse_id"]})
    loc_to_wh = {loc["id"]: loc.get("warehouse_id") for loc in locations}
    totals: dict[tuple, float] = {}
    for q in quants:
        loc = q.get("location_id")
        if not loc:
            continue
        wh = loc_to_wh.get(loc[0])
        k = (wh[0], wh[1]) if wh else (None, "Other / Unassigned")
        totals[k] = totals.get(k, 0) + q["quantity"]
    result_list = [
        {"warehouse_id": wid, "warehouse_name": wn, "quantity": qty}
        for (wid, wn), qty in totals.items() if qty
    ]
    result_list.sort(key=lambda x: x["warehouse_name"] or "")
    r = {"stock_by_warehouse": result_list}
    _cache.set(cache_key, r, TTL_PRODUCT_STOCK)
    return r


def _fetch_packagings(product_id: int) -> dict:
    """Internal helper: uses same product.packaging logic as the route handler."""
    cache_key = f"packagings:{product_id}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    packagings = _fetch_product_packagings(product_id)
    r = {"packagings": packagings}
    _cache.set(cache_key, r, TTL_PACKAGINGS)
    return r


def _fetch_my_shop_stock(product_id: int, branch_key: str) -> dict:
    cache_key = f"mystock:{product_id}:{branch_key}"
    cached = _cache.get(cache_key)
    if cached is not None:
        return cached
    recs = swag_execute("product.product", "read", [[product_id]], {"fields": ["default_code", "name"]})
    code = recs[0].get("default_code") if recs else None
    if not code:
        r = {"matched": False, "quantity": None, "reason": "This product has no code to match by."}
        _cache.set(cache_key, r, TTL_MY_SHOP_STOCK)
        return r
    laroche_products = laroche_admin_execute(
        "product.product", "search_read", [[["default_code", "=", code]]], {"fields": ["id", "name"]}
    )
    if len(laroche_products) != 1:
        reason = "not found in LAROUCHE" if not laroche_products else "multiple LAROUCHE products share this code"
        r = {"matched": False, "quantity": None, "reason": reason}
        _cache.set(cache_key, r, TTL_MY_SHOP_STOCK)
        return r
    laroche_product_id = laroche_products[0]["id"]
    warehouse_id = int(branch_key)
    quants = laroche_admin_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", laroche_product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    if not quants:
        r = {"matched": True, "quantity": 0, "laroche_product_name": laroche_products[0]["name"]}
        _cache.set(cache_key, r, TTL_MY_SHOP_STOCK)
        return r
    location_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
    locations = laroche_admin_execute("stock.location", "read", [location_ids], {"fields": ["warehouse_id"]})
    loc_to_wh_id = {loc["id"]: (loc["warehouse_id"][0] if loc.get("warehouse_id") else None) for loc in locations}
    total = sum(
        q["quantity"]
        for q in quants
        if (loc := q.get("location_id")) and loc_to_wh_id.get(loc[0]) == warehouse_id
    )
    r = {"matched": True, "quantity": total, "laroche_product_name": laroche_products[0]["name"]}
    _cache.set(cache_key, r, TTL_MY_SHOP_STOCK)
    return r


@app.get("/api/swag-warehouses")
def list_swag_warehouses(authorization: str | None = Header(None)):
    """SWAG warehouses list.  Cached 30 min — this never changes mid-session."""
    read_token(authorization)
    cached = _cache.get("swag_warehouses")
    if cached is not None:
        return cached
    warehouses = swag_execute(
        "stock.warehouse", "search_read", [[]], {"fields": ["id", "name", "code"], "order": "name asc"}
    )
    result = {"warehouses": warehouses}
    _cache.set("swag_warehouses", result, TTL_SWAG_WAREHOUSES)
    return result


def _detect_line_warehouse_field() -> str | None:
    """Detect which field on sale.order.line holds the warehouse.

    We try field NAMES and relations directly (not just labels) because
    Odoo may be in Arabic and label-based matching fails.
    Known field names across Odoo versions: warehouse_id (custom/community modules).
    We check which ones actually exist via fields_get and return the first
    many2one field whose relation is stock.warehouse.
    Falls back to label search with Arabic keywords as last resort.
    """
    cache_key = "sale.order.line:warehouse_field"
    if cache_key in _field_label_cache:
        return _field_label_cache[cache_key]

    try:
        meta = swag_execute("sale.order.line", "fields_get", [], {"attributes": ["string", "relation", "type"]})
    except Exception:
        _field_label_cache[cache_key] = None
        return None

    if not meta:
        _field_label_cache[cache_key] = None
        return None

    # Priority 1: any many2one field whose relation is stock.warehouse
    for fname, finfo in meta.items():
        if finfo.get("type") == "many2one" and finfo.get("relation") == "stock.warehouse":
            _field_label_cache[cache_key] = fname
            return fname

    # Priority 2: label-based fallback with English AND Arabic keywords
    warehouse_keywords = ["warehouse", "\u0645\u0633\u062a\u0648\u062f\u0639", "\u0645\u062e\u0632\u0646"]
    for fname, finfo in meta.items():
        label = (finfo.get("string") or "").lower()
        if any(kw in label for kw in warehouse_keywords) and finfo.get("type") == "many2one":
            _field_label_cache[cache_key] = fname
            return fname

    _field_label_cache[cache_key] = None
    return None


def _create_swag_order(swag_partner_id, items, warehouse_id, note, employee_name, branch_name, branch_key):
    line_warehouse_field = _detect_line_warehouse_field() if warehouse_id else None

    order_lines = []
    for item in items:
        if item.get("packaging_id"):
            # Ordering by packaging (e.g. "1 × P12").
            # packaging_id  = product.packaging record id (NOT a uom.uom id).
            # packaging_qty = number of packs the user selected (e.g. 1).
            # qty           = total base-UOM quantity = packaging.qty × packaging_qty
            #                 (already calculated by the frontend and sent as `qty`).
            #
            # Correct Odoo sale.order.line fields:
            #   product_packaging_id  → the product.packaging record id
            #   product_packaging_qty → number of packs
            #   product_uom_qty       → total base units (Odoo may auto-compute this
            #                           when product_packaging_id is set, but we send
            #                           it explicitly as the safe fallback)
            #
            # DO NOT set product_uom to a packaging id — product_uom must be a
            # uom.uom record id (the product's own UoM), not a packaging record.
            packaging_qty = item.get("packaging_qty")
            if not packaging_qty or float(packaging_qty) <= 0:
                packaging_qty = 1.0
            packaging_qty = float(packaging_qty)

            # Total qty in base units — sent by frontend as item["qty"]
            total_qty = float(item.get("qty") or 0)
            if total_qty <= 0:
                # Fallback: we don't have total qty; Odoo will compute it from packaging
                total_qty = packaging_qty  # Odoo will fix via onchange

            line_vals = {
                "product_id": item["product_id"],
                "product_packaging_id": item["packaging_id"],
                "product_packaging_qty": packaging_qty,
                "product_uom_qty": total_qty,
            }
        else:
            qty = float(item.get("qty") or 0)
            # Safety net: if the frontend sent a fractional qty that looks like
            # a packaging conversion without sending a packaging_id, Odoo would
            # round it to 0 and reject.  Round up to 1 so at minimum 1 piece
            # is ordered — far better than a hard 400.
            if qty <= 0:
                raise HTTPException(400, f"Product id {item['product_id']}: quantity must be greater than zero.")
            if qty < 1.0:
                qty = 1.0   # round-up: fractional base-unit qty not accepted by Odoo
            line_vals = {"product_id": item["product_id"], "product_uom_qty": qty}
        if line_warehouse_field:
            line_vals[line_warehouse_field] = warehouse_id
        order_lines.append((0, 0, line_vals))

    note_lines = [f"Ordered via Branch Portal by {employee_name} ({branch_name})"]
    if note:
        note_lines.append(note)

    vals = {
        "partner_id": swag_partner_id,
        "order_line": order_lines,
        "note": "\n".join(note_lines),
    }
    if warehouse_id:
        vals["warehouse_id"] = warehouse_id

    order_id = swag_execute("sale.order", "create", [vals], {})

    # Fix salesperson + warehouse in parallel (non-critical best-effort writes)
    def _fix_salesperson():
        try:
            partner_recs = swag_execute("res.partner", "read", [[swag_partner_id]], {"fields": ["user_id"]})
            partner_salesperson = partner_recs[0].get("user_id") if partner_recs else False
            if partner_salesperson:
                swag_execute("sale.order", "write", [[order_id], {"user_id": partner_salesperson[0]}])
        except Exception:
            pass

    def _fix_warehouse():
        if warehouse_id:
            try:
                swag_execute("sale.order", "write", [[order_id], {"warehouse_id": warehouse_id}])
            except Exception:
                pass  # non-critical — order already created
            if line_warehouse_field:
                try:
                    line_ids = swag_execute("sale.order.line", "search", [[["order_id", "=", order_id]]])
                    if line_ids:
                        swag_execute("sale.order.line", "write", [line_ids, {line_warehouse_field: warehouse_id}])
                except Exception:
                    pass

    # Run both fixes concurrently
    run_parallel(fix_sp=_fix_salesperson, fix_wh=_fix_warehouse)

    detail_fields = [
        "name", "partner_id", "partner_invoice_id", "partner_shipping_id",
        "pricelist_id", "payment_term_id", "user_id", "date_order",
        "amount_untaxed", "amount_tax", "amount_total", "state", "warehouse_id",
    ]
    collector_field = find_field_by_label("sale.order", ["collector"])
    if collector_field and collector_field not in detail_fields:
        detail_fields.append(collector_field)

    rec = swag_execute("sale.order", "read", [[order_id]], {"fields": detail_fields})
    order = rec[0] if rec else {}

    details = {
        "order_name":      order.get("name"),
        "customer":        _m2o_name(order.get("partner_id")),
        "invoice_address": _m2o_name(order.get("partner_invoice_id")),
        "delivery_address":_m2o_name(order.get("partner_shipping_id")),
        "pricelist":       _m2o_name(order.get("pricelist_id")),
        "payment_terms":   _m2o_name(order.get("payment_term_id")),
        "salesperson":     _m2o_name(order.get("user_id")),
        "warehouse":       _m2o_name(order.get("warehouse_id")),
        "collector":       _m2o_name(order.get(collector_field)) if collector_field else None,
        "order_date":      order.get("date_order"),
        "amount_untaxed":  order.get("amount_untaxed"),
        "amount_tax":      order.get("amount_tax"),
        "amount_total":    order.get("amount_total"),
        "state":           order.get("state"),
        "ordered_by":      employee_name,
    }

    database.log_order(
        swag_order_id=order_id,
        swag_order_name=details["order_name"] or str(order_id),
        branch_key=branch_key,
        branch_name=branch_name,
        employee_name=employee_name,
        swag_partner_id=swag_partner_id,
        swag_partner_name=details["customer"],
        warehouse_id=warehouse_id,
        warehouse_name=details.get("warehouse"),
        item_count=len(items),
        amount_total=order.get("amount_total") or 0,
        items_json=json.dumps(items),
    )

    # Bust the product stock cache for all products in this order
    for item in items:
        _cache.delete(f"stock:{item['product_id']}")

    return order_id, details


@app.post("/api/orders")
def create_order(body: OrderRequest, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    if not body.items:
        raise HTTPException(400, "Cart is empty.")

    employee_name = payload.get("employee_name") or "unknown"
    items = [
        {"product_id": it.product_id, "qty": it.qty, "packaging_id": it.packaging_id, "packaging_qty": it.packaging_qty}
        for it in body.items
    ]
    order_id, details = _create_swag_order(
        payload["swag_partner_id"], items, body.warehouse_id, body.note,
        employee_name, payload.get("branch_name"), payload["branch_key"],
    )
    return {"order_id": order_id, "order_name": details["order_name"] or str(order_id), "details": details}


class PendingOrderRequest(BaseModel):
    items: list[CartItem]
    warehouse_id: int | None = None
    note: str | None = None


class DecidePendingRequest(BaseModel):
    reason: str | None = None


@app.post("/api/orders/pending")
def submit_pending_order(body: PendingOrderRequest, authorization: str | None = Header(None)):
    """Save an order for manager approval — does NOT touch SWAG yet."""
    payload = read_token(authorization)
    if not body.items:
        raise HTTPException(400, "Cart is empty.")

    # Fetch all product details in ONE batch call instead of N serial calls
    product_ids = [it.product_id for it in body.items]
    product_recs = swag_execute(
        "product.product", "read", [product_ids],
        {"fields": ["id", "default_code", "name", "list_price"]},
    )
    prod_by_id = {r["id"]: r for r in (product_recs or [])}

    items_with_names = []
    for it in body.items:
        rec = prod_by_id.get(it.product_id, {})
        items_with_names.append({
            "product_id": it.product_id,
            "default_code": rec.get("default_code"),
            "name": rec.get("name"),
            "qty": it.qty,
            "price": rec.get("list_price") or 0,
            "packaging_id": it.packaging_id,
            "packaging_qty": it.packaging_qty,
        })
    amount_total = sum(i["qty"] * i["price"] for i in items_with_names)

    db = database.get_session()
    try:
        row = database.PendingOrder(
            branch_key=payload["branch_key"],
            branch_name=payload.get("branch_name"),
            swag_partner_id=payload["swag_partner_id"],
            requested_by=payload.get("employee_name") or "unknown",
            warehouse_id=body.warehouse_id,
            note=body.note,
            items_json=json.dumps(items_with_names),
            amount_total=amount_total,
            status="pending",
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return {"id": row.id, "status": "pending"}
    finally:
        db.close()


@app.get("/api/orders/pending")
def list_pending_orders(authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        rows = (
            db.query(database.PendingOrder)
            .filter(database.PendingOrder.branch_key == payload["branch_key"])
            .order_by(database.PendingOrder.created_at.desc())
            .all()
        )
        return {
            "pending_orders": [
                {
                    "id": r.id,
                    "requested_by": r.requested_by,
                    "items": json.loads(r.items_json),
                    "amount_total": r.amount_total,
                    "warehouse_id": r.warehouse_id,
                    "note": r.note,
                    "status": r.status,
                    "decided_by": r.decided_by,
                    "swag_order_name": r.swag_order_name,
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@app.post("/api/orders/pending/{pending_id}/approve")
def approve_pending_order(pending_id: int, body: DecidePendingRequest, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        row = db.query(database.PendingOrder).filter(database.PendingOrder.id == pending_id).first()
        if not row:
            raise HTTPException(404, "Pending order not found.")
        if row.branch_key != payload["branch_key"]:
            raise HTTPException(403, "This pending order doesn't belong to your outlet.")
        if row.status != "pending":
            raise HTTPException(400, f"This order was already {row.status}.")

        items = json.loads(row.items_json)
        order_items = [
            {
                "product_id": i["product_id"],
                "qty": i["qty"],
                "packaging_id": i.get("packaging_id"),
                "packaging_qty": i.get("packaging_qty"),
            }
            for i in items
        ]
        order_id, details = _create_swag_order(
            row.swag_partner_id, order_items, row.warehouse_id, row.note,
            row.requested_by, row.branch_name, row.branch_key,
        )

        row.status = "approved"
        row.decided_by = payload.get("employee_name") or "unknown"
        row.decided_at = datetime.now(timezone.utc)
        row.swag_order_id = order_id
        row.swag_order_name = details["order_name"]
        db.commit()

        return {"order_id": order_id, "order_name": details["order_name"], "details": details}
    finally:
        db.close()


@app.post("/api/orders/pending/{pending_id}/reject")
def reject_pending_order(pending_id: int, body: DecidePendingRequest, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        row = db.query(database.PendingOrder).filter(database.PendingOrder.id == pending_id).first()
        if not row:
            raise HTTPException(404, "Pending order not found.")
        if row.branch_key != payload["branch_key"]:
            raise HTTPException(403, "This pending order doesn't belong to your outlet.")
        if row.status != "pending":
            raise HTTPException(400, f"This order was already {row.status}.")
        row.status = "rejected"
        row.decided_by = payload.get("employee_name") or "unknown"
        row.decided_at = datetime.now(timezone.utc)
        row.note = (row.note or "") + (f"\nRejected: {body.reason}" if body.reason else "")
        db.commit()
        return {"ok": True}
    finally:
        db.close()


class BulkLookupRequest(BaseModel):
    codes: list[str]


@app.post("/api/products/bulk-lookup")
def bulk_lookup_products(body: BulkLookupRequest, authorization: str | None = Header(None)):
    """Excel bulk-order upload: look up N product codes in ONE Odoo call
    (was: one call per code in a loop — O(N) round-trips → O(1) round-trips)."""
    read_token(authorization)
    codes = [c.strip() for c in body.codes if c and c.strip()]
    if not codes:
        return {"matched": [], "not_found": []}

    # Single batch query for all codes
    domain = ["|"] * (len(codes) - 1) + [["default_code", "=", c] for c in codes]
    recs = swag_execute(
        "product.product", "search_read",
        [domain],
        {"fields": ["id", "default_code", "name", "list_price", "qty_available"]},
    )

    # Group by code — a code is "matched" only if exactly one product has it
    by_code: dict[str, list] = {}
    for r in (recs or []):
        c = (r.get("default_code") or "").strip()
        by_code.setdefault(c, []).append(r)

    matched = []
    not_found = []
    for code in codes:
        hits = by_code.get(code, [])
        if len(hits) == 1:
            matched.append(hits[0])
        else:
            not_found.append(code)  # 0 = missing, 2+ = ambiguous

    return {"matched": matched, "not_found": not_found}


@app.get("/api/products/by-barcode")
def product_by_barcode(code: str, authorization: str | None = Header(None)):
    read_token(authorization)
    code = (code or "").strip()
    if not code:
        raise HTTPException(400, "No barcode provided.")
    recs = swag_execute(
        "product.product", "search_read",
        [[["barcode", "=", code]]],
        {"fields": ["id", "default_code", "name", "list_price", "qty_available"]},
    )
    if not recs:
        raise HTTPException(404, f"No product found with barcode {code}.")
    if len(recs) > 1:
        raise HTTPException(409, f"Multiple products share barcode {code} — search by name/code instead.")
    return {"product": recs[0]}


@app.get("/api/my-orders")
def my_orders(limit: int = 50, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    orders = swag_execute(
        "sale.order", "search_read",
        [[["partner_id", "=", payload["swag_partner_id"]]]],
        {
            "fields": ["id", "name", "date_order", "amount_total", "state", "warehouse_id"],
            "order": "date_order desc",
            "limit": limit,
        },
    )
    for o in orders:
        o["warehouse"] = _m2o_name(o.pop("warehouse_id", None))
    return {"orders": orders}


@app.get("/api/orders/{order_id}")
def order_detail(order_id: int, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    recs = swag_execute(
        "sale.order", "read", [[order_id]],
        {"fields": ["id", "name", "partner_id", "date_order", "amount_untaxed",
                    "amount_tax", "amount_total", "state", "warehouse_id", "note"]},
    )
    if not recs:
        raise HTTPException(404, "Order not found.")
    order = recs[0]
    if not order.get("partner_id") or order["partner_id"][0] != payload["swag_partner_id"]:
        raise HTTPException(403, "This order doesn't belong to your outlet.")

    lines = swag_execute(
        "sale.order.line", "search_read",
        [[["order_id", "=", order_id], ["display_type", "=", False]]],
        {"fields": ["product_id", "name", "product_uom_qty", "price_unit", "price_subtotal"]},
    )
    line_items = [
        {
            "product_id": ln["product_id"][0] if ln.get("product_id") else None,
            "product_name": ln["product_id"][1] if ln.get("product_id") else ln.get("name"),
            "qty": ln["product_uom_qty"],
            "price_unit": ln["price_unit"],
            "subtotal": ln["price_subtotal"],
        }
        for ln in lines
    ]
    return {
        "id": order["id"],
        "name": order["name"],
        "date_order": order.get("date_order"),
        "warehouse": _m2o_name(order.get("warehouse_id")),
        "amount_untaxed": order.get("amount_untaxed"),
        "amount_tax": order.get("amount_tax"),
        "amount_total": order.get("amount_total"),
        "state": order.get("state"),
        "note": order.get("note"),
        "lines": line_items,
    }


@app.get("/api/login-history")
def login_history(limit: int = 50, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        rows = (
            db.query(database.LoginEvent)
            .filter(database.LoginEvent.branch_key == payload["branch_key"])
            .order_by(database.LoginEvent.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "logins": [
                {
                    "id": r.id,
                    "employee_name": r.employee_name,
                    "branch_name": r.branch_name,
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@app.get("/api/order-graph")
def order_graph(days: int = 30, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        rows = (
            db.query(database.OrderRecord)
            .filter(database.OrderRecord.branch_key == payload["branch_key"])
            .order_by(database.OrderRecord.created_at.asc())
            .all()
        )
        by_day: dict[str, dict] = {}
        for r in rows:
            day = r.created_at.strftime("%Y-%m-%d")
            bucket = by_day.setdefault(day, {"date": day, "orders": 0, "total": 0.0})
            bucket["orders"] += 1
            bucket["total"] += r.amount_total or 0
        series = sorted(by_day.values(), key=lambda b: b["date"])
        return {"series": series[-days:] if days else series}
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/admin/login")
def admin_login(body: AdminLoginRequest):
    if not ADMIN_PASSWORD:
        raise HTTPException(500, "Server is missing ADMIN_PASSWORD configuration.")
    if body.password != ADMIN_PASSWORD:
        raise HTTPException(401, "Incorrect admin password.")
    return {"token": make_admin_token()}


@app.get("/api/admin/summary")
def admin_summary(authorization: str | None = Header(None)):
    read_admin_token(authorization)
    db = database.get_session()
    try:
        orders = db.query(database.OrderRecord).all()
        branches = {o.branch_key for o in orders}
        total_value = sum(o.amount_total or 0 for o in orders)
        return {
            "total_orders": len(orders),
            "total_value": total_value,
            "branch_count": len(branches),
        }
    finally:
        db.close()


@app.get("/api/admin/branches")
def admin_branches(authorization: str | None = Header(None)):
    read_admin_token(authorization)
    db = database.get_session()
    try:
        orders = db.query(database.OrderRecord).all()
        by_branch: dict[str, dict] = {}
        for o in orders:
            b = by_branch.setdefault(o.branch_key, {
                "branch_key": o.branch_key,
                "branch_name": o.branch_name,
                "order_count": 0,
                "total_value": 0.0,
                "last_order_at": None,
            })
            b["order_count"] += 1
            b["total_value"] += o.amount_total or 0
            if not b["last_order_at"] or o.created_at > b["last_order_at"]:
                b["last_order_at"] = o.created_at
        result = sorted(by_branch.values(), key=lambda b: b["total_value"], reverse=True)
        for b in result:
            b["last_order_at"] = b["last_order_at"].isoformat() if b["last_order_at"] else None
        return {"branches": result}
    finally:
        db.close()


@app.get("/api/admin/graph")
def admin_graph(days: int = 30, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    db = database.get_session()
    try:
        rows = db.query(database.OrderRecord).order_by(database.OrderRecord.created_at.asc()).all()
        by_day: dict[str, dict] = {}
        for r in rows:
            day = r.created_at.strftime("%Y-%m-%d")
            bucket = by_day.setdefault(day, {"date": day, "orders": 0, "total": 0.0})
            bucket["orders"] += 1
            bucket["total"] += r.amount_total or 0
        series = sorted(by_day.values(), key=lambda b: b["date"])
        return {"series": series[-days:] if days else series}
    finally:
        db.close()


@app.get("/api/admin/orders")
def admin_orders(limit: int = 100, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    db = database.get_session()
    try:
        rows = (
            db.query(database.OrderRecord)
            .order_by(database.OrderRecord.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "orders": [
                {
                    "id": r.id,
                    "swag_order_name": r.swag_order_name,
                    "branch_name": r.branch_name,
                    "employee_name": r.employee_name,
                    "warehouse_name": r.warehouse_name,
                    "item_count": r.item_count,
                    "amount_total": r.amount_total,
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]
        }
    finally:
        db.close()


class ClearMappingRequest(BaseModel):
    branch_key: str


@app.get("/api/admin/mappings")
def admin_mappings(authorization: str | None = Header(None)):
    """All branch→SWAG mappings.  Fetches all partner names in ONE batch call
    instead of one call per mapping (was O(N) serial round-trips → O(1))."""
    read_admin_token(authorization)
    mapping = load_branch_partner_map()

    db = database.get_session()
    try:
        name_by_key: dict[str, str] = {}
        for row in db.query(database.LoginEvent).order_by(database.LoginEvent.created_at.desc()).all():
            name_by_key.setdefault(row.branch_key, row.branch_name)
    finally:
        db.close()

    if not mapping:
        return {"mappings": []}

    # Batch: fetch ALL partner names in one Odoo call
    all_partner_ids = list(mapping.values())
    try:
        partner_recs = swag_execute("res.partner", "read", [all_partner_ids], {"fields": ["id", "name"]})
        name_by_partner_id = {r["id"]: r["name"] for r in (partner_recs or [])}
    except Exception:
        name_by_partner_id = {}

    result = []
    for branch_key, swag_partner_id in mapping.items():
        result.append({
            "branch_key": branch_key,
            "branch_name": name_by_key.get(branch_key, "(unknown — no login recorded yet)"),
            "swag_partner_id": swag_partner_id,
            "swag_partner_name": name_by_partner_id.get(swag_partner_id,
                                    "(SWAG partner not found — may have been deleted)"),
        })
    result.sort(key=lambda r: r["branch_name"])
    return {"mappings": result}


@app.post("/api/admin/mappings/clear")
def admin_clear_mapping(body: ClearMappingRequest, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    mapping = load_branch_partner_map()
    if body.branch_key in mapping:
        del mapping[body.branch_key]
        save_branch_partner_map(mapping)
    _resolved_cache.pop(body.branch_key, None)
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# CACHE MANAGEMENT (admin utility)
# ─────────────────────────────────────────────────────────────────────────────
@app.post("/api/admin/cache/clear")
def admin_clear_cache(authorization: str | None = Header(None)):
    """Flush the entire in-process cache — useful after a product import or
    price update in Odoo without waiting for TTLs to expire."""
    read_admin_token(authorization)
    _cache._store.clear()
    return {"ok": True, "message": "Cache cleared."}
