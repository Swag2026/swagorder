"""
SWAG Branch Order Portal — Backend
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
stock.warehouse id). The mapping is resolved from the outlet's own name;
ambiguous matches require a one-time human pick from the verified candidate
list. Parent-company VAT/email/phone values are deliberately not used because
they can be shared by multiple branches or sibling brands.

Deploy: Railway (this folder as the service root).

Env vars required (Railway → Variables):
    LAROUCHE_URL            e.g. https://outfit.laroche.sa  (NO trailing /odoo)
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
import xmlrpc.client
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import database

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
LAROUCHE_URL = os.environ.get("LAROUCHE_URL", "").rstrip("/")
LAROUCHE_DB = os.environ.get("LAROUCHE_DB", "")
LAROUCHE_ADMIN_USER = os.environ.get("LAROUCHE_ADMIN_USER", "")
LAROUCHE_ADMIN_API_KEY = os.environ.get("LAROUCHE_ADMIN_API_KEY", "")

SWAG_URL = os.environ.get("SWAG_URL", "").rstrip("/")
SWAG_DB = os.environ.get("SWAG_DB", "")
SWAG_USER = os.environ.get("SWAG_USER", "")
SWAG_API_KEY = os.environ.get("SWAG_API_KEY", "")

JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-railway-variables")
ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

MAP_FILE = Path(__file__).parent / "branch_partner_map.json"
MAPPING_VERSION = 2


def load_branch_partner_map() -> dict:
    """Return only mappings created by the current, branch-safe matcher.

    Older releases stored a plain ``{warehouse_id: partner_id}`` object. Those
    values may have been selected using the parent company's shared VAT/phone,
    so they must not be trusted after upgrading.
    """
    if not MAP_FILE.exists():
        return {}
    try:
        raw = json.loads(MAP_FILE.read_text())
        if isinstance(raw, dict) and raw.get("version") == MAPPING_VERSION:
            mappings = raw.get("mappings")
            if not isinstance(mappings, dict):
                return {}
        elif isinstance(raw, dict):
            # Upgrade compatibility: the previous release stored the map as
            # {warehouse_id: partner_id}. Keep those explicit human/admin
            # selections usable, while every new write uses the versioned,
            # branch-safe format above.
            mappings = raw
        else:
            return {}
        return {str(k): int(v) for k, v in mappings.items()}
    except Exception:
        return {}


def save_branch_partner_map(mapping: dict) -> None:
    """Persist a resolved mapping so future selections skip the auto-match
    search. Best-effort: on ephemeral-filesystem hosts this may not survive a
    redeploy, but it still avoids repeat lookups during the running instance
    and gives the admin a file to inspect/edit directly."""
    try:
        MAP_FILE.write_text(json.dumps({
            "version": MAPPING_VERSION,
            "mappings": {str(k): int(v) for k, v in mapping.items()},
        }, indent=2, ensure_ascii=False))
    except Exception:
        pass


_resolved_cache: dict[str, int] = {}


def assert_current_branch_partner(branch_key: str, partner_id: int) -> None:
    """Refuse to use a session/order partner that is no longer mapped.

    The signed session token is not enough on its own: an admin or branch
    employee can clear a bad mapping while an old token is still valid.
    Checking the current mapping at every authenticated request prevents that
    old token (and any pending order created from it) from reaching SWAG.
    """
    current_partner_id = (
        load_branch_partner_map().get(str(branch_key))
        or _resolved_cache.get(str(branch_key))
    )
    if current_partner_id is None:
        raise HTTPException(
            401,
            "This branch/customer mapping needs to be verified again — please log in again.",
        )
    if int(current_partner_id) != int(partner_id):
        raise HTTPException(
            401,
            "This branch/customer mapping changed — please log in again.",
        )

app = FastAPI(title="SWAG Branch Order Portal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# ODOO HELPERS
# ─────────────────────────────────────────────────────────────────────────────
class _TimeoutTransport(xmlrpc.client.SafeTransport):
    """Same as the default HTTPS transport, but with a socket timeout — so a
    slow/unresponsive Odoo server gives us a clear error in ~30s instead of
    hanging until Railway's own gateway times out (502, no useful info)."""

    def __init__(self, timeout=30, use_datetime=False):
        super().__init__(use_datetime=use_datetime)
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


def _proxy(url: str, endpoint: str):
    return xmlrpc.client.ServerProxy(
        f"{url}/xmlrpc/2/{endpoint}", allow_none=True, transport=_TimeoutTransport(timeout=30)
    )


def laroche_authenticate(email: str, password: str) -> int:
    """Authenticate with the EMPLOYEE'S OWN LAROUCHE credentials — this is
    what gives us accountability (every order traces back to who logged in)."""
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
    """Run a call under a specific (already-authenticated) LAROUCHE user."""
    try:
        return _proxy(LAROUCHE_URL, "object").execute_kw(
            LAROUCHE_DB, uid, password, model, method, args, kwargs or {}
        )
    except xmlrpc.client.Fault as e:
        raise HTTPException(400, f"LAROUCHE Odoo error: {e.faultString}")
    except Exception as e:
        raise HTTPException(502, f"Could not reach LAROUCHE Odoo: {e}")


def laroche_optional_read(uid: int, password: str, model: str, record_id: int, fields: list[str]) -> list[dict]:
    """Read optional profile fields without turning a login into a 400.

    Some LAROUCHE databases expose ``res.users`` fields in metadata but do
    not allow every employee to read them. Login only needs the employee name
    and company; an unavailable optional field must not block authentication.
    """
    try:
        return laroche_execute_as(
            uid,
            password,
            model,
            "read",
            [[record_id]],
            {"fields": fields},
        )
    except HTTPException:
        return []


_laroche_admin_uid_cache: int | None = None


def laroche_admin_uid() -> int:
    """Authenticate the LAROUCHE admin-style account once and cache the uid.
    Used ONLY for reliably listing warehouses — never for identity."""
    global _laroche_admin_uid_cache
    if _laroche_admin_uid_cache:
        return _laroche_admin_uid_cache
    if not (LAROUCHE_URL and LAROUCHE_DB and LAROUCHE_ADMIN_USER and LAROUCHE_ADMIN_API_KEY):
        raise HTTPException(500, "Server is missing LAROUCHE_ADMIN_USER / LAROUCHE_ADMIN_API_KEY.")
    try:
        uid = _proxy(LAROUCHE_URL, "common").authenticate(LAROUCHE_DB, LAROUCHE_ADMIN_USER, LAROUCHE_ADMIN_API_KEY, {})
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
    "مستودع", "سوق", "للأزياء", "التجاري", "مصنع", "محل", "مكتب", "co", "company", "trading",
    "branch", "store", "shop", "warehouse", "ltd", "llc", "est", "establishment",
}


def _brand_keywords(name: str | None) -> set[str]:
    words = _norm(name).split()
    # len >= 3 (not 2) — short 2-letter fragments are too easy to coincide
    # with an unrelated word purely by chance (e.g. a mis-spelled variant of
    # the brand name matching a completely unrelated record).
    return {w for w in words if w not in _GENERIC_NAME_WORDS and len(w) >= 3}


def _clean_warehouse_name(name: str) -> str:
    """Warehouse names look like 'W101/ مستودع جدة' or '205/ اوت فيت الامير
    سلطان' — the part before '/' is just an internal code; the part after it
    is the actual outlet name we want to match against SWAG."""
    if "/" in (name or ""):
        return name.split("/", 1)[1].strip()
    return (name or "").strip()


def auto_resolve_swag_partner(branch_info: dict):
    """
    Try to find the ONE SWAG res.partner that corresponds to this outlet.
    Checks branch-owned identifiers (if supplied). Exact outlet-name matches
    are treated as candidates, not proof: the same location name can exist
    under another brand/customer in SWAG and must be confirmed explicitly.
    Parent-company identifiers are intentionally not supplied by the caller.

    If still ambiguous, candidates are returned for an explicit human
    confirmation. Location-name similarity is used only to order the choices;
    it is never allowed to silently choose a different customer's record.

    Returns (partner_id, partner_name, matched_on, candidates).
    """
    # Build ONE combined OR query across every identifier we have, instead of
    # up to 5 separate round-trips to Odoo — this is what made branch
    # selection feel slow. We still evaluate priority (vat > email > mobile
    # > phone > name) afterwards, just against data already fetched.
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
    # Odoo polish-notation OR domain: ["|", "|", term1, term2, term3, ...]
    combined_domain = (["|"] * (len(or_terms) - 1)) + or_terms

    all_matches = swag_execute(
        "res.partner", "search_read", [combined_domain],
        {"fields": ["id", "name", "city", "street", "email", "phone", "mobile", "vat"]},
    )

    best_candidates: list[dict] = []
    for label, value in identifiers:
        matches = [m for m in all_matches if m.get(label) == value]
        if len(matches) == 1 and label != "name":
            return matches[0]["id"], matches[0]["name"], label, []
        if len(matches) > 1:
            candidates = [{"id": m["id"], "name": m["name"], "city": m.get("city")} for m in matches]
            if not best_candidates or len(candidates) < len(best_candidates):
                best_candidates = candidates

    if best_candidates:
        # Only use location words to make the review list easier to scan.
        # Never auto-select here: a shared city/brand word is not proof that
        # two records represent the same outlet.
        brand_words = _brand_keywords(branch_info.get("company_name"))
        keywords = _brand_keywords(branch_info.get("name")) - brand_words
        if keywords:
            best_candidates.sort(
                key=lambda c: len(keywords & set(_norm(c["name"]).split())),
                reverse=True,
            )

    return None, None, None, best_candidates


def find_branch_partner_candidates(branch_info: dict) -> list[dict]:
    """Find review candidates from the outlet name only.

    Tries keyword search first. If keywords are too generic or empty after
    filtering, falls back to searching by the full outlet name (ilike) so
    the branch is never left with zero candidates to pick from.
    """
    brand_words = _brand_keywords(branch_info.get("company_name"))
    keywords = _brand_keywords(branch_info.get("name")) - brand_words

    if keywords:
        terms = [["name", "ilike", keyword] for keyword in sorted(keywords)]
        domain = [terms[0]] if len(terms) == 1 else (["|"] * (len(terms) - 1)) + terms
        results = swag_execute(
            "res.partner", "search_read", [domain],
            {"fields": ["id", "name", "city", "street"], "limit": 40, "order": "name asc"},
        )
        if results:
            return results

    # Fallback: search by the full outlet name (handles cases where all
    # keywords were generic or got subtracted by the company name filter)
    outlet_name = (branch_info.get("name") or "").strip()
    if not outlet_name:
        return []
    return swag_execute(
        "res.partner", "search_read", [[["name", "ilike", outlet_name]]],
        {"fields": ["id", "name", "city", "street"], "limit": 40, "order": "name asc"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# AUTH TOKENS
# ─────────────────────────────────────────────────────────────────────────────
def make_login_token(email: str, employee_name: str, company_id: int, company_name: str) -> str:
    """Short-lived token proving the employee already authenticated with
    their own LAROUCHE credentials — used while they pick their outlet."""
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


def make_token(branch_key: str, branch_name: str, swag_partner_id: int, swag_partner_name: str, employee_name: str,
                default_warehouse_id: int | None = None, default_warehouse_name: str | None = None) -> str:
    payload = {
        "type": "session",
        "mapping_version": MAPPING_VERSION,
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
        "mapping_version": MAPPING_VERSION,
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
    if payload.get("mapping_version") != MAPPING_VERSION:
        raise HTTPException(401, "Your session is outdated — please log in again to refresh the branch/customer mapping.")
    disabled, reason = database.is_branch_disabled(payload["branch_key"])
    if disabled:
        raise HTTPException(403, f"Access to this portal has been disabled by admin" + (f": {reason}" if reason else "."))
    assert_current_branch_partner(payload["branch_key"], payload["swag_partner_id"])
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
    packaging_qty: float | None = None  # number of boxes/packages, if ordering by packaging


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
    """Employee logs in with THEIR OWN LAROUCHE credentials — this is what
    gives us accountability. If their own account is already scoped to a
    specific warehouse (its 'Default Warehouse' field), we resolve straight
    to that outlet — no company-wide picker, no access beyond their own
    warehouse. Only accounts without a default warehouse fall back to
    picking from their company's outlet list."""
    email = body.email.strip()
    uid = laroche_authenticate(email, body.password)

    # Read defensively — custom warehouse fields may be absent or restricted
    # for employee accounts. The basic identity read must remain sufficient
    # for login; the optional warehouse lookup is attempted separately.
    identity_fields = ["name", "company_id"]
    try:
        meta = laroche_execute_as(uid, body.password, "res.users", "fields_get", [], {"attributes": []})
        optional_warehouse_field = next(
            (
                field
                for field in ("property_warehouse_id", "default_warehouse_id", "warehouse_id")
                if field in meta
            ),
            None,
        )
    except Exception:
        optional_warehouse_field = None

    user_recs = laroche_execute_as(uid, body.password, "res.users", "read", [[uid]], {"fields": identity_fields})
    if not user_recs:
        raise HTTPException(500, "Could not load your LAROUCHE user profile.")
    employee_name = user_recs[0]["name"]
    company = user_recs[0].get("company_id") or False
    if not company:
        raise HTTPException(500, "Your LAROUCHE account has no company/brand assigned — ask an admin to check it.")
    company_id, company_name = company[0], company[1]

    own_warehouse = False
    if optional_warehouse_field:
        optional_recs = laroche_optional_read(
            uid,
            body.password,
            "res.users",
            uid,
            [optional_warehouse_field],
        )
        own_warehouse = optional_recs[0].get(optional_warehouse_field) if optional_recs else False
    if own_warehouse:
        # This login is already scoped to exactly one outlet — resolve
        # straight through, skipping the picker (and any access to other
        # outlets under the same brand) entirely.
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
    """Every outlet under the LOGGED-IN employee's own company — never lets
    them browse another brand's outlets."""
    payload = read_login_token(login_token)
    if payload["company_id"] != company_id:
        raise HTTPException(403, "You can only view outlets for your own company.")
    warehouses = laroche_admin_execute(
        "stock.warehouse", "search_read", [[["company_id", "=", company_id]]],
        {"fields": ["id", "name", "code"], "order": "name asc"},
    )
    return {"warehouses": warehouses}


def auto_resolve_swag_warehouse(branch_info: dict, swag_partner_id: int | None = None):
    """
    Find the SWAG stock.warehouse that represents this outlet's own stock.
    Two strategies, tried in order of reliability:

      1. Direct link — if a SWAG warehouse's own `partner_id` is exactly the
         customer we already matched this branch to, that's a guaranteed
         correct link (no guessing at all).
      2. Name matching — same scoring approach as customer matching, but
         against warehouse names (which don't have VAT/email/phone).

    This is a convenience default (which warehouse's stock to show/assume
    first), NOT a correctness-critical mapping like the customer match, so
    if nothing is a clear unique winner we simply leave it unset rather than
    blocking anything — the branch can still pick any warehouse manually.
    """
    if swag_partner_id:
        try:
            direct = swag_execute(
                "stock.warehouse", "search_read", [[["partner_id", "=", swag_partner_id]]],
                {"fields": ["id", "name"]},
            )
            if len(direct) == 1:
                return direct[0]["id"], direct[0]["name"]
        except Exception:
            pass  # field may not exist on this Odoo setup — fall through to name matching

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
    """Shared logic: look up cached mapping, else auto-resolve, else return a
    selection_required payload."""
    key = str(branch_id)
    disabled, reason = database.is_branch_disabled(key)
    if disabled:
        raise HTTPException(403, "This outlet's access has been disabled by admin" + (f": {reason}" if reason else "."))
    mapping = load_branch_partner_map()
    swag_partner_id = mapping.get(key) or _resolved_cache.get(key)
    matched_on = "saved mapping" if swag_partner_id else None

    if not swag_partner_id:
        resolved_id, resolved_name, matched_on, candidates = auto_resolve_swag_partner(branch_info)
        if not resolved_id:
            if not candidates:
                candidates = find_branch_partner_candidates(branch_info)
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

    partner_recs = swag_execute("res.partner", "read", [[swag_partner_id]], {"fields": ["name"]})
    if not partner_recs:
        raise HTTPException(500, f"Mapped SWAG partner id {swag_partner_id} was not found in SWAG Odoo.")
    swag_partner_name = partner_recs[0]["name"]

    default_warehouse_id, default_warehouse_name = auto_resolve_swag_warehouse(branch_info, swag_partner_id)

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
    """Given a specific stock.warehouse id (already known to belong to
    company_id), verify it, build the branch_info used for SWAG matching,
    and resolve/confirm the session. Shared by /api/select-branch and the
    auto-resolve-on-login path (when an employee's own account is already
    scoped to one warehouse)."""
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

    # Do not use the parent company's VAT, phone, or email here. Those values
    # are commonly shared by every outlet under a brand and sometimes by
    # sibling brands, which can silently map this branch to another SWAG
    # customer. The outlet name is the only cross-system identity evidence
    # available from this LAROUCHE record.
    branch_info = {"name": outlet_name, "company_name": company_name}
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
    if payload.get("mapping_version") != MAPPING_VERSION:
        raise HTTPException(401, "This branch selection expired — please start again.")

    q = body.q.strip()
    candidate_ids = [int(candidate_id) for candidate_id in payload.get("candidate_ids", [])]
    if not candidate_ids:
        return {"results": []}
    domain = [["id", "in", candidate_ids]]
    if q:
        domain = ["&", domain[0], ["name", "ilike", q]]
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
    if payload.get("mapping_version") != MAPPING_VERSION:
        raise HTTPException(401, "This branch selection expired — please start again.")

    candidate_ids = {int(candidate_id) for candidate_id in payload.get("candidate_ids", [])}
    if body.partner_id not in candidate_ids:
        raise HTTPException(
            403,
            "That SWAG customer was not one of the verified candidates for this outlet. "
            "Search and select a customer from this branch's list.",
        )

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

    # Best-effort default-warehouse guess: branch_name is "{company} — {outlet}".
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
    """Self-service correction: if a branch employee notices this session is
    linked to the wrong SWAG customer, this clears the saved mapping for
    their outlet so the NEXT login re-runs matching (and asks a human to
    confirm if it's ambiguous) instead of reusing the wrong cached result."""
    payload = read_token(authorization)
    key = payload["branch_key"]
    mapping = load_branch_partner_map()
    if key in mapping:
        del mapping[key]
        save_branch_partner_map(mapping)
    _resolved_cache.pop(key, None)
    return {"ok": True, "message": "Mapping cleared — please log out and log back in to re-verify."}


@app.get("/api/products")
def search_products(q: str = "", limit: int = 10, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    domain = [["sale_ok", "=", True]]
    q = (q or "").strip()
    if q:
        domain = ["&", domain[0], "|", ["default_code", "ilike", q], ["name", "ilike", q]]
    fields = ["id", "default_code", "name", "list_price", "qty_available", "uom_id"]
    products = swag_execute(
        "product.product", "search_read",
        [domain], {"fields": fields, "limit": limit, "order": "default_code asc"},
    )
    hidden_ids = database.get_hidden_product_ids_for_branch(payload["branch_key"])
    if hidden_ids:
        products = [p for p in products if p["id"] not in hidden_ids]
    overrides = database.get_all_min_overrides()
    for p in products:
        p["min_qty"] = overrides.get(p["id"], 4)
    return {"products": products}


@app.get("/api/products/{product_id}/image")
def product_image(product_id: int, authorization: str | None = Header(None)):
    read_token(authorization)
    recs = swag_execute("product.product", "read", [[product_id]], {"fields": ["image_128"]})
    image_b64 = recs[0].get("image_128") if recs else None
    return {"image_base64": image_b64}


class BatchDetailsRequest(BaseModel):
    product_ids: list[int]


@app.post("/api/products/batch-details")
def product_batch_details(body: BatchDetailsRequest, authorization: str | None = Header(None)):
    """Same data as /api/products/{id}/details, but for MANY products in one
    shot — a handful of Odoo queries total instead of one full set per
    product. This is what the catalog uses for search results now, since
    fetching 10 products individually was the main thing making search feel
    slow."""
    payload = read_token(authorization)
    product_ids = body.product_ids
    if not product_ids:
        return {"results": {}}

    results = {pid: {"stock_by_warehouse": [], "packagings": [], "shop_stock": None} for pid in product_ids}

    # ---- SWAG stock, batched across all products ----
    quants = swag_execute(
        "stock.quant", "search_read",
        [[["product_id", "in", product_ids], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["product_id", "location_id", "quantity"]},
    )
    if quants:
        location_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
        locations = swag_execute("stock.location", "read", [location_ids], {"fields": ["warehouse_id"]})
        loc_to_wh = {loc["id"]: loc.get("warehouse_id") for loc in locations}
        totals: dict[tuple, float] = {}
        for q in quants:
            pid = q["product_id"][0] if q.get("product_id") else None
            loc = q.get("location_id")
            if pid is None or not loc:
                continue
            wh = loc_to_wh.get(loc[0])
            key = (pid, wh[0] if wh else None, wh[1] if wh else "Other / Unassigned")
            totals[key] = totals.get(key, 0) + q["quantity"]
        by_product: dict[int, list] = {}
        for (pid, wh_id, wh_name), qty in totals.items():
            if not qty:
                continue
            by_product.setdefault(pid, []).append({"warehouse_id": wh_id, "warehouse_name": wh_name, "quantity": qty})
        for pid, rows in by_product.items():
            for row in rows:
                if row["warehouse_id"]:
                    held = reserved_by_others(pid, row["warehouse_id"], payload["branch_key"])
                    row["quantity"] = max(0, row["quantity"] - held)
            if pid in results:
                results[pid]["stock_by_warehouse"] = rows

    # ---- Product info (base uom + alt uoms + default_code), batched ----
    prods = swag_execute("product.product", "read", [product_ids], {"fields": ["uom_id", "uom_ids", "default_code"]})
    prod_by_id = {p["id"]: p for p in prods}

    all_alt_uom_ids: set[int] = set()
    for pid, p in prod_by_id.items():
        base_uom_id = p["uom_id"][0] if p.get("uom_id") else None
        for uid in (p.get("uom_ids") or []):
            if uid != base_uom_id:
                all_alt_uom_ids.add(uid)

    uom_qty_by_id: dict[int, float | None] = {}
    uom_name_by_id: dict[int, str] = {}
    if all_alt_uom_ids:
        uom_list = list(all_alt_uom_ids)
        found = False
        for qty_field in ("factor_inv", "factor", "ratio"):
            try:
                raw = swag_execute("uom.uom", "read", [uom_list], {"fields": ["name", qty_field]})
                for u in raw:
                    uom_name_by_id[u["id"]] = u["name"]
                    uom_qty_by_id[u["id"]] = u[qty_field]
                found = True
                break
            except Exception:
                continue
        if not found:
            raw = swag_execute("uom.uom", "read", [uom_list], {"fields": ["name"]})
            for u in raw:
                uom_name_by_id[u["id"]] = u["name"]
                uom_qty_by_id[u["id"]] = None

    for pid, p in prod_by_id.items():
        if pid not in results:
            continue
        base_uom_id = p["uom_id"][0] if p.get("uom_id") else None
        alt_ids = [uid for uid in (p.get("uom_ids") or []) if uid != base_uom_id]
        pkgs = [{"id": uid, "name": uom_name_by_id.get(uid), "qty": uom_qty_by_id.get(uid)} for uid in alt_ids]
        pkgs.sort(key=lambda x: (x["qty"] is None, x["qty"]))
        results[pid]["packagings"] = pkgs

    # ---- LAROUCHE shop stock, batched by default_code ----
    codes = [prod_by_id[pid].get("default_code") for pid in product_ids if pid in prod_by_id and prod_by_id[pid].get("default_code")]
    code_to_pid = {prod_by_id[pid]["default_code"]: pid for pid in product_ids if pid in prod_by_id and prod_by_id[pid].get("default_code")}
    if codes:
        lc_products = laroche_admin_execute(
            "product.product", "search_read", [[["default_code", "in", codes]]], {"fields": ["id", "default_code", "name"]}
        )
        # Guard against a code matching more than one LAROUCHE product — same
        # "don't guess" rule as the single-product version.
        by_code: dict[str, list] = {}
        for lp in lc_products:
            by_code.setdefault(lp["default_code"], []).append(lp)

        matched_lc_ids = []
        lc_id_to_pid = {}
        for code, lps in by_code.items():
            pid = code_to_pid.get(code)
            if pid is None:
                continue
            if len(lps) == 1:
                matched_lc_ids.append(lps[0]["id"])
                lc_id_to_pid[lps[0]["id"]] = pid
                results[pid]["shop_stock"] = {"matched": True, "quantity": 0.0, "laroche_product_name": lps[0]["name"]}
            else:
                results[pid]["shop_stock"] = {"matched": False, "quantity": None, "reason": "multiple LAROUCHE products share this code"}
        for code in codes:
            if code not in by_code:
                pid = code_to_pid.get(code)
                if pid is not None:
                    results[pid]["shop_stock"] = {"matched": False, "quantity": None, "reason": "not found in LAROUCHE"}

        if matched_lc_ids:
            warehouse_id = int(payload["branch_key"])
            lc_quants = laroche_admin_execute(
                "stock.quant", "search_read",
                [[["product_id", "in", matched_lc_ids], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
                {"fields": ["product_id", "location_id", "quantity"]},
            )
            if lc_quants:
                lc_loc_ids = list({q["location_id"][0] for q in lc_quants if q.get("location_id")})
                lc_locs = laroche_admin_execute("stock.location", "read", [lc_loc_ids], {"fields": ["warehouse_id"]})
                lc_loc_to_wh = {loc["id"]: (loc["warehouse_id"][0] if loc.get("warehouse_id") else None) for loc in lc_locs}
                for q in lc_quants:
                    lc_pid = q["product_id"][0] if q.get("product_id") else None
                    loc = q.get("location_id")
                    if lc_pid is None or not loc:
                        continue
                    if lc_loc_to_wh.get(loc[0]) == warehouse_id:
                        pid = lc_id_to_pid.get(lc_pid)
                        if pid is not None:
                            results[pid]["shop_stock"]["quantity"] += q["quantity"]

    return {"results": results}


@app.get("/api/products/{product_id}/details")
def product_details_combined(product_id: int, authorization: str | None = Header(None)):
    """Combines stock-by-warehouse + packagings + my-shop-stock into ONE
    request — used by the catalog so each product card needs a single
    round-trip instead of three, which is what was making search feel slow."""
    payload = read_token(authorization)

    # --- stock by warehouse ---
    quants = swag_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    stock_by_warehouse = []
    if quants:
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
        stock_by_warehouse = [
            {"warehouse_id": wh_id, "warehouse_name": wh_name, "quantity": qty}
            for (wh_id, wh_name), qty in totals.items() if qty
        ]
        # Subtract what other branches currently have reserved in their carts,
        # so this branch doesn't try to order stock someone else is mid-way
        # through claiming.
        for row in stock_by_warehouse:
            if row["warehouse_id"]:
                held = reserved_by_others(product_id, row["warehouse_id"], payload["branch_key"])
                row["quantity"] = max(0, row["quantity"] - held)
                row["held_by_others"] = held

    # --- packagings (uom_ids) ---
    prod = swag_execute("product.product", "read", [[product_id]],
                         {"fields": ["uom_id", "uom_ids", "default_code"]})
    packagings = []
    if prod:
        base_uom = prod[0].get("uom_id") or False
        base_uom_id = base_uom[0] if base_uom else None
        alt_uom_ids = [uid for uid in (prod[0].get("uom_ids") or []) if uid != base_uom_id]
        if alt_uom_ids:
            uoms = None
            for qty_field in ("factor_inv", "factor", "ratio"):
                try:
                    raw = swag_execute("uom.uom", "read", [alt_uom_ids], {"fields": ["name", qty_field]})
                    uoms = [{"id": u["id"], "name": u["name"], "qty": u[qty_field]} for u in raw]
                    break
                except Exception:
                    uoms = None
            if uoms is None:
                raw = swag_execute("uom.uom", "read", [alt_uom_ids], {"fields": ["name"]})
                uoms = [{"id": u["id"], "name": u["name"], "qty": None} for u in raw]
            uoms.sort(key=lambda p: (p["qty"] is None, p["qty"]))
            packagings = uoms

    # --- my-shop-stock (LAROUCHE) ---
    shop_stock = {"matched": False, "quantity": None, "reason": "This product has no code to match by."}
    code = prod[0].get("default_code") if prod else None
    if code:
        laroche_products = laroche_admin_execute(
            "product.product", "search_read", [[["default_code", "=", code]]], {"fields": ["id", "name"]}
        )
        if len(laroche_products) == 1:
            lc_id = laroche_products[0]["id"]
            wh_id = int(payload["branch_key"])
            lc_quants = laroche_admin_execute(
                "stock.quant", "search_read",
                [[["product_id", "=", lc_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
                {"fields": ["location_id", "quantity"]},
            )
            total = 0.0
            if lc_quants:
                lc_loc_ids = list({q["location_id"][0] for q in lc_quants if q.get("location_id")})
                lc_locs = laroche_admin_execute("stock.location", "read", [lc_loc_ids], {"fields": ["warehouse_id"]})
                lc_loc_to_wh = {loc["id"]: (loc["warehouse_id"][0] if loc.get("warehouse_id") else None) for loc in lc_locs}
                for q in lc_quants:
                    loc = q.get("location_id")
                    if loc and lc_loc_to_wh.get(loc[0]) == wh_id:
                        total += q["quantity"]
            shop_stock = {"matched": True, "quantity": total, "laroche_product_name": laroche_products[0]["name"]}
        else:
            reason = "not found in LAROUCHE" if not laroche_products else "multiple LAROUCHE products share this code"
            shop_stock = {"matched": False, "quantity": None, "reason": reason}

    return {"stock_by_warehouse": stock_by_warehouse, "packagings": packagings, "shop_stock": shop_stock}


def laroche_shop_stock_for(swag_product_id: int, warehouse_id: int):
    """Same lookup as /api/products/{id}/my-shop-stock, but for an arbitrary
    branch's warehouse_id (used by the admin dashboard to compare any
    branch's own stock against what they ordered)."""
    recs = swag_execute("product.product", "read", [[swag_product_id]], {"fields": ["default_code"]})
    code = recs[0].get("default_code") if recs else None
    if not code:
        return {"matched": False, "quantity": None, "reason": "This product has no code to match by."}

    laroche_products = laroche_admin_execute(
        "product.product", "search_read", [[["default_code", "=", code]]], {"fields": ["id", "name"]}
    )
    if len(laroche_products) != 1:
        reason = "not found in LAROUCHE" if not laroche_products else "multiple LAROUCHE products share this code"
        return {"matched": False, "quantity": None, "reason": reason}
    lc_id = laroche_products[0]["id"]

    quants = laroche_admin_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", lc_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    total = 0.0
    if quants:
        loc_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
        locs = laroche_admin_execute("stock.location", "read", [loc_ids], {"fields": ["warehouse_id"]})
        loc_to_wh = {loc["id"]: (loc["warehouse_id"][0] if loc.get("warehouse_id") else None) for loc in locs}
        for q in quants:
            loc = q.get("location_id")
            if loc and loc_to_wh.get(loc[0]) == warehouse_id:
                total += q["quantity"]
    return {"matched": True, "quantity": total, "laroche_product_name": laroche_products[0]["name"]}


@app.get("/api/admin/orders/{record_id}/detail")
def admin_order_detail(record_id: int, authorization: str | None = Header(None)):
    """Full detail for one order in the admin dashboard — every line item,
    plus (as a comparison) how much of that same product the ordering
    branch already has in its own shop right now."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        row = db.query(database.OrderRecord).filter(database.OrderRecord.id == record_id).first()
        if not row:
            raise HTTPException(404, "Order record not found.")
        items = json.loads(row.items_json)
    finally:
        db.close()

    branch_warehouse_id = int(row.branch_key) if row.branch_key and row.branch_key.isdigit() else None

    lines = []
    for it in items:
        product_id = it.get("product_id")
        prod = swag_execute("product.product", "read", [[product_id]],
                             {"fields": ["default_code", "name", "list_price"]}) if product_id else []
        pname = prod[0]["name"] if prod else f"Product #{product_id}"
        pcode = prod[0].get("default_code") if prod else None
        price = prod[0].get("list_price") if prod else 0
        qty = it.get("qty", 0)

        branch_stock = None
        if branch_warehouse_id and product_id:
            try:
                branch_stock = laroche_shop_stock_for(product_id, branch_warehouse_id)
            except Exception:
                branch_stock = {"matched": False, "quantity": None, "reason": "lookup failed"}

        lines.append({
            "product_id": product_id,
            "default_code": pcode,
            "name": pname,
            "qty_ordered": qty,
            "price": price,
            "subtotal": qty * (price or 0),
            "branch_current_stock": branch_stock,
        })

    return {
        "id": row.id,
        "swag_order_name": row.swag_order_name,
        "branch_name": row.branch_name,
        "employee_name": row.employee_name,
        "warehouse_name": row.warehouse_name,
        "amount_total": row.amount_total,
        "created_at": row.created_at.isoformat(),
        "lines": lines,
    }


@app.get("/api/products/{product_id}/stock")
def product_stock_by_warehouse(product_id: int, authorization: str | None = Header(None)):
    """How much of this product sits in EACH SWAG warehouse — not just the
    total. Reads stock.quant (per-location on-hand quantity), then maps each
    location back to its warehouse."""
    read_token(authorization)

    quants = swag_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    if not quants:
        return {"stock_by_warehouse": []}

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

    result = [
        {"warehouse_id": wh_id, "warehouse_name": wh_name, "quantity": qty}
        for (wh_id, wh_name), qty in totals.items()
        if qty  # drop zero totals after summing
    ]
    result.sort(key=lambda r: r["warehouse_name"] or "")
    return {"stock_by_warehouse": result}


@app.get("/api/products/{product_id}/packagings")
def product_packagings(product_id: int, authorization: str | None = Header(None)):
    """SWAG's own 'Packagings' — in this instance these are actually
    alternate Units of Measure (the `uom_ids` field on the product, labeled
    'Packagings' in the UI), e.g. a 'P48' unit worth 48 base units. Returns
    each alternate unit's id/name/pieces-per-unit, excluding the product's
    own base unit (ordering "1 unit" isn't offered when packagings exist)."""
    read_token(authorization)

    prod = swag_execute("product.product", "read", [[product_id]], {"fields": ["uom_id", "uom_ids"]})
    if not prod:
        return {"packagings": []}

    base_uom = prod[0].get("uom_id") or False
    base_uom_id = base_uom[0] if base_uom else None
    alt_uom_ids = prod[0].get("uom_ids") or []
    alt_uom_ids = [uid for uid in alt_uom_ids if uid != base_uom_id]
    if not alt_uom_ids:
        return {"packagings": []}

    uoms = None
    for qty_field in ("factor_inv", "factor", "ratio"):
        try:
            uoms = swag_execute("uom.uom", "read", [alt_uom_ids], {"fields": ["name", qty_field]})
            uoms = [{"id": u["id"], "name": u["name"], "qty": u[qty_field]} for u in uoms]
            break
        except Exception:
            uoms = None
    if uoms is None:
        # Last resort: just names, qty unknown (still lets branch pick the box by name)
        raw = swag_execute("uom.uom", "read", [alt_uom_ids], {"fields": ["name"]})
        uoms = [{"id": u["id"], "name": u["name"], "qty": None} for u in raw]
    packagings = uoms
    packagings.sort(key=lambda p: (p["qty"] is None, p["qty"]))
    return {"packagings": packagings}


@app.get("/api/products/{product_id}/my-shop-stock")
def product_my_shop_stock(product_id: int, authorization: str | None = Header(None)):
    """How much of this SAME product the branch already has on hand in
    THEIR OWN shop — i.e. LAROUCHE's own stock at their specific outlet
    (warehouse), not SWAG's supplier-side stock. Matched by product code
    (default_code) between the two separate Odoo systems."""
    payload = read_token(authorization)

    recs = swag_execute("product.product", "read", [[product_id]], {"fields": ["default_code", "name"]})
    code = recs[0].get("default_code") if recs else None
    if not code:
        return {"matched": False, "quantity": None, "reason": "This product has no code to match by."}

    laroche_products = laroche_admin_execute(
        "product.product", "search_read", [[["default_code", "=", code]]], {"fields": ["id", "name"]}
    )
    if len(laroche_products) != 1:
        reason = "not found in LAROUCHE" if not laroche_products else "multiple LAROUCHE products share this code"
        return {"matched": False, "quantity": None, "reason": reason}
    laroche_product_id = laroche_products[0]["id"]

    warehouse_id = int(payload["branch_key"])
    quants = laroche_admin_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", laroche_product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    if not quants:
        return {"matched": True, "quantity": 0, "laroche_product_name": laroche_products[0]["name"]}

    location_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
    locations = laroche_admin_execute("stock.location", "read", [location_ids], {"fields": ["warehouse_id"]})
    loc_to_wh_id = {loc["id"]: (loc["warehouse_id"][0] if loc.get("warehouse_id") else None) for loc in locations}

    total = 0.0
    for q in quants:
        loc = q.get("location_id")
        if not loc:
            continue
        if loc_to_wh_id.get(loc[0]) == warehouse_id:
            total += q["quantity"]

    return {"matched": True, "quantity": total, "laroche_product_name": laroche_products[0]["name"]}


@app.get("/api/swag-warehouses")
def list_swag_warehouses(authorization: str | None = Header(None)):
    """SWAG's own warehouses — the branch picks which one should fulfill
    (source stock for) their order."""
    read_token(authorization)
    warehouses = swag_execute(
        "stock.warehouse", "search_read", [[]], {"fields": ["id", "name", "code"], "order": "name asc"}
    )
    return {"warehouses": warehouses}


def _create_swag_order(swag_partner_id, items, warehouse_id, note, employee_name, branch_name, branch_key):
    """Core order-creation logic — shared by the direct 'Submit' path and the
    manager-approval path (once a pending order is approved)."""
    # This is the final guard before anything is sent to SWAG. It protects
    # direct orders and both approval paths from stale pending rows/tokens.
    assert_current_branch_partner(branch_key, swag_partner_id)
    line_warehouse_field = find_field_by_label("sale.order.line", ["warehouse"]) if warehouse_id else None

    order_lines = []
    for item in items:
        # "qty" is always the TOTAL PIECES (already computed from box-count ×
        # pack-size on the frontend when packaging was used) — we deliberately
        # do NOT set product_uom_id to the box unit here. Setting it would
        # make Odoo display "Quantity: 1, Unit: P12" (technically correct —
        # 1 box of 12 — but confusing at a glance), so instead we record the
        # line in the product's normal base unit, showing "Quantity: 12"
        # directly, matching what branches expect to see.
        line_vals = {"product_id": item["product_id"], "product_uom_qty": item["qty"]}
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

    # Odoo defaults the new order's salesperson (user_id) to whichever
    # account is making the API call (our shared service account) rather
    # than the customer's own pre-assigned salesperson. If this customer has
    # one configured, force it onto the order — same pattern as the
    # warehouse_id fix below.
    try:
        partner_recs = swag_execute("res.partner", "read", [[swag_partner_id]], {"fields": ["user_id"]})
        partner_salesperson = partner_recs[0].get("user_id") if partner_recs else False
        if partner_salesperson:
            swag_execute("sale.order", "write", [[order_id], {"user_id": partner_salesperson[0]}])
    except Exception:
        pass  # non-critical — order still gets created either way

    # Odoo's warehouse_id is a computed field (defaults from the partner/
    # company) that can silently override whatever we pass at create() time.
    # Writing it again right after forces our explicit choice to stick.
    if warehouse_id:
        swag_execute("sale.order", "write", [[order_id], {"warehouse_id": warehouse_id}])

    # Same force-write for the custom per-LINE warehouse field, if this SWAG
    # instance has one (separate from the order-header warehouse_id above).
    if warehouse_id and line_warehouse_field:
        try:
            line_ids = swag_execute("sale.order.line", "search", [[["order_id", "=", order_id]]])
            if line_ids:
                swag_execute("sale.order.line", "write", [line_ids, {line_warehouse_field: warehouse_id}])
        except Exception:
            pass  # non-critical — order still gets created either way

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
        "order_name": order.get("name"),
        "customer": _m2o_name(order.get("partner_id")),
        "invoice_address": _m2o_name(order.get("partner_invoice_id")),
        "delivery_address": _m2o_name(order.get("partner_shipping_id")),
        "pricelist": _m2o_name(order.get("pricelist_id")),
        "payment_terms": _m2o_name(order.get("payment_term_id")),
        "salesperson": _m2o_name(order.get("user_id")),
        "warehouse": _m2o_name(order.get("warehouse_id")),
        "collector": _m2o_name(order.get(collector_field)) if collector_field else None,
        "order_date": order.get("date_order"),
        "amount_untaxed": order.get("amount_untaxed"),
        "amount_tax": order.get("amount_tax"),
        "amount_total": order.get("amount_total"),
        "state": order.get("state"),
        "ordered_by": employee_name,
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

    return order_id, details


@app.post("/api/orders")
def check_branch_order_limit(branch_key: str, new_order_value: float):
    daily_limit, monthly_limit = database.get_branch_limits(branch_key)
    if daily_limit is None and monthly_limit is None:
        return
    db = database.get_session()
    try:
        now = datetime.now(timezone.utc)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        rows = (
            db.query(database.OrderRecord)
            .filter(database.OrderRecord.branch_key == branch_key)
            .all()
        )
        today_total = sum(
            r.amount_total or 0 for r in rows
            if (r.created_at if r.created_at.tzinfo else r.created_at.replace(tzinfo=timezone.utc)) >= day_start
        )
        month_total = sum(
            r.amount_total or 0 for r in rows
            if (r.created_at if r.created_at.tzinfo else r.created_at.replace(tzinfo=timezone.utc)) >= month_start
        )
    finally:
        db.close()

    if daily_limit is not None and today_total + new_order_value > daily_limit:
        raise HTTPException(
            403,
            f"This would exceed your branch's daily order limit ({daily_limit}). "
            f"Already ordered {today_total:.2f} today.",
        )
    if monthly_limit is not None and month_total + new_order_value > monthly_limit:
        raise HTTPException(
            403,
            f"This would exceed your branch's monthly order limit ({monthly_limit}). "
            f"Already ordered {month_total:.2f} this month.",
        )


def check_maintenance_mode():
    if database.get_setting("maintenance_mode") == "1":
        message = database.get_setting("maintenance_message") or "Ordering is temporarily paused for maintenance. Please try again shortly."
        raise HTTPException(503, message)


def create_order(body: OrderRequest, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    check_maintenance_mode()
    if not body.items:
        raise HTTPException(400, "Cart is empty.")

    employee_name = payload.get("employee_name") or "unknown"
    items = [
        {"product_id": it.product_id, "qty": it.qty, "packaging_id": it.packaging_id, "packaging_qty": it.packaging_qty}
        for it in body.items
    ]
    verify_stock_before_order(items, body.warehouse_id, payload["branch_key"])

    estimated_value = 0.0
    for it in items:
        try:
            recs = swag_execute("product.product", "read", [[it["product_id"]]], {"fields": ["list_price"]})
            estimated_value += it["qty"] * (recs[0]["list_price"] if recs else 0)
        except Exception:
            pass
    check_branch_order_limit(payload["branch_key"], estimated_value)

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

    # Look up names/prices for a readable summary later.
    items_with_names = []
    for it in body.items:
        recs = swag_execute("product.product", "read", [[it.product_id]],
                             {"fields": ["default_code", "name", "list_price"]})
        rec = recs[0] if recs else {}
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
    """Every pending/approved/rejected order for this branch — the approval queue."""
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
    """Approve a pending order — THIS is when it actually gets created in SWAG."""
    payload = read_token(authorization)
    check_maintenance_mode()
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
        verify_stock_before_order(order_items, row.warehouse_id, row.branch_key)
        check_branch_order_limit(row.branch_key, row.amount_total or 0)
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
    """Used by the Excel bulk-order upload: given a list of product codes
    (default_code), find each one's exact match in SWAG. Codes that don't
    match anything (or match more than one product) are returned separately
    so the branch can review them by hand — never guessed."""
    read_token(authorization)
    codes = [c.strip() for c in body.codes if c and c.strip()]
    matched = []
    not_found = []
    for code in codes:
        recs = swag_execute(
            "product.product", "search_read",
            [[["default_code", "=", code]]],
            {"fields": ["id", "default_code", "name", "list_price", "qty_available"]},
        )
        if len(recs) == 1:
            matched.append(recs[0])
        else:
            not_found.append(code)
    return {"matched": matched, "not_found": not_found}


@app.get("/api/products/by-barcode")
def product_by_barcode(code: str, authorization: str | None = Header(None)):
    """Used by the barcode scanner — look up a product by its barcode.
    Tries an exact match first (fast, precise), then falls back to
    leading-zero variants and a partial match — some scanners/labels differ
    slightly in exactly how the digits are formatted."""
    read_token(authorization)
    code = (code or "").strip()
    if not code:
        raise HTTPException(400, "No barcode provided.")

    fields = ["id", "default_code", "name", "list_price", "qty_available"]

    recs = swag_execute("product.product", "search_read", [[["barcode", "=", code]]], {"fields": fields})

    if not recs:
        # Try with/without a leading zero — a common source of mismatch
        # between what a scanner outputs and what's stored.
        variants = {code.lstrip("0"), "0" + code}
        variants.discard(code)
        for variant in variants:
            if not variant:
                continue
            recs = swag_execute("product.product", "search_read", [[["barcode", "=", variant]]], {"fields": fields})
            if recs:
                break

    if not recs:
        # Last resort: partial match, in case of stray characters/whitespace
        # baked into the stored barcode value.
        recs = swag_execute("product.product", "search_read", [[["barcode", "ilike", code]]], {"fields": fields})

    if not recs:
        raise HTTPException(404, f"No product found with barcode {code}.")
    if len(recs) > 1:
        raise HTTPException(409, f"Multiple products share barcode {code} — search by name/code instead.")
    return {"product": recs[0]}


@app.get("/api/my-orders")
def my_orders(limit: int = 50, authorization: str | None = Header(None)):
    """Order history for the logged-in branch's own SWAG customer — powers
    the 'My Orders' dashboard."""
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


@app.get("/api/top-products")
def top_products(limit: int = 5, authorization: str | None = Header(None)):
    """Most frequently ordered products for this branch's own SWAG customer
    — aggregated across their order history."""
    payload = read_token(authorization)
    orders = swag_execute(
        "sale.order", "search_read",
        [[["partner_id", "=", payload["swag_partner_id"]]]],
        {"fields": ["id"], "limit": 200},
    )
    order_ids = [o["id"] for o in orders]
    if not order_ids:
        return {"products": []}

    lines = swag_execute(
        "sale.order.line", "search_read",
        [[["order_id", "in", order_ids], ["display_type", "=", False]]],
        {"fields": ["product_id", "product_uom_qty", "price_subtotal"]},
    )
    totals: dict[int, dict] = {}
    for ln in lines:
        pid = ln.get("product_id")
        if not pid:
            continue
        entry = totals.setdefault(pid[0], {"name": pid[1], "qty": 0, "revenue": 0})
        entry["qty"] += ln.get("product_uom_qty") or 0
        entry["revenue"] += ln.get("price_subtotal") or 0

    result = sorted(totals.values(), key=lambda t: t["qty"], reverse=True)[:limit]
    return {"products": result}


@app.get("/api/orders/{order_id}")
def order_detail(order_id: int, authorization: str | None = Header(None)):
    """Full detail (including line items) for one order in the dashboard.
    Scoped to the logged-in branch's own SWAG customer — can't look up an
    arbitrary order id belonging to someone else."""
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
    """Every time this branch has logged in — the audit trail requested for
    accountability, stored in our own tracking database (not Odoo)."""
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
    """Daily order count + value for this branch, from our own tracking
    database (orders placed through this portal) — powers the dashboard
    chart. This complements /api/my-orders (which reads the full, live
    authoritative history straight from SWAG)."""
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
# ADMIN DASHBOARD — separate login (shared password), combined view across
# every branch. Reads only from our own tracking database, never touches
# Odoo directly, so it stays fast regardless of how many branches exist.
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
    """Every branch that has placed at least one order, with its own totals
    — powers the comparison table/chart."""
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
        now = datetime.now(timezone.utc)
        for b in result:
            last = b["last_order_at"]
            days_since = None
            if last:
                last_cmp = last if last.tzinfo else last.replace(tzinfo=timezone.utc)
                days_since = (now - last_cmp).days
            b["days_since_last_order"] = days_since
            b["is_inactive"] = days_since is not None and days_since >= 10
            b["last_order_at"] = last.isoformat() if last else None
        return {"branches": result}
    finally:
        db.close()


@app.get("/api/admin/branches/{branch_key}/detail")
def admin_branch_detail(branch_key: str, authorization: str | None = Header(None)):
    """Everything about ONE specific branch — its own order history, login
    history, and its own most-ordered products — for the admin drill-down."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        order_rows = (
            db.query(database.OrderRecord)
            .filter(database.OrderRecord.branch_key == branch_key)
            .order_by(database.OrderRecord.created_at.desc())
            .all()
        )
        login_rows = (
            db.query(database.LoginEvent)
            .filter(database.LoginEvent.branch_key == branch_key)
            .order_by(database.LoginEvent.created_at.desc())
            .limit(20)
            .all()
        )
    finally:
        db.close()

    branch_name = order_rows[0].branch_name if order_rows else (login_rows[0].branch_name if login_rows else branch_key)

    # This branch's own top products, from its own order history only.
    totals: dict[int, dict] = {}
    for o in order_rows:
        try:
            items = json.loads(o.items_json or "[]")
        except Exception:
            continue
        for it in items:
            pid = it.get("product_id")
            if not pid:
                continue
            bucket = totals.setdefault(pid, {"product_id": pid, "qty_ordered": 0.0, "order_count": 0})
            bucket["qty_ordered"] += it.get("qty", 0) or 0
            bucket["order_count"] += 1
    top_products = sorted(totals.values(), key=lambda b: b["qty_ordered"], reverse=True)[:10]
    for row in top_products:
        try:
            recs = swag_execute("product.product", "read", [[row["product_id"]]], {"fields": ["default_code", "name"]})
            row["default_code"] = recs[0].get("default_code") if recs else None
            row["name"] = recs[0].get("name") if recs else f"Product #{row['product_id']}"
        except Exception:
            row["default_code"] = None
            row["name"] = f"Product #{row['product_id']}"

    return {
        "branch_key": branch_key,
        "branch_name": branch_name,
        "orders": [
            {
                "id": o.id,
                "swag_order_name": o.swag_order_name,
                "employee_name": o.employee_name,
                "warehouse_name": o.warehouse_name,
                "item_count": o.item_count,
                "created_at": o.created_at.isoformat(),
            }
            for o in order_rows
        ],
        "logins": [
            {"employee_name": r.employee_name, "created_at": r.created_at.isoformat()}
            for r in login_rows
        ],
        "top_products": top_products,
    }


@app.get("/api/admin/top-products")
def admin_top_products(limit: int = 15, authorization: str | None = Header(None)):
    """Which products get ordered the most, across ALL branches combined —
    aggregated from every order's stored line items."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        orders = db.query(database.OrderRecord).all()
    finally:
        db.close()

    totals: dict[int, dict] = {}
    for o in orders:
        try:
            items = json.loads(o.items_json or "[]")
        except Exception:
            continue
        for it in items:
            pid = it.get("product_id")
            if not pid:
                continue
            bucket = totals.setdefault(pid, {"product_id": pid, "qty_ordered": 0.0, "order_count": 0})
            bucket["qty_ordered"] += it.get("qty", 0) or 0
            bucket["order_count"] += 1

    top = sorted(totals.values(), key=lambda b: b["qty_ordered"], reverse=True)[:limit]

    for row in top:
        try:
            recs = swag_execute("product.product", "read", [[row["product_id"]]],
                                 {"fields": ["default_code", "name"]})
            row["default_code"] = recs[0].get("default_code") if recs else None
            row["name"] = recs[0].get("name") if recs else f"Product #{row['product_id']}"
        except Exception:
            row["default_code"] = None
            row["name"] = f"Product #{row['product_id']}"

    return {"products": top}


@app.get("/api/admin/graph")
def admin_graph(days: int = 30, authorization: str | None = Header(None)):
    """Daily order count + value across ALL branches combined — the overall
    trend line for the admin dashboard."""
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
    """Every order placed through the portal, across all branches — the
    full raw list behind the summary/comparison views."""
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
    """Every branch -> SWAG customer mapping currently in effect, with the
    best-known branch name (from login/order history) and the live SWAG
    customer name — lets an admin spot and fix a wrong match immediately."""
    read_admin_token(authorization)
    mapping = load_branch_partner_map()

    db = database.get_session()
    try:
        name_by_key: dict[str, str] = {}
        for row in db.query(database.LoginEvent).order_by(database.LoginEvent.created_at.desc()).all():
            name_by_key.setdefault(row.branch_key, row.branch_name)
    finally:
        db.close()

    result = []
    for branch_key, swag_partner_id in mapping.items():
        swag_name = None
        try:
            recs = swag_execute("res.partner", "read", [[swag_partner_id]], {"fields": ["name"]})
            swag_name = recs[0]["name"] if recs else None
        except Exception:
            pass
        result.append({
            "branch_key": branch_key,
            "branch_name": name_by_key.get(branch_key, "(unknown — no login recorded yet)"),
            "swag_partner_id": swag_partner_id,
            "swag_partner_name": swag_name or "(SWAG partner not found — may have been deleted)",
        })
    result.sort(key=lambda r: r["branch_name"])
    return {"mappings": result}


@app.post("/api/admin/mappings/clear")
def admin_clear_mapping(body: ClearMappingRequest, authorization: str | None = Header(None)):
    """Admin-triggered version of the branch's own 'report wrong customer' —
    clears one branch's saved mapping so its next login re-verifies from
    scratch."""
    read_admin_token(authorization)
    mapping = load_branch_partner_map()
    if body.branch_key in mapping:
        del mapping[body.branch_key]
        save_branch_partner_map(mapping)
    _resolved_cache.pop(body.branch_key, None)
    database.log_admin_action("mapping_cleared", f"branch_key={body.branch_key}")
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# STOCK RESERVATIONS — a short hold while an item sits in someone's cart, so
# two branches don't both try to order the last few pieces of the same
# product from the same warehouse at the same time.
# ─────────────────────────────────────────────────────────────────────────────
def reservation_ttl_minutes() -> int:
    val = database.get_setting("reservation_ttl_minutes")
    try:
        return int(val) if val else 15
    except ValueError:
        return 15


class ReserveRequest(BaseModel):
    product_id: int
    warehouse_id: int
    qty: float


def reserved_by_others(product_id: int, warehouse_id: int, own_branch_key: str) -> float:
    """Total quantity other branches currently have reserved (not expired)
    for this exact product+warehouse — subtract this from raw SWAG stock
    before showing/allowing an order."""
    db = database.get_session()
    try:
        now = datetime.now(timezone.utc)
        rows = (
            db.query(database.StockReservation)
            .filter(
                database.StockReservation.product_id == product_id,
                database.StockReservation.warehouse_id == warehouse_id,
                database.StockReservation.branch_key != own_branch_key,
                database.StockReservation.expires_at > now,
            )
            .all()
        )
        return sum(r.qty for r in rows)
    finally:
        db.close()


def get_available_stock(product_id: int, warehouse_id: int, own_branch_key: str) -> float:
    """Fresh SWAG stock for this product in this warehouse, minus whatever
    other branches currently have reserved."""
    quants = swag_execute(
        "stock.quant", "search_read",
        [[["product_id", "=", product_id], ["location_id.usage", "=", "internal"], ["quantity", "!=", 0]]],
        {"fields": ["location_id", "quantity"]},
    )
    if not quants:
        return 0.0
    location_ids = list({q["location_id"][0] for q in quants if q.get("location_id")})
    locations = swag_execute("stock.location", "read", [location_ids], {"fields": ["warehouse_id"]})
    loc_to_wh = {loc["id"]: (loc["warehouse_id"][0] if loc.get("warehouse_id") else None) for loc in locations}
    total = 0.0
    for q in quants:
        loc = q.get("location_id")
        if loc and loc_to_wh.get(loc[0]) == warehouse_id:
            total += q["quantity"]
    return max(0.0, total - reserved_by_others(product_id, warehouse_id, own_branch_key))


def verify_stock_before_order(items: list[dict], warehouse_id: int | None, branch_key: str):
    """Last-second re-check right before actually creating an order — stock
    shown a few minutes ago in the catalog may have moved on. Raises a clear
    409 error naming exactly which product(s) fell short, instead of letting
    SWAG silently oversell."""
    shortfalls = []
    if warehouse_id:
        for item in items:
            available = get_available_stock(item["product_id"], warehouse_id, branch_key)
            needed = item["qty"]
            if needed > available:
                shortfalls.append(f"product #{item['product_id']} (need {needed}, only {available} left)")
    else:
        # No specific warehouse was chosen (e.g. an older "Reorder" flow) —
        # we can't check one exact warehouse, but we can still confirm the
        # product isn't fully out of stock everywhere before letting it
        # through, rather than skipping verification entirely.
        for item in items:
            recs = swag_execute("product.product", "read", [[item["product_id"]]], {"fields": ["qty_available"]})
            available = recs[0]["qty_available"] if recs else 0
            needed = item["qty"]
            if needed > available:
                shortfalls.append(f"product #{item['product_id']} (need {needed}, only {available} left anywhere)")
    if shortfalls:
        raise HTTPException(
            409,
            "Stock changed since you added these items — " + "; ".join(shortfalls) +
            ". Please adjust your cart and try again.",
        )


@app.post("/api/reservations")
def upsert_reservation(body: ReserveRequest, authorization: str | None = Header(None)):
    """Called when a branch adds/updates a cart line — holds that quantity
    for RESERVATION_TTL_MINUTES, refreshing the timer each time the qty
    changes so an active cart never expires mid-edit."""
    payload = read_token(authorization)
    branch_key = payload["branch_key"]
    db = database.get_session()
    try:
        row = (
            db.query(database.StockReservation)
            .filter(
                database.StockReservation.product_id == body.product_id,
                database.StockReservation.warehouse_id == body.warehouse_id,
                database.StockReservation.branch_key == branch_key,
            )
            .first()
        )
        ttl = reservation_ttl_minutes()
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=ttl)
        if row:
            row.qty = body.qty
            row.expires_at = expires_at
        else:
            db.add(database.StockReservation(
                product_id=body.product_id, warehouse_id=body.warehouse_id,
                branch_key=branch_key, qty=body.qty, expires_at=expires_at,
            ))
        db.commit()
        return {"ok": True, "expires_in_minutes": ttl}
    finally:
        db.close()


@app.delete("/api/reservations/{product_id}/{warehouse_id}")
def release_reservation(product_id: int, warehouse_id: int, authorization: str | None = Header(None)):
    """Called when a branch removes an item from their cart, or right after
    successfully submitting an order (the real SWAG stock now reflects it,
    so the temporary hold is no longer needed)."""
    payload = read_token(authorization)
    branch_key = payload["branch_key"]
    db = database.get_session()
    try:
        db.query(database.StockReservation).filter(
            database.StockReservation.product_id == product_id,
            database.StockReservation.warehouse_id == warehouse_id,
            database.StockReservation.branch_key == branch_key,
        ).delete()
        db.commit()
        return {"ok": True}
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# ORDER TEMPLATES — save the current cart under a name, reuse it later.
# ─────────────────────────────────────────────────────────────────────────────
class SaveTemplateRequest(BaseModel):
    name: str
    items: list[dict]  # [{product_id, default_code, name, qty, price, packaging_id, packaging_qty}]


@app.post("/api/templates")
def save_template(body: SaveTemplateRequest, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    if not body.items:
        raise HTTPException(400, "Cart is empty — nothing to save.")
    db = database.get_session()
    try:
        row = database.OrderTemplate(
            branch_key=payload["branch_key"],
            name=body.name.strip() or "Untitled",
            items_json=json.dumps(body.items),
        )
        db.add(row)
        db.commit()
        return {"ok": True, "id": row.id}
    finally:
        db.close()


@app.get("/api/templates")
def list_templates(authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        rows = (
            db.query(database.OrderTemplate)
            .filter(database.OrderTemplate.branch_key == payload["branch_key"])
            .order_by(database.OrderTemplate.created_at.desc())
            .all()
        )
        return {
            "templates": [
                {
                    "id": r.id,
                    "name": r.name,
                    "item_count": len(json.loads(r.items_json or "[]")),
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@app.get("/api/templates/{template_id}")
def get_template(template_id: int, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        row = db.query(database.OrderTemplate).filter(database.OrderTemplate.id == template_id).first()
        if not row:
            raise HTTPException(404, "Template not found.")
        if row.branch_key != payload["branch_key"]:
            raise HTTPException(403, "This template doesn't belong to your outlet.")
        return {"id": row.id, "name": row.name, "items": json.loads(row.items_json or "[]")}
    finally:
        db.close()


@app.delete("/api/templates/{template_id}")
def delete_template(template_id: int, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        row = db.query(database.OrderTemplate).filter(database.OrderTemplate.id == template_id).first()
        if not row:
            raise HTTPException(404, "Template not found.")
        if row.branch_key != payload["branch_key"]:
            raise HTTPException(403, "This template doesn't belong to your outlet.")
        db.delete(row)
        db.commit()
        return {"ok": True}
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# ANOMALY DETECTION — warn if a quantity is way above this branch's own
# historical average for that product, catching typos before they're submitted.
# ─────────────────────────────────────────────────────────────────────────────
class AnomalyCheckRequest(BaseModel):
    items: list[dict]  # [{product_id, qty}]


@app.post("/api/check-anomaly")
def check_anomaly(body: AnomalyCheckRequest, authorization: str | None = Header(None)):
    payload = read_token(authorization)
    db = database.get_session()
    try:
        past_orders = (
            db.query(database.OrderRecord)
            .filter(database.OrderRecord.branch_key == payload["branch_key"])
            .all()
        )
    finally:
        db.close()

    # Build a history of quantities ordered per product, from past orders.
    history: dict[int, list[float]] = {}
    for o in past_orders:
        try:
            items = json.loads(o.items_json or "[]")
        except Exception:
            continue
        for it in items:
            pid = it.get("product_id")
            if pid:
                history.setdefault(pid, []).append(it.get("qty", 0) or 0)

    warnings = []
    for item in body.items:
        pid = item.get("product_id")
        qty = item.get("qty", 0) or 0
        past_qtys = history.get(pid, [])
        # Need at least 2 past orders of this exact product before we trust
        # an "average" enough to flag something as unusual.
        if len(past_qtys) < 2:
            continue
        avg = sum(past_qtys) / len(past_qtys)
        if avg > 0 and qty > avg * 3:
            warnings.append({
                "product_id": pid,
                "qty": qty,
                "usual_avg": round(avg, 1),
            })

    return {"warnings": warnings}


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN CONTROL — branch access, order cancel, bulk approvals, per-product
# minimums, maintenance mode.
# ─────────────────────────────────────────────────────────────────────────────
class BranchAccessRequest(BaseModel):
    disabled: bool
    reason: str | None = None


@app.post("/api/admin/branches/{branch_key}/access")
def admin_set_branch_access(branch_key: str, body: BranchAccessRequest, authorization: str | None = Header(None)):
    """Turn a branch's access to this portal on/off — their real LAROUCHE
    account is untouched, only whether THIS app will let them in."""
    read_admin_token(authorization)
    database.set_branch_disabled(branch_key, body.disabled, body.reason)
    database.log_admin_action(
        "branch_access_disabled" if body.disabled else "branch_access_enabled",
        f"branch_key={branch_key} reason={body.reason or ''}",
    )
    return {"ok": True}


@app.get("/api/admin/branches-access")
def admin_list_branch_access(authorization: str | None = Header(None)):
    """Every branch currently disabled, for the admin UI to show a status list."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        rows = db.query(database.BranchAccessControl).filter(database.BranchAccessControl.is_disabled == 1).all()
        return {"disabled": [{"branch_key": r.branch_key, "reason": r.reason} for r in rows]}
    finally:
        db.close()


@app.post("/api/admin/orders/{record_id}/cancel")
def admin_cancel_order(record_id: int, authorization: str | None = Header(None)):
    """Cancel a real SWAG order that was placed wrongly — sets its state to
    cancelled in SWAG directly (this is a real, immediate change there)."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        row = db.query(database.OrderRecord).filter(database.OrderRecord.id == record_id).first()
        if not row:
            raise HTTPException(404, "Order record not found.")
        swag_order_id = row.swag_order_id
    finally:
        db.close()
    swag_execute("sale.order", "write", [[swag_order_id], {"state": "cancel"}])
    database.log_admin_action("order_cancelled", f"record_id={record_id} swag_order_id={swag_order_id}")
    return {"ok": True}


class BulkDecideRequest(BaseModel):
    ids: list[int]
    action: str  # "approve" | "reject"
    reason: str | None = None


@app.get("/api/admin/pending-orders")
def admin_list_pending_orders(authorization: str | None = Header(None)):
    """Every pending-approval order across ALL branches — for the admin
    bulk-approve/reject view."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        rows = (
            db.query(database.PendingOrder)
            .filter(database.PendingOrder.status == "pending")
            .order_by(database.PendingOrder.created_at.desc())
            .all()
        )
        return {
            "pending_orders": [
                {
                    "id": r.id,
                    "branch_name": r.branch_name,
                    "requested_by": r.requested_by,
                    "items": json.loads(r.items_json),
                    "amount_total": r.amount_total,
                    "created_at": r.created_at.isoformat(),
                }
                for r in rows
            ]
        }
    finally:
        db.close()


@app.post("/api/admin/pending-orders/bulk-decide")
def admin_bulk_decide(body: BulkDecideRequest, authorization: str | None = Header(None)):
    """Approve or reject many pending orders at once, across any branch."""
    read_admin_token(authorization)
    if body.action not in ("approve", "reject"):
        raise HTTPException(400, "action must be 'approve' or 'reject'.")
    database.log_admin_action(f"bulk_{body.action}", f"ids={body.ids}")

    results = []
    for pending_id in body.ids:
        db = database.get_session()
        try:
            row = db.query(database.PendingOrder).filter(database.PendingOrder.id == pending_id).first()
            if not row or row.status != "pending":
                results.append({"id": pending_id, "ok": False, "error": "not found or already decided"})
                continue

            if body.action == "reject":
                row.status = "rejected"
                row.decided_by = "admin"
                row.decided_at = datetime.now(timezone.utc)
                row.note = (row.note or "") + (f"\nRejected: {body.reason}" if body.reason else "")
                db.commit()
                results.append({"id": pending_id, "ok": True, "status": "rejected"})
            else:
                items = json.loads(row.items_json)
                order_items = [
                    {"product_id": i["product_id"], "qty": i["qty"],
                     "packaging_id": i.get("packaging_id"), "packaging_qty": i.get("packaging_qty")}
                    for i in items
                ]
                try:
                    check_maintenance_mode()
                    verify_stock_before_order(order_items, row.warehouse_id, row.branch_key)
                    order_id, details = _create_swag_order(
                        row.swag_partner_id, order_items, row.warehouse_id, row.note,
                        row.requested_by, row.branch_name, row.branch_key,
                    )
                    row.status = "approved"
                    row.decided_by = "admin"
                    row.decided_at = datetime.now(timezone.utc)
                    row.swag_order_id = order_id
                    row.swag_order_name = details["order_name"]
                    db.commit()
                    results.append({"id": pending_id, "ok": True, "status": "approved", "order_name": details["order_name"]})
                except HTTPException as e:
                    results.append({"id": pending_id, "ok": False, "error": e.detail})
        finally:
            db.close()

    return {"results": results}


class MinQtyRequest(BaseModel):
    product_id: int
    min_qty: int


@app.get("/api/admin/product-min-overrides")
def admin_list_min_overrides(authorization: str | None = Header(None)):
    read_admin_token(authorization)
    overrides = database.get_all_min_overrides()
    result = []
    for pid, min_qty in overrides.items():
        name = f"Product #{pid}"
        try:
            recs = swag_execute("product.product", "read", [[pid]], {"fields": ["default_code", "name"]})
            if recs:
                name = f"{recs[0].get('default_code') or ''} {recs[0].get('name') or ''}".strip()
        except Exception:
            pass
        result.append({"product_id": pid, "min_qty": min_qty, "name": name})
    return {"overrides": result}


@app.post("/api/admin/product-min-overrides")
def admin_set_min_override(body: MinQtyRequest, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    if body.min_qty < 1:
        raise HTTPException(400, "min_qty must be at least 1.")
    database.set_product_min_qty(body.product_id, body.min_qty)
    database.log_admin_action("min_qty_set", f"product_id={body.product_id} min_qty={body.min_qty}")
    return {"ok": True}


@app.delete("/api/admin/product-min-overrides/{product_id}")
def admin_delete_min_override(product_id: int, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    database.delete_product_min_override(product_id)
    database.log_admin_action("min_qty_override_removed", f"product_id={product_id}")
    return {"ok": True}


class MaintenanceRequest(BaseModel):
    enabled: bool
    message: str | None = None


@app.get("/api/admin/maintenance")
def admin_get_maintenance(authorization: str | None = Header(None)):
    read_admin_token(authorization)
    return {
        "enabled": database.get_setting("maintenance_mode") == "1",
        "message": database.get_setting("maintenance_message") or "",
    }


@app.post("/api/admin/maintenance")
def admin_set_maintenance(body: MaintenanceRequest, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    database.set_setting("maintenance_mode", "1" if body.enabled else "0")
    if body.message:
        database.set_setting("maintenance_message", body.message)
    database.log_admin_action(
        "maintenance_enabled" if body.enabled else "maintenance_disabled", body.message or ""
    )
    return {"ok": True}


# ─────────────────────────────────────────────────────────────────────────────
# Batch 2: branch spending limits, per-branch product visibility, backup
# export, and the audit-log viewer.
# ─────────────────────────────────────────────────────────────────────────────
class BranchLimitRequest(BaseModel):
    daily_limit: float | None = None
    monthly_limit: float | None = None


@app.post("/api/admin/branches/{branch_key}/limits")
def admin_set_branch_limits(branch_key: str, body: BranchLimitRequest, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    database.set_branch_limits(branch_key, body.daily_limit, body.monthly_limit)
    database.log_admin_action(
        "branch_limits_set",
        f"branch_key={branch_key} daily={body.daily_limit} monthly={body.monthly_limit}",
    )
    return {"ok": True}


@app.get("/api/admin/branches/{branch_key}/limits")
def admin_get_branch_limits(branch_key: str, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    daily, monthly = database.get_branch_limits(branch_key)
    return {"daily_limit": daily, "monthly_limit": monthly}


class ProductVisibilityRequest(BaseModel):
    product_id: int
    branch_key: str


@app.post("/api/admin/hide-product")
def admin_hide_product(body: ProductVisibilityRequest, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    database.hide_product(body.product_id, body.branch_key)
    database.log_admin_action("product_hidden", f"product_id={body.product_id} branch_key={body.branch_key}")
    return {"ok": True}


@app.post("/api/admin/unhide-product")
def admin_unhide_product(body: ProductVisibilityRequest, authorization: str | None = Header(None)):
    read_admin_token(authorization)
    database.unhide_product(body.product_id, body.branch_key)
    database.log_admin_action("product_unhidden", f"product_id={body.product_id} branch_key={body.branch_key}")
    return {"ok": True}


@app.get("/api/admin/hidden-products")
def admin_list_hidden_products(authorization: str | None = Header(None)):
    read_admin_token(authorization)
    rows = database.list_hidden_products()
    for row in rows:
        try:
            recs = swag_execute("product.product", "read", [[row["product_id"]]], {"fields": ["default_code", "name"]})
            row["product_name"] = f"{recs[0].get('default_code') or ''} {recs[0].get('name') or ''}".strip() if recs else str(row["product_id"])
        except Exception:
            row["product_name"] = str(row["product_id"])
    return {"hidden": rows}


@app.get("/api/admin/audit-log")
def admin_audit_log(limit: int = 100, authorization: str | None = Header(None)):
    """Every meaningful admin action, most recent first — full accountability trail."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        rows = (
            db.query(database.AdminAuditLog)
            .order_by(database.AdminAuditLog.created_at.desc())
            .limit(limit)
            .all()
        )
        return {
            "log": [
                {"id": r.id, "action": r.action, "details": r.details, "created_at": r.created_at.isoformat()}
                for r in rows
            ]
        }
    finally:
        db.close()


@app.get("/api/admin/export")
def admin_export(authorization: str | None = Header(None)):
    """Full backup — every order, branch mapping, and pending-approval
    record this portal knows about, as one downloadable JSON payload."""
    read_admin_token(authorization)
    db = database.get_session()
    try:
        orders = db.query(database.OrderRecord).order_by(database.OrderRecord.created_at.desc()).all()
        pending = db.query(database.PendingOrder).order_by(database.PendingOrder.created_at.desc()).all()
        logins = db.query(database.LoginEvent).order_by(database.LoginEvent.created_at.desc()).limit(500).all()
    finally:
        db.close()

    return {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "orders": [
            {
                "swag_order_name": o.swag_order_name, "branch_name": o.branch_name,
                "employee_name": o.employee_name, "warehouse_name": o.warehouse_name,
                "item_count": o.item_count, "amount_total": o.amount_total,
                "items": json.loads(o.items_json or "[]"), "created_at": o.created_at.isoformat(),
            }
            for o in orders
        ],
        "pending_orders": [
            {
                "branch_name": p.branch_name, "requested_by": p.requested_by, "status": p.status,
                "amount_total": p.amount_total, "created_at": p.created_at.isoformat(),
            }
            for p in pending
        ],
        "mappings": load_branch_partner_map(),
        "logins": [
            {"branch_name": l.branch_name, "employee_name": l.employee_name, "created_at": l.created_at.isoformat()}
            for l in logins
        ],
    }
