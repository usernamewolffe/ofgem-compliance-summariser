# api/server.py

# ---------------------------------------------------------------------------
# Environment & imports
# ---------------------------------------------------------------------------
from dotenv import load_dotenv
load_dotenv()

import os
import csv
import io
import json
import re
import sqlite3
import requests
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Set, Dict, Any, Tuple
from urllib.parse import urlparse, urlencode as _urlencode
from contextlib import closing

from fastapi import (
    FastAPI,
    Request,
    Query,
    HTTPException,
    Form,
    Body,
    APIRouter,
    Path as FPath,
)
from fastapi.responses import (
    RedirectResponse,
    StreamingResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    FileResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import TemplateNotFound, ChoiceLoader, FileSystemLoader
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from passlib.context import CryptContext
from pydantic import BaseModel
from openai import OpenAI

from tools.email_utils import send_article_email

# ---------------------------------------------------------------------------
# Database connection helpers – SINGLE source of truth
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = str((BASE_DIR / "ofgem.db").resolve())

print(f"##### server.py loaded from: {__file__} #####")
print(f"##### USING DB FILE: {DB_PATH} #####")

# Legacy hook: leave this as None so hasattr(db, ...) checks are harmless.
db = None

# ---------------------------------------------------------------------------
# Constants & paths
# ---------------------------------------------------------------------------
TOPIC_TAGS = [
    "CAF/NIS",
    "Cyber",
    "Incident",
    "Consultation",
    "Guidance",
    "Enforcement",
    "Penalty",
]

PUBLIC_DIR = BASE_DIR / "public"
TEMPLATES_DIR = BASE_DIR / "summariser" / "templates"

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI()

# Main static (Tailwind etc – matches base.html)
app.mount("/static", StaticFiles(directory="web/static"), name="static")

# Optional legacy/static JSON
if PUBLIC_DIR.exists():
    app.mount("/public", StaticFiles(directory=str(PUBLIC_DIR), html=False), name="public")

# Templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.loader = ChoiceLoader(
    [
        FileSystemLoader(str(TEMPLATES_DIR)),
        FileSystemLoader(str(TEMPLATES_DIR / "account")),
    ]
)
templates.env.globals["urlencode"] = _urlencode

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_sqlite_conn() -> sqlite3.Connection:
    """
    Always open a fresh connection to the single DB this app uses.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _sql_exec(sql: str, params: tuple | None = None) -> None:
    """
    Run a write statement or a block of DDL.
    If params is None, treat sql as a script (for CREATE TABLE, etc.).
    """
    conn = _get_sqlite_conn()
    try:
        if params is None:
            conn.executescript(sql)
        else:
            conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def _sql_all(sql: str, params: Tuple = ()) -> List[dict]:
    """
    Generic 'fetchall' helper returning list[dict].
    """
    if hasattr(db, "all"):
        return db.all(sql, params)  # type: ignore[attr-defined]
    if hasattr(db, "query"):
        return db.query(sql, params)  # type: ignore[attr-defined]
    conn = _get_sqlite_conn()
    try:
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _sql_one(sql: str, params: Tuple = ()) -> Optional[dict]:
    """
    Generic 'fetchone' helper returning dict | None.
    """
    if hasattr(db, "one"):
        return db.one(sql, params)  # type: ignore[attr-defined]
    conn = _get_sqlite_conn()
    try:
        cur = conn.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _sql_all_safe(q: str, params: Tuple = ()) -> List[dict]:
    try:
        return _sql_all(q, params)
    except Exception:
        return []


def _sql_one_safe(q: str, params: Tuple = ()) -> Optional[dict]:
    try:
        return _sql_one(q, params)
    except Exception:
        return None


def _sql_many(*args, **kwargs):
    # Simple alias; helps keep call sites readable
    return _sql_all(*args, **kwargs)


# Convenience helpers that replace old DB methods
def _list_orgs() -> List[dict]:
    return _sql_all("SELECT id, name FROM orgs ORDER BY name COLLATE NOCASE")


def _list_sites_for_org(org_id: int) -> List[dict]:
    return _sql_all(
        """
        SELECT id, org_id, name, code, location
        FROM sites
        WHERE org_id = ?
        ORDER BY name
        """,
        (org_id,),
    )


def _list_items(limit: int) -> List[dict]:
    return _sql_all(
        """
        SELECT *
        FROM items
        ORDER BY published_at DESC
        LIMIT ?
        """,
        (limit,),
    )


def _set_sites_for_risk(risk_id: int, site_ids: List[int]) -> None:
    """
    Replace all site mappings for a given org_risk_id with the provided site_ids.
    """
    _sql_exec("DELETE FROM org_risk_sites WHERE org_risk_id = ?", (risk_id,))
    for sid in site_ids:
        _sql_exec(
            "INSERT INTO org_risk_sites (org_risk_id, site_id) VALUES (?, ?)",
            (risk_id, sid),
        )


# ---------------------------------------------------------------------------
# Sessions & middleware (rolling inactivity)
# ---------------------------------------------------------------------------
INACTIVITY_SECONDS = int(os.getenv("INACTIVITY_SECONDS", str(3 * 60 * 60)))  # 3h
SESSION_COOKIE = os.getenv("SESSION_COOKIE", "ofgem_session")
SESSION_MAX_AGE = int(os.getenv("SESSION_MAX_AGE", str(3 * 60 * 60)))  # 3h


class RollingSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        now = datetime.now(timezone.utc)

        def is_api(req: Request) -> bool:
            accept = (req.headers.get("accept") or "").lower()
            return req.url.path.startswith("/api/") or "application/json" in accept

        if "session" in request.scope and request.session.get("uid"):
            last = request.session.get("last_activity")
            try:
                last_dt = datetime.fromisoformat(last) if last else now
            except Exception:
                last_dt = now

            if (now - last_dt) > timedelta(seconds=INACTIVITY_SECONDS):
                request.session.clear()
                if is_api(request):
                    return JSONResponse({"detail": "Session expired"}, status_code=401)
                return RedirectResponse("/account/login", status_code=303)

            request.session["last_activity"] = now.isoformat()

        response = await call_next(request)

        if "session" in request.scope and request.session.get("uid"):
            cookie_val = request.cookies.get(SESSION_COOKIE)
            if cookie_val:
                response.set_cookie(
                    key=SESSION_COOKIE,
                    value=cookie_val,
                    max_age=INACTIVITY_SECONDS,
                    httponly=True,
                    secure=False,  # True in production
                    samesite="lax",
                    path="/",
                )

        return response


# Apply middleware (Session outermost)
app.add_middleware(RollingSessionMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSIONS_SECRET", "dev-only-change-me"),
    session_cookie=SESSION_COOKIE,
    max_age=SESSION_MAX_AGE,
    same_site="lax",
    https_only=False,
)

# ---------------------------------------------------------------------------
# Auth helpers & tables
# ---------------------------------------------------------------------------
pwd_ctx = CryptContext(
    schemes=["pbkdf2_sha256", "bcrypt_sha256", "bcrypt"],
    default="pbkdf2_sha256",
    deprecated="auto",
)


def current_user_email(request: Request) -> str:
    # We store this in session at login
    sess_email = getattr(request, "session", {}).get("user_email")
    if sess_email:
        return sess_email
    return os.getenv("DEV_USER") or "andrewpeat@example.com"


def _get_user_by_email(email: str) -> Optional[dict]:
    return _sql_one("SELECT id, email, password_hash FROM users WHERE email = ?", (email,))


def _create_user(email: str, password_hash: str) -> int:
    _sql_exec(
        "INSERT INTO users (email, password_hash) VALUES (?, ?)",
        (email, password_hash),
    )
    row = _sql_one("SELECT id FROM users WHERE email = ?", (email,))
    if not row:
        raise RuntimeError("Failed to create user")
    return int(row["id"])


def _add_user_to_org(user_id: int, org_id: int, make_default: bool = False) -> None:
    """
    Link a user to an org. If make_default=True, set this as the default org.
    """
    org = _sql_one("SELECT id FROM orgs WHERE id = ?", (org_id,))
    if not org:
        raise HTTPException(400, f"Organisation {org_id} does not exist")

    user = _sql_one("SELECT id FROM users WHERE id = ?", (user_id,))
    if not user:
        raise HTTPException(400, f"User {user_id} does not exist")

    existing = _sql_one(
        "SELECT user_id, org_id, is_default FROM user_orgs WHERE user_id=? AND org_id=?",
        (user_id, org_id),
    )
    if not existing:
        _sql_exec(
            "INSERT INTO user_orgs (user_id, org_id, is_default) VALUES (?, ?, 0)",
            (user_id, org_id),
        )

    if make_default:
        _sql_exec("UPDATE user_orgs SET is_default=0 WHERE user_id=?", (user_id,))
        _sql_exec(
            "UPDATE user_orgs SET is_default=1 WHERE user_id=? AND org_id=?",
            (user_id, org_id),
        )


def _get_default_org_for_user(user_id: int) -> int:
    """
    Return the default org for a user.
    """
    row = _sql_one(
        "SELECT org_id FROM user_orgs WHERE user_id=? AND is_default=1",
        (user_id,),
    )
    if row and row.get("org_id") is not None:
        return int(row["org_id"])

    row = _sql_one(
        "SELECT org_id FROM user_orgs WHERE user_id=? ORDER BY org_id LIMIT 1",
        (user_id,),
    )
    if row and row.get("org_id") is not None:
        org_id = int(row["org_id"])
        _add_user_to_org(user_id, org_id, make_default=True)
        return org_id

    org = _sql_one("SELECT id FROM orgs ORDER BY id LIMIT 1")
    if not org:
        raise HTTPException(
            400,
            "No organisations exist yet. Create an organisation before assigning users.",
        )
    org_id = int(org["id"])
    _add_user_to_org(user_id, org_id, make_default=True)
    return org_id


def _list_orgs_for_user(user_id: int) -> list[dict]:
    return _sql_all(
        """
        SELECT o.id, o.name, uo.is_default
        FROM user_orgs uo
        JOIN orgs o ON o.id = uo.org_id
        WHERE uo.user_id=?
        ORDER BY o.name COLLATE NOCASE
        """,
        (user_id,),
    )


def _list_users_for_org(org_id: int) -> list[dict]:
    return _sql_all(
        """
        SELECT u.id, u.email, uo.is_default
        FROM user_orgs uo
        JOIN users u ON u.id = uo.user_id
        WHERE uo.org_id=?
        ORDER BY u.email COLLATE NOCASE
        """,
        (org_id,),
    )


def get_user_id(request: Request) -> Optional[int]:
    try:
        return request.session.get("uid")
    except Exception:
        return None


def require_user_id(request: Request) -> int:
    uid = get_user_id(request)
    if not uid:
        raise HTTPException(401, "Login required")
    return uid


def _ensure_users_tables():
    # USERS
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS users (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          email         TEXT NOT NULL UNIQUE,
          password_hash TEXT NOT NULL,
          created_at    TEXT,
          is_admin      INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    _sql_exec(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)"
    )

    # FOLDERS
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS folders (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          user_email  TEXT NOT NULL,
          name        TEXT NOT NULL,
          created_at  TEXT NOT NULL
        )
        """
    )
    _sql_exec(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_folders_user_name ON folders(user_email, name)"
    )

    # SAVED ITEMS
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS saved_items (
          user_email  TEXT NOT NULL,
          item_guid   TEXT NOT NULL,
          folder_id   INTEGER,
          note        TEXT,
          created_at  TEXT NOT NULL,
          PRIMARY KEY (user_email, item_guid),
          FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE SET NULL
        )
        """
    )
    _sql_exec(
        "CREATE INDEX IF NOT EXISTS idx_saved_items_folder ON saved_items(folder_id)"
    )

    # USER TAGS
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS user_item_tags (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          user_email      TEXT NOT NULL,
          item_guid       TEXT NOT NULL,
          org_id          INTEGER NOT NULL,
          site_id         INTEGER,
          org_control_id  INTEGER,
          created_at      TEXT NOT NULL,
          FOREIGN KEY (site_id)        REFERENCES sites(id)         ON DELETE CASCADE,
          FOREIGN KEY (org_control_id) REFERENCES org_controls(id)  ON DELETE CASCADE
        )
        """
    )
    _sql_exec(
        "CREATE INDEX IF NOT EXISTS idx_u_tags_user_item ON user_item_tags(user_email, item_guid)"
    )
    _sql_exec(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_u_tags_uniqueness
        ON user_item_tags(user_email, item_guid, IFNULL(site_id,-1), IFNULL(org_control_id,-1))
        """
    )

    # USER ↔ ORGS
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS user_orgs (
          user_id    INTEGER NOT NULL,
          org_id     INTEGER NOT NULL,
          is_default INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (user_id, org_id),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY (org_id)  REFERENCES orgs(id)  ON DELETE CASCADE
        )
        """
    )
    _sql_exec(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_user_org_default
        ON user_orgs(user_id)
        WHERE is_default = 1
        """
    )


def _ensure_org_risk_tables():
    """
    Make sure the new junction tables for org risks exist.
    Safe to run on every startup.
    """
    # Link org_risks ↔ org_controls
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS org_controls_risks (
          org_risk_id    INTEGER NOT NULL,
          org_control_id INTEGER NOT NULL,
          PRIMARY KEY (org_risk_id, org_control_id),
          FOREIGN KEY (org_risk_id)    REFERENCES org_risks(id)    ON DELETE CASCADE,
          FOREIGN KEY (org_control_id) REFERENCES org_controls(id) ON DELETE CASCADE
        )
        """
    )
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_ocr_risk ON org_controls_risks(org_risk_id)")
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_ocr_control ON org_controls_risks(org_control_id)")

    # Many-to-many link: org_risks ↔ sites
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS org_risk_sites (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          org_risk_id INTEGER NOT NULL,
          site_id     INTEGER NOT NULL,
          FOREIGN KEY (org_risk_id) REFERENCES org_risks(id) ON DELETE CASCADE,
          FOREIGN KEY (site_id)     REFERENCES sites(id)     ON DELETE CASCADE
        )
        """
    )
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_ors_risk ON org_risk_sites(org_risk_id)")
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_ors_site ON org_risk_sites(site_id)")

    # NEW: Many-to-many link: org_risks ↔ news items
    _sql_exec(
        """
        CREATE TABLE IF NOT EXISTS org_risk_items (
          org_risk_id INTEGER NOT NULL,
          item_guid   TEXT NOT NULL,
          created_at  TEXT NOT NULL DEFAULT (datetime('now')),
          PRIMARY KEY (org_risk_id, item_guid),
          FOREIGN KEY (org_risk_id) REFERENCES org_risks(id) ON DELETE CASCADE
        )
        """
    )
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_ori_risk ON org_risk_items(org_risk_id)")
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_ori_guid ON org_risk_items(item_guid)")


@app.on_event("startup")
def _startup():
    # Legacy hook – db is None, so this is effectively a no-op
    if hasattr(db, "init_auth"):
        try:
            db.init_auth()  # type: ignore[attr-defined]
        except Exception:
            pass
    _ensure_users_tables()
    _ensure_org_risk_tables()


# ---------------------------------------------------------------------------
# Org resolution helpers (for header + routes)
# ---------------------------------------------------------------------------
def _org_name_by_id(org_id: Optional[int]) -> Optional[str]:
    if org_id is None:
        return None
    row = _sql_one("SELECT name FROM orgs WHERE id = ?", (int(org_id),))
    return row["name"] if row else f"Organisation {org_id}"


def resolve_org_id(request: Request) -> int:
    """
    Priority:
    1) ?org_id=...
    2) session["org_id"]
    3) user default org
    4) env ORG_ID / DEFAULT_ORG_ID
    5) single org in DB
    """
    q = request.query_params.get("org_id")
    if q:
        try:
            oid = int(q)
            request.session["org_id"] = oid
            uid = request.session.get("uid")
            if uid:
                _add_user_to_org(int(uid), oid, make_default=True)
            return oid
        except ValueError:
            pass

    try:
        if "org_id" in request.session:
            return int(request.session["org_id"])
    except Exception:
        pass

    uid = request.session.get("uid")
    if uid:
        oid = _get_default_org_for_user(int(uid))
        request.session["org_id"] = oid
        return oid

    for key in ("ORG_ID", "DEFAULT_ORG_ID"):
        v = os.getenv(key)
        if v and v.isdigit():
            oid = int(v)
            request.session["org_id"] = oid
            return oid

    orgs = _list_orgs()
    if len(orgs) == 1:
        oid = int(orgs[0]["id"])
        request.session["org_id"] = oid
        return oid

    raise HTTPException(
        status_code=400,
        detail="No organisation selected. Ask your administrator to link your user to an organisation.",
    )


def resolve_org_id_soft(request: Request) -> int | None:
    try:
        return resolve_org_id(request)
    except HTTPException:
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Render helper
# ---------------------------------------------------------------------------
def render(request: Request, template_name: str, ctx: Optional[dict] = None):
    ctx = dict(ctx or {})
    ctx["request"] = request

    try:
        uid = request.session.get("uid")
    except Exception:
        uid = None
    ctx.setdefault("uid", uid)

    oid = resolve_org_id_soft(request)
    ctx.setdefault("org_id", oid)
    ctx.setdefault("org_name", _org_name_by_id(oid))

    try:
        return templates.TemplateResponse(template_name, ctx)
    except TemplateNotFound:
        search_dirs = getattr(templates.env.loader, "searchpath", [])
        searched = ", ".join(search_dirs) if search_dirs else "(unknown)"
        bare = Path(template_name).name
        if bare != template_name:
            try:
                return templates.TemplateResponse(bare, ctx)
            except TemplateNotFound as e2:
                return PlainTextResponse(
                    f"Template not found: '{template_name}' or '{bare}'.\n"
                    f"Searched in: {searched}\n\n{e2}",
                    status_code=500,
                )
        return PlainTextResponse(
            f"Template not found: '{template_name}'.\nSearched in: {searched}",
            status_code=500,
        )
    except Exception as e:  # pragma: no cover
        return PlainTextResponse(
            f"Template render error in '{template_name}':\n\n{e}", status_code=500
        )


# ---------------------------------------------------------------------------
# Basic routes
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return RedirectResponse(url="/summaries")


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/items")
def items(limit: int = Query(50, ge=1, le=200)):
    return _list_items(limit=limit)


@app.get("/feed.json")
def feed(limit: int = Query(5000, ge=1, le=20000)):
    return _list_items(limit=limit)


@app.get("/feed.csv")
def feed_csv(limit: int = Query(5000, ge=1, le=20000)):
    rows = _list_items(limit=limit)
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(
        ["title", "link", "published_at", "tags", "guid", "source", "summary"]
    )
    for r in rows:
        writer.writerow(
            [
                r.get("title", ""),
                r.get("link", ""),
                r.get("published_at", ""),
                r.get("tags", ""),
                r.get("guid", ""),
                r.get("source", ""),
                (
                    r.get("ai_summary")
                    or r.get("summary")
                    or (r.get("content") or "")[:220]
                ).replace("\n", " "),
            ]
        )
    out.seek(0)
    return StreamingResponse(
        io.BytesIO(out.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ofgem_feed.csv"},
    )


@app.get("/items.json")
def items_json():
    path = PUBLIC_DIR / "items.json"
    if not path.exists():
        return JSONResponse(
            {"error": "items.json not found. Run tools/export_json.py first."},
            status_code=404,
        )
    return FileResponse(path, media_type="application/json")

def tag_item_to_risk(
    user_email: str,
    item_guid: str,
    org_id: int,
    risk_id: int,
) -> None:
    """
    Idempotently tag an item to an org-level risk for a given user.
    """
    now = datetime.now(timezone.utc).isoformat()

    _sql_exec(
        """
        INSERT OR IGNORE INTO user_item_tags (
          user_email,
          item_guid,
          org_id,
          org_risk_id,
          created_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (user_email, item_guid, int(org_id), int(risk_id), now),
    )


def untag_item_from_risk(
    user_email: str,
    item_guid: str,
    org_id: int,
    risk_id: int,
) -> None:
    """
    Remove a tag linking an item to a risk for a user.
    """
    _sql_exec(
        """
        DELETE FROM user_item_tags
        WHERE user_email = ?
          AND item_guid = ?
          AND org_id = ?
          AND org_risk_id = ?
        """,
        (user_email, item_guid, int(org_id), int(risk_id)),
    )


def list_items_for_risk(org_id: int, risk_id: int, user_email: str | None = None) -> list[dict]:
    """
    Return items linked to a risk (optionally filtered by user).
    """
    params: list[Any] = [int(org_id), int(risk_id)]
    user_clause = ""
    if user_email:
        user_clause = "AND t.user_email = ?"
        params.append(user_email)

    return _sql_all(
        f"""
        SELECT
          i.guid,
          i.title,
          i.link,
          i.source,
          i.published_at,
          i.ai_summary,
          i.content,
          i.tags
        FROM user_item_tags t
        JOIN items i ON i.guid = t.item_guid
        WHERE t.org_id = ?
          AND t.org_risk_id = ?
          {user_clause}
        ORDER BY i.published_at DESC
        """,
        tuple(params),
    )

# ---------------------------------------------------------------------------
# Summaries UI
# ---------------------------------------------------------------------------
@app.get("/summaries", response_class=HTMLResponse)
def summaries_page(
    request: Request,
    q: str = "",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    sources: List[str] = Query(default=[]),
    topics: List[str] = Query(default=[]),
    page: int = 1,
    per_page: int = 25,
):
    org_id = resolve_org_id(request)
    org_name = _org_name_by_id(org_id)

    # Pull items directly from the DB, including ai_summary
    rows = _sql_many(
        """
        SELECT
            guid,
            link,
            title,
            source,
            published_at,
            ai_summary,
            content,
            tags,
            created_at
        FROM items
        ORDER BY published_at DESC
        LIMIT ?
        """,
        (20000,),
    )

    all_items: List[dict] = []
    for r in rows:
        e = dict(r)
        tags_raw = e.get("tags")
        if isinstance(tags_raw, str):
            try:
                e["tags"] = json.loads(tags_raw)
            except Exception:
                e["tags"] = []
        elif isinstance(tags_raw, list):
            e["tags"] = tags_raw
        else:
            e["tags"] = []
        all_items.append(e)

    def in_date_range(dt_str: str) -> bool:
        if not (date_from or date_to):
            return True
        try:
            dt = datetime.fromisoformat(str(dt_str).replace("Z", "+00:00"))
        except Exception:
            return True
        if date_from and dt.date() < datetime.fromisoformat(date_from).date():
            return False
        if date_to and dt.date() > datetime.fromisoformat(date_to).date():
            return False
        return True

    q_lower = (q or "").lower().strip()
    src_set: Set[str] = set(sources or [])
    topic_set = {t.lower() for t in (topics or [])}

    filtered: List[dict] = []
    for e in all_items:
        text = " ".join(
            [
                str(e.get("title", "")),
                str(e.get("content", "")),
                str(e.get("ai_summary", "")),
            ]
        ).lower()

        if q_lower and q_lower not in text:
            continue
        if not in_date_range(e.get("published_at")):
            continue

        tags = [t.lower() for t in (e.get("tags") or [])]
        if topic_set and not any(t in tags for t in topic_set):
            continue

        if src_set and (e.get("source") not in src_set):
            continue

        filtered.append(e)

    filtered.sort(key=lambda e: e.get("published_at", "") or "", reverse=True)

    page = max(1, int(page))
    per_page = max(1, min(200, int(per_page)))
    total = len(filtered)
    start = (page - 1) * per_page
    end = start + per_page
    page_items = filtered[start:end]
    total_pages = (total + per_page - 1) // per_page if total else 1
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    all_sources = sorted(
        {(i.get("source") or "").strip() for i in all_items if i.get("source")}
    )
    if not sources and not any([q, date_from, date_to, topics, page != 1]):
        sources = list(all_sources)

    uid = get_user_id(request)
    saved_filters: list[dict] = []  # legacy feature – currently disabled
    folders: list[dict] = []        # ditto

    # NEW: load org risks for the dropdown
    org_risks = _sql_all(
        """
        SELECT id, code, title
        FROM org_risks
        WHERE org_id = ?
        ORDER BY COALESCE(code, ''), title
        """,
        (org_id,),
    )

    return render(
        request,
        "summaries.html",
        {
            "entries": page_items,
            "page": page,
            "total_pages": total_pages,
            "page_numbers": page_numbers,
            "active": {
                "q": q,
                "date_from": date_from or "",
                "date_to": date_to or "",
                "sources": sources,
                "topics": topics,
                "per_page": per_page,
            },
            "all_sources": all_sources,
            "all_topics": TOPIC_TAGS,
            "saved_filters": saved_filters,
            "folders": folders,
            "org_id": org_id,
            "org_name": org_name,
            "org_risks": org_risks,
        },
    )

@app.get("/api/debug-openai-key")
def debug_openai_key():
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return {"has_key": False, "message": "No OPENAI_API_KEY in environment"}

    return {
        "has_key": True,
        "prefix": key[:15],
        "length": len(key),
    }


# ---------------------------------------------------------------------------
# Org selection
# ---------------------------------------------------------------------------
@app.get("/orgs/switch")
def switch_org(request: Request, org_id: int = Query(...), next: str = Query("/summaries")):
    request.session["org_id"] = int(org_id)
    return RedirectResponse(url=next, status_code=303)


@app.get("/orgs/select", response_class=HTMLResponse)
def select_org_page(request: Request):
    orgs = _list_orgs()
    return render(request, "org_select.html", {"orgs": orgs})


@app.get("/orgs/new", response_class=HTMLResponse)
def org_new_form(request: Request):
    return render(request, "org_new.html", {})


@app.post("/orgs/new")
def org_new_create(
    request: Request,
    name: str = Form(...),
    phone: str = Form(""),
    email: str = Form(""),
    head_office_address: str = Form(""),
    website: str = Form(""),
):
    if not name.strip():
        raise HTTPException(400, "Name is required")

    now = datetime.now(timezone.utc).isoformat()
    created_by = current_user_email(request)

    _sql_exec(
        """
        INSERT INTO orgs (name, phone, email, head_office_address, website, created_at, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (name.strip(), phone.strip(), email.strip(), head_office_address.strip(), website.strip(), now, created_by),
    )

    row = _sql_one(
        "SELECT id FROM orgs WHERE name = ? ORDER BY id DESC LIMIT 1",
        (name.strip(),),
    )
    new_id = int(row["id"]) if row else None
    if not new_id:
        raise HTTPException(500, "Failed to create organisation")

    request.session["org_id"] = new_id
    return RedirectResponse(url=f"/orgs/{new_id}", status_code=303)


# ---------------------------------------------------------------------------
# Organisation + sites helpers
# ---------------------------------------------------------------------------
def _org_personnel(org_id: int):
    """
    Returns (key_people, ultimate_owner) from org_members.
    """
    rows = _sql_all_safe(
        """
        SELECT
          id,
          name,
          COALESCE(role, '')   AS role,
          COALESCE(email, '')  AS email,
          COALESCE(is_key_personnel, 0)       AS is_key_personnel,
          COALESCE(is_ultimate_risk_owner, 0) AS is_ultimate_risk_owner
        FROM org_members
        WHERE org_id = ?
        ORDER BY
          is_ultimate_risk_owner DESC,
          is_key_personnel       DESC,
          LOWER(name)
        """,
        (org_id,),
    )

    ultimate_owner = next(
        (r for r in rows if int(r.get("is_ultimate_risk_owner", 0)) == 1),
        None,
    )

    key_people = [
        r for r in rows
        if int(r.get("is_key_personnel", 0)) == 1 or r is ultimate_owner
    ]

    return key_people, ultimate_owner


def _org_basic(org_id: int):
    row = _sql_one_safe(
        """
        SELECT id, name,
               COALESCE(phone,'') AS phone,
               COALESCE(email,'') AS email,
               COALESCE(head_office_address,'') AS head_office_address,
               COALESCE(website,'') AS website
        FROM orgs WHERE id = ?
        """,
        (org_id,),
    )
    return row or {
        "id": org_id,
        "name": f"Organisation {org_id}",
        "phone": "",
        "email": "",
        "head_office_address": "",
        "website": "",
    }


def _org_sites(org_id: int) -> list[dict]:
    return _sql_all(
        "SELECT id, org_id, name FROM sites WHERE org_id = ? ORDER BY name",
        (org_id,),
    )


def _org_counts(org_id: int):
    c_org = _sql_one_safe(
        "SELECT COUNT(*) AS n FROM org_controls WHERE org_id = ?",
        (org_id,),
    ) or {"n": 0}

    r_org = _sql_one_safe(
        "SELECT COUNT(*) AS n FROM org_risks WHERE org_id = ?",
        (org_id,),
    ) or {"n": 0}
    n_org_risks = r_org["n"]

    c_site = _sql_one_safe(
        """
        SELECT COUNT(*) AS n
        FROM site_controls sc
        JOIN sites s ON s.id = sc.site_id
        WHERE s.org_id = ?
        """,
        (org_id,),
    ) or {"n": 0}

    r_site = _sql_one_safe(
        """
        SELECT COUNT(*) AS n
        FROM site_risks sr
        JOIN sites s ON s.id = sr.site_id
        WHERE s.org_id = ?
        """,
        (org_id,),
    ) or {"n": 0}

    return {
        "org_controls": c_org["n"],
        "org_risks": n_org_risks,
        "site_controls": c_site["n"],
        "site_risks": r_site["n"],
    }


def _site_columns():
    rows = _sql_all("PRAGMA table_info(sites)")
    return {r["name"] for r in rows} if rows else set()


def _site_basic(site_id: int):
    return _sql_one_safe(
        """
        SELECT s.id, s.org_id, s.name,
               COALESCE(s.address,'') AS address,
               COALESCE(s.phone,'') AS phone,
               COALESCE(s.email,'') AS email
        FROM sites s WHERE s.id = ?
        """,
        (site_id,),
    ) or {
        "id": site_id,
        "org_id": None,
        "name": f"Site {site_id}",
        "address": "",
        "phone": "",
        "email": "",
    }


def _site_personnel(site_id: int):
    return _sql_all_safe(
        """
        SELECT id, name, COALESCE(role,'') AS role, COALESCE(email,'') AS email,
               COALESCE(is_key_personnel,0) AS is_key_personnel
        FROM site_members
        WHERE site_id = ?
        ORDER BY is_key_personnel DESC, LOWER(name)
        """,
        (site_id,),
    )


def _site_counts(site_id: int):
    c = _sql_one_safe(
        "SELECT COUNT(*) AS n FROM site_controls WHERE site_id = ?", (site_id,)
    ) or {"n": 0}
    r = _sql_one_safe(
        "SELECT COUNT(*) AS n FROM site_risks WHERE site_id = ?", (site_id,)
    ) or {"n": 0}
    return {"controls": c["n"], "risks": r["n"]}


def _current_user_display(request: Request) -> str:
    return current_user_email(request) or os.getenv("DEV_USER") or "andrewpeat"


def _get_org_or_404(org_id: int) -> dict:
    """
    Fetch an organisation by id or raise 404 if it doesn't exist.
    """
    row = _sql_one("SELECT * FROM orgs WHERE id = ?", (org_id,))
    if not row:
        raise HTTPException(status_code=404, detail="Organisation not found")
    return row


# ---------------------------------------------------------------------------
# Organisation & Sites pages
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}", response_class=HTMLResponse)
def org_overview_page(request: Request, org_id: int):
    request.session["org_id"] = int(org_id)

    org = _org_basic(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organisation not found")

    key_people, ultimate_owner = _org_personnel(org_id)

    sites = _org_sites(org_id)
    counts = _org_counts(org_id)

    return render(
        request,
        "org_overview.html",
        {
            "org": org,
            "org_id": org_id,
            "sites": sites,
            "counts": counts,
            "key_people": key_people,
            "ultimate_owner": ultimate_owner,
        },
    )


# ---------------------------------------------------------------------------
# Org members (people)
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}/members", response_class=HTMLResponse)
def org_members_page(request: Request, org_id: int):
    """
    List all people for this organisation.
    """
    org = _org_basic(org_id)

    members = _sql_all(
        """
        SELECT id, org_id, name, role, email, phone, notes, is_key_person
        FROM org_members
        WHERE org_id = ?
        ORDER BY is_key_person DESC, name COLLATE NOCASE
        """,
        (org_id,),
    )

    return templates.TemplateResponse(
        "org_members.html",
        {
            "request": request,
            "org": org,
            "org_id": org_id,
            "members": members,
        },
    )


@app.get("/orgs/{org_id}/members/new", response_class=HTMLResponse)
def org_member_new_form(request: Request, org_id: int):
    """
    Show the 'add person' form.
    """
    org = _org_basic(org_id)

    member = {
        "id": None,
        "name": "",
        "role": "",
        "email": "",
        "phone": "",
        "notes": "",
        "is_key_person": 1,  # default to key person
    }

    return templates.TemplateResponse(
        "org_member_form.html",
        {
            "request": request,
            "org": org,
            "org_id": org_id,
            "member": member,
            "mode": "create",
            "form_action": f"/orgs/{org_id}/members/new",
        },
    )


@app.post("/orgs/{org_id}/members/new")
def org_member_create(
    org_id: int,
    name: str = Form(""),
    role: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    notes: str = Form(""),
    is_key_person: str | None = Form(None),
):
    """
    Handle 'add person' submission.
    """
    if not name.strip():
        raise HTTPException(status_code=400, detail="Name is required")

    is_key = 1 if is_key_person else 0

    _sql_exec(
        """
        INSERT INTO org_members (org_id, name, role, email, phone, notes, is_key_person)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (org_id, name.strip(), role.strip(), email.strip(), phone.strip(), notes.strip(), is_key),
    )

    return RedirectResponse(
        url=f"/orgs/{org_id}/members",
        status_code=303,
    )


@app.get("/orgs/{org_id}/members/{member_id}/edit", response_class=HTMLResponse)
def org_member_edit_form(request: Request, org_id: int, member_id: int):
    """
    Show the 'edit person' form.
    """
    org = _org_basic(org_id)

    member = _sql_one(
        """
        SELECT id, org_id, name, role, email, phone, notes, is_key_person
        FROM org_members
        WHERE org_id = ? AND id = ?
        """,
        (org_id, member_id),
    )
    if not member:
        raise HTTPException(status_code=404, detail="Person not found")

    return templates.TemplateResponse(
        "org_member_form.html",
        {
            "request": request,
            "org": org,
            "org_id": org_id,
            "member": member,
            "mode": "edit",
            "form_action": f"/orgs/{org_id}/members/{member_id}/edit",
        },
    )


@app.post("/orgs/{org_id}/members/{member_id}/edit")
def org_member_update(
    org_id: int,
    member_id: int,
    name: str = Form(""),
    role: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    notes: str = Form(""),
    is_key_person: str | None = Form(None),
):
    """
    Handle 'edit person' submission.
    """
    member = _sql_one(
        "SELECT * FROM org_members WHERE org_id = ? AND id = ?",
        (org_id, member_id),
    )
    if not member:
        raise HTTPException(status_code=404, detail="Person not found")

    is_key = 1 if is_key_person else 0

    _sql_exec(
        """
        UPDATE org_members
        SET name = ?, role = ?, email = ?, phone = ?, notes = ?, is_key_person = ?
        WHERE org_id = ? AND id = ?
        """,
        (
            name.strip(),
            role.strip(),
            email.strip(),
            phone.strip(),
            notes.strip(),
            is_key,
            org_id,
            member_id,
        ),
    )

    return RedirectResponse(
        url=f"/orgs/{org_id}/members",
        status_code=303,
    )


@app.post("/orgs/{org_id}/members/{member_id}/delete")
def org_member_delete(org_id: int, member_id: int):
    """
    Delete a person.
    """
    _sql_exec(
        "DELETE FROM org_members WHERE org_id = ? AND id = ?",
        (org_id, member_id),
    )

    return RedirectResponse(
        url=f"/orgs/{org_id}/members",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Sites
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}/sites")
def list_sites_page(request: Request, org_id: int):
    """
    Organisation sites page — lists all sites for an org.
    """
    org = _sql_one("SELECT * FROM orgs WHERE id = ?", (org_id,))

    sites = _list_sites_for_org(org_id)

    return templates.TemplateResponse(
        "org_sites.html",
        {
            "request": request,
            "org": org,
            "org_id": org_id,
            "sites": sites,
        },
    )


@app.get("/orgs/{org_id}/sites/new", response_class=HTMLResponse)
def new_site_form(request: Request, org_id: int):
    return render(
        request,
        "site_new.html",
        {"org_id": org_id},
    )


@app.post("/orgs/{org_id}/sites/new")
def create_site(
    request: Request,
    org_id: int,
    name: str = Form(...),
    code: Optional[str] = Form(None),
    location: Optional[str] = Form(None),
):
    if not name.strip():
        return JSONResponse({"detail": "Name is required"}, status_code=400)

    cols = _site_columns()
    fields = ["org_id", "name"]
    params: list[Any] = [org_id, name.strip()]

    if "code" in cols:
        fields.append("code")
        params.append((code or "").strip())
    if "location" in cols:
        fields.append("location")
        params.append((location or "").strip())
    if "created_at" in cols:
        fields.append("created_at")
        params.append(datetime.now(timezone.utc).isoformat())
    if "updated_at" in cols:
        fields.append("updated_at")
        params.append(datetime.now(timezone.utc).isoformat())

    placeholders = ",".join(["?"] * len(fields))
    _sql_exec(
        f"INSERT INTO sites ({','.join(fields)}) VALUES ({placeholders})",
        tuple(params),
    )

    return RedirectResponse(url=f"/orgs/{org_id}/controls", status_code=303)


@app.get("/orgs/{org_id}/sites/{site_id}", response_class=HTMLResponse)
def site_overview_page(
    request: Request,
    org_id: int = FPath(...),
    site_id: int = FPath(...),
):
    site = _site_basic(site_id)
    if site and site.get("org_id") not in (None, org_id):
        raise HTTPException(404, "Site not in organisation")
    people = _site_personnel(site_id)
    counts = _site_counts(site_id)
    org = _org_basic(org_id)
    return render(
        request,
        "site_overview.html",
        {
            "org": org,
            "site": site,
            "people": people,
            "counts": counts,
            "org_id": org_id,
            "site_id": site_id,
        },
    )


@app.get("/orgs/{org_id}/sites/{site_id}/edit", response_class=HTMLResponse)
def site_edit_form(request: Request, org_id: int, site_id: int):
    site = _sql_one("SELECT * FROM sites WHERE id=? AND org_id=?", (site_id, org_id))
    if not site:
        raise HTTPException(404, "Site not found")
    org = _org_basic(org_id)
    cols = _site_columns()
    field_flags = {
        "code": "code" in cols,
        "city": "city" in cols,
        "address": "address" in cols,
        "phone": "phone" in cols,
        "email": "email" in cols,
    }
    return render(
        request,
        "site_edit.html",
        {
            "org": org,
            "site": site,
            "org_id": org_id,
            "site_id": site_id,
            "fields": field_flags,
        },
    )


@app.post("/orgs/{org_id}/sites/{site_id}/edit")
def site_edit_save(
    request: Request,
    org_id: int,
    site_id: int,
    name: Optional[str] = Form(None),
    code: Optional[str] = Form(None),
    city: Optional[str] = Form(None),
    address: Optional[str] = Form(None),
    phone: Optional[str] = Form(None),
    email: Optional[str] = Form(None),
):
    site = _sql_one("SELECT id FROM sites WHERE id=? AND org_id=?", (site_id, org_id))
    if not site:
        raise HTTPException(404, "Site not found")

    cols = _site_columns()
    updates: Dict[str, Any] = {}
    if name is not None:
        updates["name"] = name
    if code is not None:
        updates["code"] = code
    if city is not None:
        updates["city"] = city
    if address is not None:
        updates["address"] = address
    if phone is not None:
        updates["phone"] = phone
    if email is not None:
        updates["email"] = email

    updates = {k: v for k, v in updates.items() if k in cols}

    if updates:
        set_sql = ", ".join([f"{k}=?" for k in updates.keys()])
        params = list(updates.values()) + [site_id]
        _sql_exec(f"UPDATE sites SET {set_sql} WHERE id=?", tuple(params))

    if "updated_at" in cols:
        _sql_exec(
            "UPDATE sites SET updated_at=datetime('now') WHERE id=?", (site_id,)
        )

    return RedirectResponse(url=f"/orgs/{org_id}/sites/{site_id}", status_code=302)


# ---------------------------------------------------------------------------
# OpenAI client + helpers
# ---------------------------------------------------------------------------
def _openai_client():
    """
    Returns an OpenAI client or None if no key is configured.
    Uses whatever is in OPENAI_API_KEY (project keys are fine).
    """
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        print("[AI] ⚠️ No OPENAI_API_KEY found in environment")
        return None

    print(f"[AI] ✅ OPENAI_API_KEY detected, prefix={key[:10]}, length={len(key)}")
    return OpenAI(api_key=key)


def _extract_text_from_response(resp):
    """
    Helper for Responses API objects:
    flattens all text segments into a single string.
    """
    parts = []
    for out in resp.output:
        for item in out.content:
            if item.type == "output_text":
                parts.append(item.text)
    return "".join(parts).strip()


def _ensure_ai_summary_for_item(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Given a DB row from 'items', populate row['ai_summary'] if missing
    using the OpenAI client, and persist it to the DB.
    NOTE: relies on _generate_ai_summary being defined elsewhere.
    """
    if row.get("ai_summary"):
        return row

    client = _openai_client()
    if not client:
        print("[AI] ⚠️ Skipping AI summary – no client")
        return row

    title = row.get("title") or ""
    url = row.get("guid_or_link") or row.get("link") or ""
    body = row.get("content") or ""

    # _generate_ai_summary should be defined elsewhere in your codebase.
    summary = _generate_ai_summary(  # type: ignore[name-defined]
        client,
        title=title,
        url=url,
        body=body,
    )

    if not summary:
        return row

    _sql_exec(
        "UPDATE items SET ai_summary = ? WHERE id = ?",
        (summary, row["id"]),
    )
    row["ai_summary"] = summary
    return row


@app.get("/api/test-openai")
def api_test_openai():
    """
    Simple health-check endpoint to verify OpenAI is reachable and the key works.
    """
    client = _openai_client()
    if not client:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "No OPENAI_API_KEY configured"},
        )

    try:
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Reply with the single word: pong"},
            ],
            max_output_tokens=10,
        )
        text = _extract_text_from_response(resp)
        return JSONResponse(
            status_code=200,
            content={"ok": True, "response": text},
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": str(e)},
        )

@app.post("/api/orgs/{org_id}/org-risks/{risk_id}/tag-item")
async def api_tag_item_to_risk(
    request: Request,
    org_id: int,
    risk_id: int,
    guid: str = Form(...),
):
    """
    Tag a single item (by guid) to an org-level risk.
    Called from the summaries UI (e.g. right-hand drawer or action menu).
    """
    user_email = current_user_email(request)
    if not guid.strip():
        return JSONResponse({"ok": False, "error": "Missing guid"}, status_code=400)

    tag_item_to_risk(user_email, guid.strip(), org_id, risk_id)
    return JSONResponse({"ok": True})


@app.post("/api/orgs/{org_id}/org-risks/{risk_id}/untag-item")
async def api_untag_item_from_risk(
    request: Request,
    org_id: int,
    risk_id: int,
    guid: str = Form(...),
):
    """
    Remove a link between an item and a risk.
    """
    user_email = current_user_email(request)
    if not guid.strip():
        return JSONResponse({"ok": False, "error": "Missing guid"}, status_code=400)

    untag_item_from_risk(user_email, guid.strip(), org_id, risk_id)
    return JSONResponse({"ok": True})

# ---------------------------------------------------------------------------
# Org controls
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}/controls")
def org_controls_page(
    request: Request,
    org_id: int,
    site: Optional[int] = Query(None),
):
    """
    Organisation controls page — shows controls grouped by site, plus sidebar of sites.
    """
    org = _sql_one("SELECT * FROM orgs WHERE id = ?", (org_id,))

    sites = _sql_all(
        "SELECT id, name FROM sites WHERE org_id = ? ORDER BY name",
        (org_id,),
    )

    where = "c.org_id = ?"
    params: list[Any] = [org_id]

    if site:
        where += " AND c.site_id = ?"
        params.append(site)

    rows = _sql_all(
        f"""
        SELECT
            c.*,
            s.name AS site_name
        FROM org_controls AS c
        LEFT JOIN sites AS s
          ON s.id = c.site_id
        WHERE {where}
        ORDER BY
          COALESCE(s.name, 'Corporate'),
          COALESCE(c.code, ''),
          c.title
        """,
        tuple(params),
    )

    for r in rows:
        tags = r.get("tags")
        if isinstance(tags, str) and tags.strip():
            r["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
        else:
            r["tags"] = []

    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        site_name = r.get("site_name") or "Corporate"
        grouped.setdefault(site_name, []).append(r)

    return render(
        request,
        "org_controls.html",
        {
            "org": org,
            "org_id": org_id,
            "sites": sites,
            "grouped": grouped,
        },
    )


@app.post("/orgs/{org_id}/controls/{control_id}/update")
async def update_control(
    org_id: int,
    control_id: int,
    site_id: Optional[int] = Form(None),
    code: str = Form(""),
    title: str = Form(...),
    description: str = Form(""),
    owner_email: str = Form(""),
    tags: str = Form(""),
    status: str = Form("Active"),
    risk: str = Form(""),
    review_frequency_days: Optional[int] = Form(None),
    next_review_at: str = Form(""),
):
    conn = _get_sqlite_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE org_controls
            SET site_id = ?, code = ?, title = ?, description = ?,
                owner_email = ?, tags = ?, status = ?, risk = ?,
                review_frequency_days = ?, next_review_at = ?,
                updated_at = datetime('now')
            WHERE id = ? AND org_id = ?
            """,
            (
                site_id,
                code,
                title,
                description,
                owner_email,
                tags,
                status,
                risk,
                review_frequency_days,
                next_review_at or None,
                control_id,
                org_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    return RedirectResponse(f"/orgs/{org_id}/controls", status_code=303)


@app.post("/orgs/{org_id}/controls/{control_id}/delete")
def delete_control(org_id: int, control_id: int):
    conn = _get_sqlite_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM org_controls WHERE id = ? AND org_id = ?",
            (control_id, org_id),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse(f"/orgs/{org_id}/controls", status_code=303)


@app.get("/orgs/{org_id}/controls/new", response_class=HTMLResponse)
def org_control_new_page(request: Request, org_id: int, site_id: Optional[int] = None):
    return render(
        request,
        "org_control_new.html",
        {
            "org_id": org_id,
            "site_id": site_id,
        },
    )


@app.post("/orgs/{org_id}/controls/new")
def org_control_create(
    request: Request,
    org_id: int,
    title: str = Form(...),
    code: str = Form(""),
    description: str = Form(""),
    owner_email: str = Form(""),
    tags: str = Form(""),
    status: str = Form("Active"),
    risk: str = Form(""),
    review_frequency_days: Optional[str] = Form(None),
    next_review_at: Optional[str] = Form(None),
    site_id: Optional[int] = Form(None),
):
    if not title.strip():
        return JSONResponse({"detail": "Title required"}, status_code=400)

    rfd: Optional[int] = None
    if review_frequency_days:
        try:
            rfd = int(review_frequency_days)
        except ValueError:
            rfd = None

    user = _current_user_display(request)
    now = datetime.now(timezone.utc).isoformat()

    _sql_exec(
        """
        INSERT INTO org_controls
          (org_id, site_id, code, title, description, owner_email,
           tags, status, risk, review_frequency_days, next_review_at,
           created_at, updated_at, created_by)
        VALUES
          (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            org_id,
            site_id,
            code or None,
            title.strip(),
            description.strip() or None,
            owner_email.strip() or None,
            tags.strip() or None,
            status or "Active",
            risk.strip() or None,
            rfd,
            next_review_at or None,
            now,
            now,
            user,
        ),
    )

    return RedirectResponse(url=f"/orgs/{org_id}/controls", status_code=303)


# ---------------------------------------------------------------------------
# Org & site risks pages
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}/org-risks", response_class=HTMLResponse)
def org_risks_page(
    request: Request,
    org_id: int,
    status: str | None = Query(None),
    severity: str | None = Query(None),
    category: str | None = Query(None),
    location: str | None = Query(None),  # "", "corp" or site_id as string
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
):
    """
    List all risks for an org – both corporate and site-specific – using
    org_risks + org_risk_sites (many-to-many).
    """
    org = _org_basic(org_id)
    sites = _list_sites_for_org(org_id)
    total_sites = len(sites)

    loc = (location or "").strip()
    loc_is_corp = (loc == "corp")
    loc_site_id: int | None = None
    if loc and not loc_is_corp:
        try:
            loc_site_id = int(loc)
        except ValueError:
            loc_site_id = None

    risk_rows = _sql_all(
        """
        SELECT id, org_id, code, title, description,
               status, severity, category,
               owner_name, owner_email,
               created_at, updated_at
        FROM org_risks
        WHERE org_id = ?
        """,
        (org_id,),
    )

    mapping_rows = _sql_all(
        """
        SELECT ors.org_risk_id, ors.site_id, s.name AS site_name
        FROM org_risk_sites ors
        JOIN sites s ON s.id = ors.site_id
        WHERE s.org_id = ?
        """,
        (org_id,),
    )

    risk_sites: dict[int, list[dict]] = {}
    for row in mapping_rows:
        rid = row["org_risk_id"]
        risk_sites.setdefault(rid, []).append(
            {"site_id": row["site_id"], "site_name": row["site_name"]}
        )

    control_counts_rows = _sql_all(
        """
        SELECT org_risk_id, COUNT(*) AS n
        FROM org_controls_risks
        GROUP BY org_risk_id
        """
    )
    control_counts = {row["org_risk_id"]: row["n"] for row in control_counts_rows}

    all_risks: list[dict] = []

    for r in risk_rows:
        rid = r["id"]
        site_links = risk_sites.get(rid, [])

        is_corporate = (
            (total_sites > 0 and len(site_links) == total_sites)
            or (total_sites == 0 and not site_links)
        )

        if status and (r.get("status") or "").lower() != status.lower():
            continue
        if severity and (r.get("severity") or "").lower() != severity.lower():
            continue
        if category and (r.get("category") or "").lower() != category.lower():
            continue

        if loc_is_corp:
            if not is_corporate:
                continue
        elif loc_site_id is not None:
            if not (
                is_corporate
                or any(sl["site_id"] == loc_site_id for sl in site_links)
            ):
                continue

        if is_corporate:
            location_label = "Corporate (all sites)" if total_sites else "Corporate"
            location_kind = "corp"
            primary_site_id = None
        elif not site_links:
            location_label = "Unmapped"
            location_kind = "unmapped"
            primary_site_id = None
        elif len(site_links) == 1:
            location_label = site_links[0]["site_name"] or "Site"
            location_kind = "site"
            primary_site_id = site_links[0]["site_id"]
        else:
            location_label = f"{len(site_links)} sites"
            location_kind = "multi"
            primary_site_id = None

        controls_count = control_counts.get(rid, 0)

        all_risks.append(
            {
                "id": rid,
                "code": r.get("code"),
                "title": r.get("title"),
                "status": r.get("status"),
                "severity": r.get("severity"),
                "category": r.get("category"),
                "owner_name": r.get("owner_name"),
                "owner_email": r.get("owner_email"),
                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
                "description": r.get("description"),
                "controls_count": controls_count,
                "location_label": location_label,
                "location_kind": location_kind,
                "site_id": primary_site_id,
                "source": "org",
            }
        )

    def _dt_key(rec: dict) -> str:
        return (rec.get("updated_at") or rec.get("created_at") or "") or ""

    all_risks.sort(key=_dt_key, reverse=True)

    total = len(all_risks)
    page = max(1, int(page))
    per_page = max(1, min(200, int(per_page)))
    start = (page - 1) * per_page
    end = start + per_page
    page_risks = all_risks[start:end]

    total_pages = max(1, (total + per_page - 1) // per_page)
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    status_choices = ["Open", "In progress", "Mitigated", "Closed"]
    severity_choices = ["Low", "Medium", "High", "Severe"]

    cats = _sql_all(
        """
        SELECT DISTINCT category
        FROM org_risks
        WHERE org_id = ?
          AND category IS NOT NULL
          AND category != ''
        ORDER BY LOWER(category)
        """,
        (org_id,),
    )
    category_choices = [c["category"] for c in cats]

    ctx = {
        "org": org,
        "org_id": org_id,
        "sites": sites,
        "risks": page_risks,
        "status_choices": status_choices,
        "severity_choices": severity_choices,
        "category_choices": category_choices,
        "active": {
            "status": status,
            "severity": severity,
            "category": category,
            "location": loc,
        },
        "page": page,
        "total_pages": total_pages,
        "page_numbers": page_numbers,
    }

    return render(request, "org_risks.html", ctx)


@app.get("/orgs/{org_id}/org-risks/{risk_id}", response_class=HTMLResponse)
def org_risk_detail_page(request: Request, org_id: int, risk_id: int):
    """
    Full-page view of a single org-level risk, including:
      - linked org controls
      - news/items that have been mapped to this risk
    """
    org = _org_basic(org_id)

    risk = _sql_one(
        """
        SELECT *
        FROM org_risks
        WHERE org_id = ? AND id = ?
        """,
        (org_id, risk_id),
    )
    if not risk:
        raise HTTPException(status_code=404, detail="Risk not found for this organisation")

    controls = _sql_all(
        """
        SELECT id, code, title, status
        FROM org_controls
        WHERE org_id = ?
        ORDER BY COALESCE(code, ''), title
        """,
        (org_id,),
    )

    linked_rows = _sql_all(
        "SELECT org_control_id FROM org_controls_risks WHERE org_risk_id = ?",
        (risk_id,),
    )
    linked_ids = {row["org_control_id"] for row in linked_rows}

    # NEW: items linked to this risk
    linked_items = _sql_all(
        """
        SELECT
          i.guid,
          i.title,
          i.link,
          i.source,
          i.published_at,
          i.ai_summary,
          i.content
        FROM org_risk_items ori
        JOIN items i ON i.guid = ori.item_guid
        WHERE ori.org_risk_id = ?
        ORDER BY i.published_at DESC
        """,
        (risk_id,),
    )

    return render(
        request,
        "org_risk_detail.html",
        {
            "org": org,
            "org_id": org_id,
            "risk": risk,
            "controls": controls,
            "linked_ids": linked_ids,
            "linked_items": linked_items,
        },
    )

@app.get("/orgs/{org_id}/org-risks/{risk_id}/modal", response_class=HTMLResponse)
def org_risk_modal(request: Request, org_id: int, risk_id: int):
    org = _org_basic(org_id)

    risk = _sql_one(
        """
        SELECT id, org_id, code, title, description,
               status, severity, category,
               owner_name, owner_email,
               created_at, updated_at
        FROM org_risks
        WHERE org_id = ? AND id = ?
        """,
        (org_id, risk_id),
    )
    if not risk:
        raise HTTPException(
            status_code=404,
            detail=f"Risk {risk_id} not found for organisation {org_id}",
        )

    sites = _list_sites_for_org(org_id)

    site_rows = _sql_all(
        "SELECT site_id FROM org_risk_sites WHERE org_risk_id = ?",
        (risk_id,),
    )
    site_ids_for_risk = [row["site_id"] for row in site_rows]

    controls = _sql_all(
        """
        SELECT id, code, title, status
        FROM org_controls
        WHERE org_id = ?
        ORDER BY COALESCE(code, ''), title
        """,
        (org_id,),
    )

    linked_rows = _sql_all(
        "SELECT org_control_id FROM org_controls_risks WHERE org_risk_id = ?",
        (risk_id,),
    )
    linked_ids = {row["org_control_id"] for row in linked_rows}

    return templates.TemplateResponse(
        "org_risk_form.html",
        {
            "request": request,
            "org": org,
            "org_id": org_id,
            "mode": "edit",
            "risk": risk,
            "sites": sites,
            "site_ids_for_risk": site_ids_for_risk,
            "controls": controls,
            "linked_ids": linked_ids,
            "form_action": f"/orgs/{org_id}/org-risks/{risk_id}/update",
        },
    )


@app.post("/orgs/{org_id}/org-risks/{risk_id}/update")
async def org_risk_update(
    org_id: int,
    risk_id: int,
    request: Request,
):
    """
    Update an org-level risk *and* its linked controls.
    Expects:
      - standard fields: code, title, category, status, severity, owner_id, description
      - multi-select:   control_ids  (checkbox list in the modal)
    """
    form = await request.form()

    code        = (form.get("code") or "").strip()
    title       = (form.get("title") or "").strip()
    category    = (form.get("category") or "").strip()
    status      = (form.get("status") or "Open").strip()
    severity    = (form.get("severity") or "").strip()
    owner_id    = (form.get("owner_id") or "").strip()
    description = (form.get("description") or "").strip()

    raw_control_ids = form.getlist("control_ids")
    control_ids: list[int] = []
    for cid in raw_control_ids:
        try:
            control_ids.append(int(cid))
        except (TypeError, ValueError):
            continue

    now = datetime.utcnow().isoformat(timespec="seconds")

    if code == "":
        code_db = None
    else:
        existing = _sql_one(
            """
            SELECT id
            FROM org_risks
            WHERE org_id = ? AND code = ? AND id != ?
            """,
            (org_id, code, risk_id),
        )
        if existing:
            return JSONResponse(
                {
                    "ok": False,
                    "error": "Another risk for this organisation already uses that code.",
                },
                status_code=400,
            )
        code_db = code

    owner_id_db = int(owner_id) if owner_id else None

    sql = """
    UPDATE org_risks
    SET
      code        = ?,
      title       = ?,
      category    = ?,
      status      = ?,
      severity    = ?,
      owner_id    = ?,
      description = ?,
      updated_at  = ?
    WHERE id = ? AND org_id = ?
    """
    params = (
        code_db,
        title,
        category or None,
        status or None,
        severity or None,
        owner_id_db,
        description or None,
        now,
        risk_id,
        org_id,
    )
    _sql_exec(sql, params)

    _sql_exec("DELETE FROM org_controls_risks WHERE org_risk_id = ?", (risk_id,))

    for cid in control_ids:
        _sql_exec(
            """
            INSERT OR IGNORE INTO org_controls_risks (org_risk_id, org_control_id)
            VALUES (?, ?)
            """,
            (risk_id, cid),
        )

    return JSONResponse({"ok": True})


@app.post("/orgs/{org_id}/org-risks/{risk_id}/delete")
def org_risk_delete(request: Request, org_id: int, risk_id: int):
    risk = _sql_one(
        "SELECT * FROM org_risks WHERE org_id = ? AND id = ?",
        (org_id, risk_id),
    )
    if not risk:
        raise HTTPException(status_code=404, detail="Risk not found for this organisation")

    _sql_exec("DELETE FROM org_controls_risks WHERE org_risk_id = ?", (risk_id,))
    _sql_exec("DELETE FROM org_risks WHERE org_id = ? AND id = ?", (org_id, risk_id))

    accepts = (request.headers.get("accept") or "").lower()
    is_ajax = "application/json" in accepts or request.headers.get("x-requested-with") == "fetch"

    if is_ajax:
        return JSONResponse({"ok": True})

    return RedirectResponse(
        url=f"/orgs/{org_id}/org-risks",
        status_code=303,
    )

@app.post("/api/orgs/{org_id}/org-risks/{risk_id}/tag-item")
async def api_tag_item_to_risk(org_id: int, risk_id: int, request: Request):
    """
    Link a scraped item (by GUID) to an org-level risk.
    Expects form-data with:
      - guid: item GUID
    Returns JSON { ok: bool, error?: str }.
    """
    form = await request.form()
    guid = (form.get("guid") or "").strip()
    if not guid:
        return JSONResponse(
            {"ok": False, "error": "Missing GUID"},
            status_code=400,
        )

    # Ensure risk belongs to this org
    risk = _sql_one(
        "SELECT id FROM org_risks WHERE id = ? AND org_id = ?",
        (risk_id, org_id),
    )
    if not risk:
        return JSONResponse(
            {"ok": False, "error": "Risk not found for this organisation"},
            status_code=404,
        )

    # Ensure item exists
    item = _sql_one(
        "SELECT guid FROM items WHERE guid = ?",
        (guid,),
    )
    if not item:
        return JSONResponse(
            {"ok": False, "error": "Item not found"},
            status_code=404,
        )

    # Insert link (idempotent)
    _sql_exec(
        """
        INSERT OR IGNORE INTO org_risk_items (org_risk_id, item_guid)
        VALUES (?, ?)
        """,
        (risk_id, guid),
    )

    return JSONResponse({"ok": True})


@app.get("/orgs/{org_id}/sites/{site_id}/risks", response_class=HTMLResponse)
def site_risks_page(
    request: Request,
    org_id: int,
    site_id: int,
    status: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    category: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
):
    """
    List risks that apply to a single site, via org_risks + org_risk_sites.
    """
    org = _org_basic(org_id)
    site = _site_basic(site_id)
    if not site or site.get("org_id") not in (None, org_id):
        raise HTTPException(404, "Site not found in this organisation")

    filters = ["r.org_id = ?", "ors.site_id = ?"]
    params: list[Any] = [org_id, site_id]

    if status:
        filters.append("LOWER(r.status) = LOWER(?)")
        params.append(status)
    if severity:
        filters.append("LOWER(r.severity) = LOWER(?)")
        params.append(severity)
    if category:
        filters.append("LOWER(r.category) = LOWER(?)")
        params.append(category)

    where_sql = " AND ".join(filters)

    total_row = _sql_one(
        f"""
        SELECT COUNT(DISTINCT r.id) AS n
        FROM org_risks r
        JOIN org_risk_sites ors ON ors.org_risk_id = r.id
        WHERE {where_sql}
        """,
        tuple(params),
    ) or {"n": 0}
    total = total_row["n"]

    page = max(1, int(page))
    per_page = max(1, min(200, int(per_page)))
    offset = (page - 1) * per_page

    risks = _sql_all(
        f"""
        SELECT DISTINCT
            r.id, r.code, r.title, r.status, r.severity, r.category,
            r.owner_name, r.owner_email, r.created_at, r.updated_at
        FROM org_risks r
        JOIN org_risk_sites ors ON ors.org_risk_id = r.id
        WHERE {where_sql}
        ORDER BY COALESCE(r.updated_at, r.created_at) DESC
        LIMIT ? OFFSET ?
        """,
        tuple(params + [per_page, offset]),
    )

    for r in risks:
        row = _sql_one(
            "SELECT COUNT(*) AS n FROM org_controls_risks WHERE org_risk_id = ?",
            (r["id"],),
        )
        r["controls_count"] = (row or {"n": 0})["n"]

    total_pages = max(1, (total + per_page - 1) // per_page)
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    cats = _sql_all(
        """
        SELECT DISTINCT r.category AS category
        FROM org_risks r
        JOIN org_risk_sites ors ON ors.org_risk_id = r.id
        WHERE ors.site_id = ?
          AND r.category IS NOT NULL
          AND r.category != ''
        ORDER BY LOWER(r.category)
        """,
        (site_id,),
    )
    category_choices = [c["category"] for c in cats]

    status_choices = ["Open", "In progress", "Mitigated", "Closed"]
    severity_choices = ["Low", "Medium", "High", "Severe"]

    return render(
        request,
        "site_risks.html",
        {
            "org": org,
            "site": site,
            "org_id": org_id,
            "site_id": site_id,
            "risks": risks,
            "status_choices": status_choices,
            "severity_choices": severity_choices,
            "category_choices": category_choices,
            "active": {
                "status": status,
                "severity": severity,
                "category": category,
            },
            "page": page,
            "total_pages": total_pages,
            "page_numbers": page_numbers,
        },
    )


# ---------------------------------------------------------------------------
# New org risk drawer + create
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}/org-risks/new/drawer", response_class=HTMLResponse)
async def org_risk_new_drawer(request: Request, org_id: int):
    org = _org_basic(org_id)
    sites = _list_sites_for_org(org_id)

    risk = {
        "id": None,
        "code": "",
        "title": "",
        "description": "",
        "owner_name": "",
        "owner_email": "",
        "status": "Open",
        "severity": "",
        "category": "",
    }

    return templates.TemplateResponse(
        "org_risk_form.html",
        {
            "request": request,
            "org": org,
            "org_id": org_id,
            "mode": "new",
            "risk": risk,
            "sites": sites,
            "site_ids_for_risk": [],
            "controls": [],
            "linked_ids": [],
            "form_action": f"/orgs/{org_id}/org-risks/create",
        },
    )


@app.post("/orgs/{org_id}/org-risks/create")
async def org_risk_create(request: Request, org_id: int):
    form = await request.form()

    title = (form.get("title") or "").strip()
    description = (form.get("description") or "").strip()
    owner_name = (form.get("owner_name") or "").strip()
    owner_email = (form.get("owner_email") or "").strip()
    status = (form.get("status") or "Open").strip()
    severity = (form.get("severity") or "").strip()
    category = (form.get("category") or "").strip()

    raw_site_ids = form.getlist("site_ids")
    site_ids: list[int] = []
    for sid in raw_site_ids:
        try:
            site_ids.append(int(sid))
        except (TypeError, ValueError):
            continue

    all_sites = _list_sites_for_org(org_id)
    if not site_ids and all_sites:
        site_ids = [s["id"] for s in all_sites]

    now = datetime.now(timezone.utc).isoformat()

    conn = _get_sqlite_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO org_risks
              (org_id, site_id, code, title, description,
               owner_name, owner_email, status, severity, category,
               created_at, updated_at)
            VALUES
              (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(org_id),
                None,
                None,
                title,
                description,
                owner_name,
                owner_email,
                status,
                severity,
                category,
                now,
                now,
            ),
        )
        new_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    _set_sites_for_risk(new_id, site_ids)

    wants_json = request.headers.get("X-Requested-With") == "fetch" or \
                 "application/json" in (request.headers.get("Accept") or "")

    if wants_json:
        return JSONResponse({"ok": True, "id": new_id})

    return RedirectResponse(
        url=f"/orgs/{org_id}/org-risks",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# /controls (simple global list) + router
# ---------------------------------------------------------------------------
router = APIRouter()


@app.get("/controls", response_class=HTMLResponse)
def controls_page(request: Request):
    all_controls = _sql_all(
        """
        SELECT
          c.*,
          s.name AS site_name,
          o.name AS org_name
        FROM org_controls c
        LEFT JOIN sites s ON s.id = c.site_id
        LEFT JOIN orgs  o ON o.id = c.org_id
        ORDER BY o.name, s.name, c.code, c.title
        """
    )
    return render(
        request,
        "controls.html",
        {
            "controls": all_controls,
        },
    )


@router.get("/controls/{cid}", response_class=HTMLResponse)
def control_detail(request: Request, cid: int):
    control = _sql_one(
        """
        SELECT
          c.*,
          s.name AS site_name,
          o.name AS org_name
        FROM org_controls c
        LEFT JOIN sites s ON s.id = c.site_id
        LEFT JOIN orgs  o ON o.id = c.org_id
        WHERE c.id = ?
        """,
        (cid,),
    )
    if not control:
        raise HTTPException(status_code=404, detail="Control not found")

    items = _sql_all(
        """
        SELECT i.*
        FROM user_item_tags t
        JOIN items i ON i.guid = t.item_guid
        WHERE t.org_control_id = ?
        ORDER BY i.published_at DESC
        LIMIT 100
        """,
        (cid,),
    )

    return render(
        request,
        "control_detail.html",
        {"control": control, "items": items},
    )


@router.get("/orgs/{org_id}/sites/{site_id}/controls")
def site_controls_redirect(org_id: int, site_id: int):
    url = f"/orgs/{org_id}/controls?site={site_id}"
    return RedirectResponse(url, status_code=303)


@router.post("/send", response_class=HTMLResponse)
async def send_article_fragment(guid: str = Form(...), email: str = Form(...)):
    items = _sql_all(
        """
        SELECT *
        FROM items
        WHERE guid = ?
        LIMIT 1
        """,
        (guid,),
    )
    if not items:
        return HTMLResponse(
            "<p class='muted' style='color:#b00;'>❌ Article not found.</p>",
            status_code=404,
        )

    item = items[0]
    ok = send_article_email(email, item)

    if ok:
        return HTMLResponse(
            f"<p class='muted'>✅ Sent to <strong>{email}</strong></p>", status_code=200
        )
    else:
        return HTMLResponse(
            "<p class='muted' style='color:#b00;'>❌ Failed to send. Please try again.</p>",
            status_code=502,
        )


app.include_router(router)


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------
@app.get("/account/login", response_class=HTMLResponse)
def account_login_get(request: Request):
    return render(request, "account/login.html", {"error": ""})


@app.get("/account/register", response_class=HTMLResponse)
def account_register_get(request: Request):
    return render(request, "account/register.html", {"error": ""})


@app.get("/login")
def login_alias():
    return RedirectResponse("/account/login", status_code=308)


@app.get("/register")
def register_alias():
    return RedirectResponse("/account/register", status_code=308)


@app.post("/account/login")
def account_login_post(request: Request, email: str = Form(...), password: str = Form(...)):
    user = _get_user_by_email(email)
    if not user or not pwd_ctx.verify(password, user["password_hash"]):
        return render(request, "account/login.html", {"error": "Invalid credentials"})

    uid = int(user["id"])
    request.session["uid"] = uid
    request.session["last_activity"] = datetime.now(timezone.utc).isoformat()

    org_id = _get_default_org_for_user(uid)
    request.session["org_id"] = org_id

    return RedirectResponse(url="/summaries", status_code=302)


@app.post("/account/register")
def account_register_post(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    confirm: str = Form(...),
):
    if password != confirm:
        return render(request, "account/register.html", {"error": "Passwords do not match"})
    if _get_user_by_email(email):
        return render(request, "account/register.html", {"error": "Email already registered"})

    uid = _create_user(email, pwd_ctx.hash(password))
    request.session["uid"] = uid
    request.session["last_activity"] = datetime.now(timezone.utc).isoformat()

    org_id = _get_default_org_for_user(uid)
    request.session["org_id"] = org_id

    return RedirectResponse(url="/summaries", status_code=302)


@app.post("/account/logout")
def account_logout_post(request: Request):
    request.session.clear()
    return RedirectResponse(url="/account/login", status_code=302)
