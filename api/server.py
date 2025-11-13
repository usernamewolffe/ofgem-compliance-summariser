# api/server.py
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
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

from tools.email_utils import send_article_email
from storage.db import DB

# ---------------------------------------------------------------------------
# Constants & Paths
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

BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = (BASE_DIR / "ofgem.db").as_posix()
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
# DB wrapper + SQLite fallback
# ---------------------------------------------------------------------------
db = DB(os.getenv("DB_PATH", "ofgem.db"))

_sqlite_conn: Optional[sqlite3.Connection] = None


def _get_sqlite_conn() -> sqlite3.Connection:
    global _sqlite_conn
    if _sqlite_conn is None:
        _sqlite_conn = sqlite3.connect(SQLITE_DB_PATH, check_same_thread=False)
        _sqlite_conn.row_factory = sqlite3.Row
    return _sqlite_conn


def _sql_exec(sql: str, params: Tuple = ()) -> None:
    if hasattr(db, "exec"):
        db.exec(sql, params)  # type: ignore[attr-defined]
        return
    if hasattr(db, "execute"):
        db.execute(sql, params)  # type: ignore[attr-defined]
        return
    conn = _get_sqlite_conn()
    with conn:
        conn.execute(sql, params)


def _sql_all(sql: str, params: Tuple = ()) -> List[dict]:
    if hasattr(db, "all"):
        return db.all(sql, params)  # type: ignore[attr-defined]
    if hasattr(db, "query"):
        return db.query(sql, params)  # type: ignore[attr-defined]
    conn = _get_sqlite_conn()
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    return [dict(r) for r in rows]


def _sql_one(sql: str, params: Tuple = ()) -> Optional[dict]:
    if hasattr(db, "one"):
        return db.one(sql, params)  # type: ignore[attr-defined]
    conn = _get_sqlite_conn()
    cur = conn.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else None


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
    This is the building block you can use from either:
      - a “assign orgs to user” screen
      - an “add user to org” screen
    """
    # ensure org exists
    org = _sql_one("SELECT id FROM orgs WHERE id = ?", (org_id,))
    if not org:
        raise HTTPException(400, f"Organisation {org_id} does not exist")

    # ensure user exists
    user = _sql_one("SELECT id FROM users WHERE id = ?", (user_id,))
    if not user:
        raise HTTPException(400, f"User {user_id} does not exist")

    # create membership if missing
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
        # clear previous default for this user
        _sql_exec("UPDATE user_orgs SET is_default=0 WHERE user_id=?", (user_id,))
        _sql_exec(
            "UPDATE user_orgs SET is_default=1 WHERE user_id=? AND org_id=?",
            (user_id, org_id),
        )


def _get_default_org_for_user(user_id: int) -> int:
    """
    Return the default org for a user.
    If none is marked default but memberships exist, pick the first and mark as default.
    If user has no orgs, auto-assign them to the first org in the system.
    """
    # 1) explicit default?
    row = _sql_one(
        "SELECT org_id FROM user_orgs WHERE user_id=? AND is_default=1",
        (user_id,),
    )
    if row and row.get("org_id") is not None:
        return int(row["org_id"])

    # 2) some membership exists – pick first and mark it default
    row = _sql_one(
        "SELECT org_id FROM user_orgs WHERE user_id=? ORDER BY org_id LIMIT 1",
        (user_id,),
    )
    if row and row.get("org_id") is not None:
        org_id = int(row["org_id"])
        _add_user_to_org(user_id, org_id, make_default=True)
        return org_id

    # 3) no membership yet – attach them to the first org in DB
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
    """
    Organisations this user belongs to.
    """
    return _sql_all("""
        SELECT o.id, o.name, uo.is_default
        FROM user_orgs uo
        JOIN orgs o ON o.id = uo.org_id
        WHERE uo.user_id=?
        ORDER BY o.name COLLATE NOCASE
    """, (user_id,))


def _list_users_for_org(org_id: int) -> list[dict]:
    """
    Users belonging to a given organisation.
    Nice for an “organisation members” admin screen.
    """
    return _sql_all("""
        SELECT u.id, u.email, uo.is_default
        FROM user_orgs uo
        JOIN users u ON u.id = uo.user_id
        WHERE uo.org_id=?
        ORDER BY u.email COLLATE NOCASE
    """, (org_id,))


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
    # FOLDERS (user-private)
    _sql_exec("""
        CREATE TABLE IF NOT EXISTS folders (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          user_email  TEXT NOT NULL,
          name        TEXT NOT NULL,
          created_at  TEXT NOT NULL
        )
    """)
    _sql_exec("CREATE UNIQUE INDEX IF NOT EXISTS idx_folders_user_name ON folders(user_email, name)")

    # SAVED ITEMS (bookmarks per user)
    _sql_exec("""
        CREATE TABLE IF NOT EXISTS saved_items (
          user_email  TEXT NOT NULL,
          item_guid   TEXT NOT NULL,
          folder_id   INTEGER,
          note        TEXT,
          created_at  TEXT NOT NULL,
          PRIMARY KEY (user_email, item_guid),
          FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE SET NULL
        )
    """)
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_saved_items_folder ON saved_items(folder_id)")

    # USER TAGS (private associations items → sites/controls)
    _sql_exec("""
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
    """)
    _sql_exec("CREATE INDEX IF NOT EXISTS idx_u_tags_user_item ON user_item_tags(user_email, item_guid)")
    _sql_exec("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_u_tags_uniqueness
        ON user_item_tags(user_email, item_guid, IFNULL(site_id,-1), IFNULL(org_control_id,-1))
    """)

    # 👇 NEW: many-to-many between users and orgs (+ one default per user)
    _sql_exec("""
        CREATE TABLE IF NOT EXISTS user_orgs (
          user_id    INTEGER NOT NULL,
          org_id     INTEGER NOT NULL,
          is_default INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (user_id, org_id),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY (org_id)  REFERENCES orgs(id)  ON DELETE CASCADE
        )
    """)
    _sql_exec("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_user_org_default
        ON user_orgs(user_id)
        WHERE is_default = 1
    """)



@app.on_event("startup")
def _startup():
    if hasattr(db, "init_auth"):
        try:
            db.init_auth()  # type: ignore[attr-defined]
        except Exception:
            pass
    _ensure_users_tables()


# ---------------------------------------------------------------------------
# Org resolution helpers (for header + routes)
# ---------------------------------------------------------------------------
def resolve_org_id(request: Request) -> int:
    """
    Priority:
    1) ?org_id=... (URL override; updates session & user_orgs default)
    2) session["org_id"]
    3) logged-in user → _get_default_org_for_user (user_orgs)
    4) env ORG_ID / DEFAULT_ORG_ID
    5) single org in DB
    """

    # 1) URL override
    q = request.query_params.get("org_id")
    if q:
        try:
            oid = int(q)
            request.session["org_id"] = oid
            uid = request.session.get("uid")
            if uid:
                # ensure membership exists and make this the default
                _add_user_to_org(int(uid), oid, make_default=True)
            return oid
        except ValueError:
            pass

    # 2) session retains current org
    try:
        if "org_id" in request.session:
            return int(request.session["org_id"])
    except Exception:
        pass

    # 3) logged-in user membership
    uid = request.session.get("uid")
    if uid:
        oid = _get_default_org_for_user(int(uid))
        request.session["org_id"] = oid
        return oid

    # 4) env fallback (mainly dev / anonymous)
    for key in ("ORG_ID", "DEFAULT_ORG_ID"):
        v = os.getenv(key)
        if v and v.isdigit():
            oid = int(v)
            request.session["org_id"] = oid
            return oid

    # 5) single org
    db_local = DB(os.getenv("DB_PATH", "ofgem.db"))
    orgs = db_local.list_orgs()
    if len(orgs) == 1:
        oid = int(orgs[0]["id"])
        request.session["org_id"] = oid
        return oid

    raise HTTPException(
        status_code=400,
        detail="No organisation selected. Ask your administrator to link your user to an organisation.",
    )
def resolve_org_id_soft(request: Request) -> int | None:
    """
    Soft version of resolve_org_id:
    - Tries to resolve an org exactly the same way.
    - If it fails (no org selected, bad state, etc.), returns None instead of raising.
    Used mainly by the global header / render() so login pages etc. don't 400.
    """
    try:
        return resolve_org_id(request)
    except HTTPException:
        return None
    except Exception:
        return None




def _org_name_by_id(org_id: Optional[int]) -> Optional[str]:
    if org_id is None:
        return None
    row = _sql_one("SELECT name FROM orgs WHERE id = ?", (int(org_id),))
    return row["name"] if row else f"Organisation {org_id}"


# ---------------------------------------------------------------------------
# Render helper (injects uid, org_id, org_name globally for base.html)
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
    return db.list_items(limit=limit)


@app.get("/feed.json")
def feed(limit: int = Query(5000, ge=1, le=20000)):
    return db.list_items(limit=limit)


@app.get("/feed.csv")
def feed_csv(limit: int = Query(5000, ge=1, le=20000)):
    rows = db.list_items(limit=limit)
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
    """Serve precomputed JSON (public/items.json)."""
    path = PUBLIC_DIR / "items.json"
    if not path.exists():
        return JSONResponse(
            {"error": "items.json not found. Run tools/export_json.py first."},
            status_code=404,
        )
    return FileResponse(path, media_type="application/json")


# ---------------------------------------------------------------------------
# Summaries UI (search + date + source + topics + saved filters)
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

    all_items = db.list_items(limit=20000)

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
        text = f"{e.get('title','')} {e.get('content','')} {e.get('summary','')}".lower()
        if q_lower and q_lower not in text:
            continue
        if not in_date_range(e.get("published_at")):
            continue

        tags_raw = e.get("tags") or []
        tags = [t.lower() for t in (tags_raw if isinstance(tags_raw, list) else [])]
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
    saved_filters = db.list_saved_filters() if uid else []

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
            "org_id": org_id,
            "org_name": org_name,
        },
    )


# ---------------------------------------------------------------------------
# Org selection (simple chooser)
# ---------------------------------------------------------------------------
@app.get("/orgs/switch")
def switch_org(request: Request, org_id: int = Query(...), next: str = Query("/summaries")):
    request.session["org_id"] = int(org_id)
    return RedirectResponse(url=next, status_code=303)


@app.get("/orgs/select")
def select_org_page(request: Request):
    orgs = db.list_orgs()
    html = ["<h1>Select organisation</h1><ul>"]
    for o in orgs:
        html.append(
            f'<li><a href="/orgs/switch?org_id={o["id"]}&next=/summaries">'
            f'{o["name"]}</a></li>'
        )
    html.append("</ul>")
    return HTMLResponse("".join(html))

def _org_personnel(org_id: int):
    """
    Returns (key_people, ultimate_owner)

    key_people: list of org_members rows flagged as key personnel
                (and always includes the ultimate risk owner if set)
    ultimate_owner: single row (or None) for is_ultimate_risk_owner = 1
    """
    rows = _sql_all_safe("""
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
    """, (org_id,))

    ultimate_owner = next(
        (r for r in rows if int(r.get("is_ultimate_risk_owner", 0)) == 1),
        None,
    )

    key_people = [
        r for r in rows
        if int(r.get("is_key_personnel", 0)) == 1 or r is ultimate_owner
    ]

    return key_people, ultimate_owner


# ---------------------------------------------------------------------------
# Saved Filters API
# ---------------------------------------------------------------------------
class SavedFilterIn(BaseModel):
    name: str
    params: Dict[str, Any]
    cadence: Optional[str] = None


@app.get("/api/saved-filters")
def list_saved_filters():
    return {"filters": db.list_saved_filters()}


@app.post("/api/saved-filters")
def create_saved_filter(payload: SavedFilterIn):
    params = payload.params or {}
    for key in ("sources", "topics"):
        if key in params and not isinstance(params[key], list):
            params[key] = [params[key]]
    fid = db.create_saved_filter(
        name=payload.name.strip(),
        params_json=json.dumps(params, ensure_ascii=False),
        cadence=payload.cadence,
    )
    return {"ok": True, "id": fid}


@app.delete("/api/saved-filters/{filter_id}")
def delete_saved_filter(filter_id: int):
    if not db.get_saved_filter(filter_id):
        raise HTTPException(status_code=404, detail="Filter not found")
    db.delete_saved_filter(filter_id)
    return {"ok": True}


@app.get("/apply-saved-filter")
def apply_saved_filter(filter_id: int):
    rec = db.get_saved_filter(filter_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Filter not found")
    try:
        params = json.loads(rec["params_json"])
        query_items: List[Tuple[str, str]] = []
        for k, v in params.items():
            if v is None or v == "":
                continue
            if k in ("sources", "topics") and isinstance(v, list):
                for val in v:
                    query_items.append((k, str(val)))
            else:
                query_items.append((k, str(v)))
        query = _urlencode(query_items, doseq=True)
        return RedirectResponse(url=f"/summaries?{query}")
    except Exception:
        raise HTTPException(status_code=400, detail="Saved filter has invalid params")


# ---------------------------------------------------------------------------
# Folders & Saved items (email-keyed)
# ---------------------------------------------------------------------------
def _list_folders(user_email: str) -> List[dict]:
    return _sql_all(
        "SELECT id, name, created_at FROM folders "
        "WHERE user_email = ? ORDER BY LOWER(name)",
        (user_email,),
    )


def _create_folder(user_email: str, name: str) -> int:
    name = (name or "").strip()
    if not name:
        raise HTTPException(400, "Folder name cannot be empty")
    try:
        _sql_exec(
            "INSERT INTO folders (user_email, name, created_at) "
            "VALUES (?, ?, datetime('now'))",
            (user_email, name),
        )
    except Exception:
        existing = _sql_one(
            "SELECT id FROM folders WHERE user_email = ? AND name = ?",
            (user_email, name),
        )
        if existing:
            raise HTTPException(409, "Folder already exists")
        raise
    row = _sql_one(
        "SELECT id FROM folders WHERE user_email = ? AND name = ?",
        (user_email, name),
    )
    if not row:
        raise HTTPException(500, "Failed to create folder")
    return int(row["id"])


def _save_item(
    user_email: str,
    item_guid: str,
    folder_id: Optional[int] = None,
) -> None:
    item_guid = (item_guid or "").strip()
    if not item_guid:
        raise HTTPException(400, "Missing guid")

    if folder_id is not None:
        owner = _sql_one(
            "SELECT id FROM folders WHERE id = ? AND user_email = ?",
            (folder_id, user_email),
        )
        if not owner:
            raise HTTPException(400, "Folder not found")

    existing = _sql_one(
        "SELECT 1 FROM saved_items WHERE user_email = ? AND item_guid = ?",
        (user_email, item_guid),
    )
    if existing:
        raise HTTPException(409, "Item already saved")

    _sql_exec(
        "INSERT INTO saved_items (user_email, item_guid, folder_id, created_at) "
        "VALUES (?, ?, ?, datetime('now'))",
        (user_email, item_guid, folder_id),
    )


def _unsave_item(user_email: str, item_guid: str) -> None:
    _sql_exec(
        "DELETE FROM saved_items WHERE user_email = ? AND item_guid = ?",
        (user_email, item_guid),
    )


def _list_saved_items(
    user_email: str,
    folder_id: Optional[int] = None,
) -> List[dict]:
    rows = _sql_all(
        "SELECT item_guid, folder_id, created_at FROM saved_items "
        "WHERE user_email = ? AND (? IS NULL OR folder_id = ?) "
        "ORDER BY datetime(created_at) DESC",
        (user_email, folder_id, folder_id),
    )
    items = db.list_items(limit=20000)
    by_guid = {str(e.get("guid") or ""): e for e in items if e.get("guid")}
    out: List[dict] = []
    for r in rows:
        g = str(r["item_guid"])
        e = by_guid.get(g) or {}
        folder_name = None
        if r.get("folder_id"):
            f = _sql_one("SELECT name FROM folders WHERE id = ?", (r["folder_id"],))
            folder_name = f["name"] if f else None
        out.append(
            {
                "guid": g,
                "title": e.get("title", "(item unavailable)"),
                "link": e.get("link", ""),
                "published_at": e.get("published_at", ""),
                "source": e.get("source", ""),
                "folder": folder_name,
            }
        )
    return out


class SaveIn(BaseModel):
    guid: str
    folder_id: Optional[int] = None


class FolderIn(BaseModel):
    name: str


@app.get("/saved", response_class=HTMLResponse)
def saved_page(request: Request, folder_id: Optional[int] = Query(default=None)):
    user_email = current_user_email(request)
    folders = _list_folders(user_email)
    items = _list_saved_items(user_email, folder_id=folder_id)

    active_folder_name = None
    if folder_id is not None:
        row = _sql_one(
            "SELECT name FROM folders WHERE id = ? AND user_email = ?",
            (folder_id, user_email),
        )
        active_folder_name = row["name"] if row else None

    return render(
        request,
        "saved.html",
        {
            "folders": folders,
            "items": items,
            "active_folder": folder_id,
            "active_folder_name": active_folder_name,
        },
    )


@app.post("/folders/new")
async def folders_new(request: Request, name: str = Form(...)):
    user_email = current_user_email(request)
    try:
        _create_folder(user_email, name)
        return RedirectResponse("/saved", status_code=303)
    except HTTPException as e:
        folders = _list_folders(user_email)
        items = _list_saved_items(user_email, None)
        return render(
            request,
            "saved.html",
            {
                "folders": folders,
                "items": items,
                "active_folder": None,
                "error": e.detail,
            },
        )


@app.post("/api/save")
def api_save(request: Request, payload: SaveIn):
    user_email = current_user_email(request)
    _save_item(user_email, payload.guid, payload.folder_id)
    return {"ok": True}


@app.api_route("/api/unsave", methods=["DELETE", "POST"])
def api_unsave(
    request: Request,
    guid: Optional[str] = Query(None),
    payload: Optional[Dict[str, Any]] = Body(None),
):
    user_email = current_user_email(request)
    if not guid and payload and "guid" in payload:
        guid = str(payload["guid"])
    if not guid:
        raise HTTPException(400, "Missing guid")
    _unsave_item(user_email, guid)
    return {"ok": True}


@app.get("/api/folders")
def api_list_folders(request: Request):
    user_email = current_user_email(request)
    return {"folders": _list_folders(user_email)}


@app.post("/api/folders")
def api_create_folder(request: Request, payload: FolderIn):
    user_email = current_user_email(request)
    fid = _create_folder(user_email, payload.name)
    return {"ok": True, "id": fid}


# ---------------------------------------------------------------------------
# Tagging items → sites & controls
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}/tag-options")
def api_tag_options(request: Request, org_id: int):
    sites = db.list_sites(org_id)
    ctrls = db.list_all_controls_for_org(org_id)
    return {"sites": sites, "controls": ctrls}


@app.get("/api/orgs/{org_id}/sites")
def api_org_sites(org_id: int):
    with db._conn() as conn:  # type: ignore[attr-defined]
        cur = conn.execute(
            "SELECT id, name FROM sites WHERE org_id=? ORDER BY name", (int(org_id),)
        )
        return JSONResponse([{"id": r["id"], "name": r["name"]} for r in cur.fetchall()])


@app.get("/api/orgs/{org_id}/org-controls")
def api_org_controls(org_id: int):
    with db._conn() as conn:  # type: ignore[attr-defined]
        cur = conn.execute(
            """
            SELECT oc.id, oc.title, oc.code, oc.site_id, s.name AS site_name
            FROM org_controls oc
            LEFT JOIN sites s ON s.id = oc.site_id
            WHERE oc.org_id=?
            ORDER BY COALESCE(s.name,''), COALESCE(oc.code,''), oc.title
            """,
            (int(org_id),),
        )
        rows = [
            {
                "id": r["id"],
                "title": r["title"],
                "code": r["code"],
                "site_id": r["site_id"],
                "site_name": r["site_name"],
            }
            for r in cur.fetchall()
        ]
        return JSONResponse(rows)


@app.get("/api/orgs/current/sites")
def api_current_org_sites(request: Request):
    oid = resolve_org_id(request)
    return api_org_sites(oid)


@app.get("/api/orgs/current/org-controls")
def api_current_org_controls(request: Request):
    oid = resolve_org_id(request)
    return api_org_controls(oid)


@app.post("/api/items/tag")
async def api_tag_item(request: Request):
    user = current_user_email(request)
    data = await request.json()

    guid = (data.get("guid") or "").strip()
    if not guid:
        return JSONResponse({"detail": "guid required"}, status_code=400)

    org_id = data.get("org_id")
    if org_id in (None, "", "null"):
        try:
            org_id = resolve_org_id(request)
        except HTTPException as hx:
            return JSONResponse({"detail": hx.detail}, status_code=hx.status_code)
    try:
        org_id = int(org_id)
    except (TypeError, ValueError):
        return JSONResponse({"detail": "org_id must be an integer"}, status_code=400)

    def _to_int_or_none(v):
        if v in (None, "", "null"):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    site_id = _to_int_or_none(data.get("site_id"))
    org_control_id = _to_int_or_none(data.get("org_control_id"))

    now = datetime.now(timezone.utc).isoformat()
    with db._conn() as conn:  # type: ignore[attr-defined]
        if site_id is not None:
            cur = conn.execute(
                "SELECT 1 FROM sites WHERE id=? AND org_id=?",
                (site_id, org_id),
            )
            if cur.fetchone() is None:
                return JSONResponse(
                    {"detail": "site_id does not belong to this org"}, status_code=400
                )

        if org_control_id is not None:
            cur = conn.execute(
                "SELECT 1 FROM org_controls WHERE id=? AND org_id=?",
                (org_control_id, org_id),
            )
            if cur.fetchone() is None:
                return JSONResponse(
                    {"detail": "org_control_id does not belong to this org"},
                    status_code=400,
                )

        cur = conn.execute(
            """
            SELECT 1 FROM user_item_tags
            WHERE user_email=? AND item_guid=? AND org_id=?
              AND IFNULL(site_id,-1) = IFNULL(?, -1)
              AND IFNULL(org_control_id,-1) = IFNULL(?, -1)
            """,
            (user, guid, org_id, site_id, org_control_id),
        )
        exists = cur.fetchone() is not None
        if not exists:
            conn.execute(
                """
                INSERT INTO user_item_tags (user_email, item_guid, org_id, site_id, org_control_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                ,
                (user, guid, org_id, site_id, org_control_id, now),
            )
            conn.commit()

        cur = conn.execute(
            """
            SELECT t.id, t.org_id, t.site_id, t.org_control_id,
                   s.name AS site_name,
                   oc.title AS control_title, oc.code AS control_code
            FROM user_item_tags t
            LEFT JOIN sites s ON s.id = t.site_id
            LEFT JOIN org_controls oc ON oc.id = t.org_control_id
            WHERE t.user_email=? AND t.item_guid=? AND t.org_id=?
            ORDER BY COALESCE(s.name,''), COALESCE(oc.code,''), COALESCE(oc.title,'')
            """,
            (user, guid, org_id),
        )
        tags = []
        for r in cur.fetchall():
            tags.append(
                {
                    "id": r["id"],
                    "org_id": r["org_id"],
                    "site_id": r["site_id"],
                    "site_name": r["site_name"],
                    "org_control_id": r["org_control_id"],
                    "control_code": r["control_code"],
                    "control_title": r["control_title"],
                }
            )

    return JSONResponse({"ok": True, "tags": tags})


@app.delete("/api/items/tag")
async def api_untag_item(request: Request):
    user = current_user_email(request)
    data = await request.json()
    guid = (data.get("guid") or "").strip()
    org_id = data.get("org_id")
    site_id = data.get("site_id")
    org_control_id = data.get("org_control_id")

    if not guid:
        return JSONResponse({"detail": "guid required"}, status_code=400)

    if org_id in (None, "", "null"):
        org_id = resolve_org_id(request)
    try:
        org_id = int(org_id)
    except (TypeError, ValueError):
        return JSONResponse({"detail": "org_id must be an integer"}, status_code=400)

    def _to_int_or_none(v):
        if v in (None, "", "null"):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    site_id = _to_int_or_none(site_id)
    org_control_id = _to_int_or_none(org_control_id)

    with db._conn() as conn:  # type: ignore[attr-defined]
        conn.execute(
            """
            DELETE FROM user_item_tags
            WHERE user_email=? AND item_guid=? AND org_id=?
              AND IFNULL(site_id,-1)=IFNULL(?, -1)
              AND IFNULL(org_control_id,-1)=IFNULL(?, -1)
            """,
            (user, guid, org_id, site_id, org_control_id),
        )
        conn.commit()
    return JSONResponse({"ok": True})


# ---------------------------------------------------------------------------
# Org & site helpers
# ---------------------------------------------------------------------------
def _sql_all_safe(q, params=()):
    try:
        return _sql_all(q, params)
    except Exception:
        return []


def _sql_one_safe(q, params=()):
    try:
        return _sql_one(q, params)
    except Exception:
        return None


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
    """
    Use the same DB wrapper as /orgs/{org_id}/sites so we see the exact
    same site list on the overview page.
    """
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    return db.list_sites(org_id)

def _org_counts(org_id: int):
    # org_controls(id, org_id, ...)
    c_org = _sql_one_safe(
        "SELECT COUNT(*) AS n FROM org_controls WHERE org_id = ?",
        (org_id,),
    ) or {"n": 0}

    # org_risks(id, org_id, ...)
    r_org = _sql_one_safe(
        "SELECT COUNT(*) AS n FROM org_risks WHERE org_id = ?",
        (org_id,),
    ) or {"n": 0}

    # site_controls: site_controls.site_id → sites.id → sites.org_id
    c_site = _sql_one_safe(
        """
        SELECT COUNT(*) AS n
        FROM site_controls sc
        JOIN sites s ON s.id = sc.site_id
        WHERE s.org_id = ?
        """,
        (org_id,),
    ) or {"n": 0}

    # site_risks: site_risks.site_id → sites.id → sites.org_id
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
        "org_risks": r_org["n"],
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


# ---------------------------------------------------------------------------
# Organisation & Sites pages
# ---------------------------------------------------------------------------
@app.get("/orgs/{org_id}", response_class=HTMLResponse)
def org_overview_page(request: Request, org_id: int):
    # Make this org the current one for the session/header
    request.session["org_id"] = int(org_id)

    org = _org_basic(org_id)
    if not org:
        raise HTTPException(status_code=404, detail="Organisation not found")

    # NEW: key personnel
    key_people, ultimate_owner = _org_personnel(org_id)

    sites  = _org_sites(org_id)
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







@app.get("/orgs/{org_id}/sites", response_class=HTMLResponse)
def list_sites_page(request: Request, org_id: int):
    org = _org_basic(org_id)
    sites = db.list_sites(org_id)
    return render(
        request,
        "sites.html",
        {
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
    db.upsert_site(org_id, name, code, location)
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
# Org controls
# ---------------------------------------------------------------------------
def _current_user_display(request: Request) -> str:
    return current_user_email(request) or os.getenv("DEV_USER") or "andrewpeat"


@app.get("/orgs/{org_id}/controls", response_class=HTMLResponse)
def org_controls_page(
    request: Request,
    org_id: int,
    site: Optional[int] = None,
):
    orgs = db.list_orgs()
    org = next((o for o in orgs if o["id"] == org_id), None)
    sites = db.list_sites(org_id)

    if site:
        controls = db.list_org_controls(org_id, site_id=site)
        current_site = next((s for s in sites if s["id"] == site), None)
    else:
        controls = db.list_org_controls(org_id)
        current_site = None

    grouped: Dict[str, List[dict]] = {}
    for c in controls:
        group = c.get("site_name") or (
            current_site["name"] if current_site else "Corporate"
        )
        grouped.setdefault(group, []).append(c)

    return render(
        request,
        "org_controls.html",
        {
            "org": org,
            "org_id": org_id,
            "sites": sites,
            "grouped": grouped,
            "current_site": current_site,
            "active_tab": "controls",
        },
    )


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
    db.upsert_org_control(
        org_id=org_id,
        site_id=site_id,
        code=code,
        title=title,
        description=description,
        owner_email=owner_email,
        tags=tags,
        status=status,
        risk=risk,
        review_frequency_days=rfd,
        next_review_at=next_review_at,
        created_by=user,
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
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))

    org = _sql_one("""
        SELECT id, name, head_office_address, phone, email
        FROM orgs WHERE id = ?
    """, (org_id,)) or {"id": org_id, "name": f"Organisation {org_id}"}

    filters, params = ["org_id = ?"], [org_id]
    if status:
        filters.append("LOWER(status) = LOWER(?)"); params.append(status)
    if severity:
        filters.append("LOWER(severity) = LOWER(?)"); params.append(severity)
    if category:
        filters.append("LOWER(category) = LOWER(?)"); params.append(category)
    where_sql = "WHERE " + " AND ".join(filters)

    total = (_sql_one(f"SELECT COUNT(*) AS n FROM org_risks {where_sql}", tuple(params)) or {"n": 0})["n"]
    offset = (page - 1) * per_page

    # pull description as well so the modal can show it
    risks = _sql_all(f"""
        SELECT id, code, title, description, status, severity, category,
               owner_name, owner_email, created_at, updated_at
        FROM org_risks
        {where_sql}
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT ? OFFSET ?
    """, tuple(params + [per_page, offset]))

    # attach control counts
    for r in risks:
        row = _sql_one("SELECT COUNT(*) AS n FROM org_controls_risks WHERE org_risk_id = ?", (r["id"],))
        r["controls_count"] = (row or {"n": 0})["n"]

    total_pages = max(1, (total + per_page - 1) // per_page)
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    status_choices   = ["Open", "In progress", "Mitigated", "Closed"]
    severity_choices = ["Low", "Medium", "High", "Severe"]
    cats = _sql_all("""
        SELECT DISTINCT category
        FROM org_risks
        WHERE org_id = ? AND category IS NOT NULL AND category != ''
        ORDER BY LOWER(category)
    """, (org_id,))
    category_choices = [c["category"] for c in cats]

    # 👇 NEW: all org-level controls, for the mapping dropdown in the modal
    org_controls = db.list_org_controls(org_id)

    return templates.TemplateResponse("org_risks.html", {
        "request": request,
        "org": org,
        "org_id": org_id,
        "risks": risks,
        "status_choices": status_choices,
        "severity_choices": severity_choices,
        "category_choices": category_choices,
        "active": {"status": status, "severity": severity, "category": category},
        "page": page, "total_pages": total_pages, "page_numbers": page_numbers,
        "controls": org_controls,   # 👈 important
    })

@app.post("/orgs/{org_id}/org-risks/link-control")
def org_risk_link_control(
    request: Request,
    org_id: int,
    risk_id: int = Form(...),
    control_id: int = Form(...),
):
    # adjust columns if your join table is slightly different
    _sql_exec(
        """
        INSERT OR IGNORE INTO org_controls_risks (org_control_id, org_risk_id)
        VALUES (?, ?)
        """,
        (control_id, risk_id),
    )
    return RedirectResponse(
        url=f"/orgs/{org_id}/org-risks",
        status_code=303,
    )


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
    org = _sql_one("SELECT id, name FROM orgs WHERE id = ?", (org_id,)) or {
        "id": org_id,
        "name": f"Organisation {org_id}",
    }
    site = _sql_one(
        "SELECT id, org_id, name, address, phone, email FROM sites WHERE id = ?",
        (site_id,),
    )
    if not site or site["org_id"] != org_id:
        raise HTTPException(404, "Site not found in this organisation")

    filters, params = ["site_id = ?"], [site_id]
    if status:
        filters.append("LOWER(status) = LOWER(?)")
        params.append(status)
    if severity:
        filters.append("LOWER(severity) = LOWER(?)")
        params.append(severity)
    if category:
        filters.append("LOWER(category) = LOWER(?)")
        params.append(category)
    where_sql = "WHERE " + " AND ".join(filters)

    total = (
        _sql_one(
            f"SELECT COUNT(*) AS n FROM site_risks {where_sql}", tuple(params)
        )
        or {"n": 0}
    )["n"]
    offset = (page - 1) * per_page

    risks = _sql_all(
        f"""
        SELECT id, code, title, status, severity, category,
               owner_name, owner_email, created_at, updated_at
        FROM site_risks
        {where_sql}
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT ? OFFSET ?
        """,
        tuple(params + [per_page, offset]),
    )

    for r in risks:
        row = _sql_one(
            "SELECT COUNT(*) AS n FROM site_controls_risks WHERE site_risk_id = ?",
            (r["id"],),
        )
        r["controls_count"] = (row or {"n": 0})["n"]

    total_pages = max(1, (total + per_page - 1) // per_page)
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    cats = _sql_all(
        """
        SELECT DISTINCT category FROM site_risks
        WHERE site_id = ? AND category IS NOT NULL AND category != ''
        ORDER BY LOWER(category)
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
# /controls (simple global list, not org-scoped)
# ---------------------------------------------------------------------------
router = APIRouter()


@app.get("/controls", response_class=HTMLResponse)
def controls_page(request: Request):
    all_controls = db.list_controls()
    return render(
        request,
        "controls.html",
        {
            "controls": all_controls,
        },
    )


@router.get("/controls/{cid}", response_class=HTMLResponse)
def control_detail(request: Request, cid: int):
    items = db.list_items_for_org_control(cid, limit=100)
    oc = [r for r in db.list_org_controls(1) if r["id"] == cid]
    return render(
        request,
        "control_detail.html",
        {"control": oc[0] if oc else None, "items": items},
    )


@router.post("/send", response_class=HTMLResponse)
async def send_article_fragment(guid: str = Form(...), email: str = Form(...)):
    items = [i for i in db.list_items(limit=5000) if i.get("guid") == guid]
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
# AI Summary API (+ PDF handling)
# ---------------------------------------------------------------------------
class AISummaryReq(BaseModel):
    guid: str


def _fallback_ai_summary(text: str, limit_words: int = 100) -> str:
    words = (text or "").split()
    snippet = " ".join(words[:limit_words])
    return snippet + ("…" if len(words) > limit_words else "")


def _openai_client():
    try:
        from openai import OpenAI

        key = os.getenv("OPENAI_API_KEY")
        if not key:
            print("[AI] ⚠️ No OPENAI_API_KEY found in environment")
            return None
        print("[AI] ✅ OpenAI API key found, creating client")
        return OpenAI(api_key=key)
    except Exception as e:  # pragma: no cover
        print(f"[AI] ❌ Failed to create OpenAI client: {e}")
        return None


_BOILERPLATE_PATTERNS = [
    r"\bskip to (main )?content\b",
    r"\b(main )?navigation\b",
    r"\b(show/?hide|toggle) menu\b",
    r"\b(sign in|register|log ?in|log ?out)\b",
    r"\b(search|search results|reset button in search)\b",
    r"\b(cookie(s)? (banner|settings|preferences)|accept all cookies)\b",
    r"\b(user account menu)\b",
    r"\bfooter\b",
    r"\bshare (this )?page\b",
    r"\brelated (content|links)\b",
    r"\bdata portal\b",
]
_BP_REGEX = re.compile("|".join(_BOILERPLATE_PATTERNS), re.IGNORECASE)


def _clean_extracted_text(title: str, text: str, max_chars: int = 12000) -> str:
    if not text:
        return text
    raw = re.sub(r"[ \t]+", " ", text)
    raw = re.sub(r"\r\n?", "\n", raw)
    lines = [ln.strip() for ln in raw.split("\n")]

    kept: List[str] = []
    ttl = (title or "").strip()
    ttl_low = ttl.lower()

    for ln in lines:
        if not ln:
            continue
        if _BP_REGEX.search(ln):
            continue
        if len(ln) <= 3:
            continue
        if len(ln) <= 18 and not ln.endswith((".", ":", "?", "!", "…")):
            continue
        if ttl and ln.lower() == ttl_low:
            continue
        kept.append(ln)

    cleaned = "\n".join(kept)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    if len(cleaned) < 200:
        cleaned = text.strip()
    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars]
    return cleaned


def _is_boilerplate_summary(text: str) -> bool:
    if not text:
        return True
    bad_snippets = [
        "skip to main content",
        "user account menu",
        "reset button in search",
        "data portal",
        "sign in / register",
        "show/hide menu",
        "main navigation",
        "cookies",
    ]
    t = text.lower().strip()
    return any(snip in t for snip in bad_snippets)


def _generate_ai_summary(
    title: str,
    text: str,
    limit_words: int = 100,
    guid: Optional[str] = None,
) -> str:
    print(f"[AI] 🔎 Generating summary guid={guid} title={title[:60]!r} len={len(text)}")
    text = (text or "").strip()
    if not text:
        print("[AI] ⚠️ No text provided to summarise.")
        return "No content available to summarise."

    client = _openai_client()
    if not client:
        print("[AI] ⚠️ No OpenAI client available — using fallback snippet.")
        return _fallback_ai_summary(text, limit_words)

    prompt = f"""Summarise the following item in up to {limit_words} words.
Plain UK English, no bullet points, no headings. Cover what it is, who it affects, and likely action/implication.

TITLE: {title}
TEXT:
{text[:6000]}
"""
    try:
        print("[AI] 🧠 Sending request to OpenAI API...")
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise UK energy regulation analyst.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        out = (resp.choices[0].message.content or "").strip()
        print("[AI] ✅ Received summary from OpenAI")
        words = out.split()
        if len(words) > limit_words:
            out = " ".join(words[:limit_words]) + "…"
        return out
    except Exception as e:  # pragma: no cover
        print(f"[AI] ❌ OpenAI request failed: {e}")
        return _fallback_ai_summary(text, limit_words)


def _is_pdf_link(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
        return path.endswith(".pdf")
    except Exception:
        return False


def _is_pdf_content_type(resp) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    return "pdf" in ct or ct.strip() == "application/octet-stream"


def _fetch_pdf_bytes(url: str, timeout: int = 30) -> bytes:
    try:
        h = requests.head(url, timeout=timeout, allow_redirects=True)
        if h.ok and not _is_pdf_content_type(h):
            pass
    except Exception:
        pass

    r = requests.get(url, timeout=timeout, allow_redirects=True, stream=True)
    r.raise_for_status()
    total = 0
    chunks: List[bytes] = []
    for chunk in r.iter_content(1024 * 64):
        if not chunk:
            break
        total += len(chunk)
        if total > 15 * 1024 * 1024:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def _pdf_bytes_to_text_pypdf(blob: bytes, max_pages: int = 8) -> str:
    from pypdf import PdfReader

    try:
        reader = PdfReader(io.BytesIO(blob))
        out: List[str] = []
        for i, page in enumerate(reader.pages):
            if i >= max_pages:
                break
            try:
                txt = page.extract_text() or ""
            except Exception:
                txt = ""
            if txt:
                out.append(txt)
        return "\n".join(out).strip()
    except Exception:
        return ""


def _find_item_by_guid(guid: str) -> Optional[dict]:
    items = db.list_items(limit=20000)
    for it in items:
        if (it.get("guid") or it.get("link")) == guid:
            return it
    return None


@app.get("/api/test-openai")
def test_openai():
    from openai import OpenAI

    key = os.getenv("OPENAI_API_KEY")
    if not key:
        return {"ok": False, "error": "No API key found"}
    try:
        client = OpenAI(api_key=key)
        resp = client.models.list()
        return {"ok": True, "models": [m.id for m in resp.data[:5]]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get_cached_ai_summary(guid: str) -> Optional[str]:
    row = _sql_one(
        "SELECT ai_summary FROM items WHERE guid = ? LIMIT 1",
        (guid,),
    )
    if row and row.get("ai_summary"):
        return row["ai_summary"]
    row = _sql_one(
        "SELECT ai_summary FROM items WHERE link = ? LIMIT 1",
        (guid,),
    )
    if row and row.get("ai_summary"):
        return row["ai_summary"]
    return None


def _set_cached_ai_summary(guid: str, summary: str) -> None:
    _sql_exec("UPDATE items SET ai_summary = ? WHERE guid = ?", (summary, guid))
    _sql_exec("UPDATE items SET ai_summary = ? WHERE link = ?", (summary, guid))


@app.post("/api/ai-summary")
def ai_summary(req: AISummaryReq):
    item = _find_item_by_guid(req.guid)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    if item.get("ai_summary"):
        print(f"[AI] ♻️ Using cached summary for {req.guid}")
        return JSONResponse({"ok": True, "summary": item["ai_summary"]})

    cached = _get_cached_ai_summary(req.guid)
    if cached:
        return JSONResponse({"ok": True, "summary": cached, "cached": True})

    title = (item.get("title") or "").strip()
    link = (item.get("link") or "").strip()
    text = (item.get("content") or item.get("summary") or "").strip()

    wants_pdf = ("[PDF document" in text) or _is_pdf_link(link)
    if wants_pdf and link:
        try:
            blob = _fetch_pdf_bytes(link)
            extracted = _pdf_bytes_to_text_pypdf(blob, max_pages=8)
            if extracted:
                text = extracted
            else:
                return JSONResponse(
                    {
                        "ok": True,
                        "summary": "This PDF appears to be image-based or has no extractable text. Please open the document to view.",
                    }
                )
        except Exception:
            return JSONResponse(
                {
                    "ok": True,
                    "summary": "Could not fetch or parse the PDF for summary. Please open the document to view.",
                }
            )

    if not text:
        return JSONResponse({"ok": True, "summary": "No content available to summarise."})

    text = _clean_extracted_text(title, text)
    summary = _generate_ai_summary(title, text, limit_words=120, guid=req.guid)

    if not _is_boilerplate_summary(summary):
        try:
            _sql_exec(
                "UPDATE items SET ai_summary = ? WHERE guid = ?",
                (summary, req.guid),
            )
        except Exception as e:
            print(f"[AI] ⚠️ Failed to cache summary: {e}")

    try:
        _set_cached_ai_summary(req.guid, summary)
    except Exception as e:
        print("[AI] ⚠️ Failed to cache summary (secondary):", e)

    return JSONResponse({"ok": True, "summary": summary, "cached": False})


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

    # 🔑 default org for this user (creates membership if needed)
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

    # 🔑 also ensure they belong to an org and pick a default
    org_id = _get_default_org_for_user(uid)
    request.session["org_id"] = org_id

    return RedirectResponse(url="/summaries", status_code=302)



@app.post("/account/logout")
def account_logout_post(request: Request):
    request.session.clear()
    return RedirectResponse(url="/account/login", status_code=302)
