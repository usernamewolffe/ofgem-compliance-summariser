# api/server.py
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import os, csv, io, json, requests, sqlite3, re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Set, Dict, Any, Tuple
from urllib.parse import urlparse, urlencode

from fastapi import FastAPI, Request, Query, HTTPException, Form, Body, APIRouter
from fastapi.responses import (
    RedirectResponse,
    StreamingResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jinja2 import TemplateNotFound, ChoiceLoader, FileSystemLoader
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from passlib.context import CryptContext
from pydantic import BaseModel
from datetime import datetime, timezone

from tools.email_utils import send_article_email
from storage.db import DB
from fastapi import Form
# api/server.py
from fastapi.staticfiles import StaticFiles



from fastapi import Body  # if not already imported

def current_user_email(request: Request) -> str:
    # Adjust to your session shape if needed
    return (getattr(request, "session", {}).get("user")
            or os.getenv("DEV_USER")
            or "andrewpeat@example.com")

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="web/static"), name="static")

# Templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# ---------------------------------------------------------------------------
# Rolling Session Middleware (handles inactivity)
# ---------------------------------------------------------------------------
INACTIVITY_SECONDS = int(os.getenv("INACTIVITY_SECONDS", str(3 * 60 * 60)))  # 3h

class RollingSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        now = datetime.now(timezone.utc)

        def is_api(req: Request) -> bool:
            accept = (req.headers.get("accept") or "").lower()
            return req.url.path.startswith("/api/") or "application/json" in accept

        # If logged in, check inactivity
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

            # Update last activity (rolling)
            request.session["last_activity"] = now.isoformat()

        response = await call_next(request)

        # Refresh cookie expiry if active
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

# ---------------------------------------------------------------------------
# Sessions & Middleware (Session added last so it runs first)
# ---------------------------------------------------------------------------
SESSION_COOKIE = os.getenv("SESSION_COOKIE", "ofgem_session")
SESSION_MAX_AGE = int(os.getenv("SESSION_MAX_AGE", str(3 * 60 * 60)))  # 3h

# 1️⃣ Add RollingSessionMiddleware first (inner)
app.add_middleware(RollingSessionMiddleware)

# 2️⃣ Add SessionMiddleware last (outermost, runs first)
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSIONS_SECRET", "dev-only-change-me"),
    session_cookie=SESSION_COOKIE,
    max_age=SESSION_MAX_AGE,
    same_site="lax",
    https_only=False,  # set True in production behind HTTPS
)

# ---------------------------------------------------------------------------
# Continue with your routes, login handlers, etc.
# ---------------------------------------------------------------------------
def current_user(request: Request) -> str:
    """
    Temporary user resolver.
    - Uses session if you already have SessionMiddleware.
    - Falls back to DEV_USER env var, else a fixed name.
    """
    return (
        getattr(request, "session", {}).get("user")  # if SessionMiddleware is on
        or os.getenv("DEV_USER")
        or "andrewpeat"
    )


# ---------------------------------------------------------------------------
# Continue with your routes and other setup below...
# ---------------------------------------------------------------------------


TOPIC_TAGS = [
    "CAF/NIS", "Cyber", "Incident", "Consultation", "Guidance", "Enforcement", "Penalty"
]

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = (BASE_DIR / "ofgem.db").as_posix()




# Project DB wrapper
db = DB("ofgem.db")

# Static (optional legacy assets)
app.mount("/static", StaticFiles(directory="api/static", html=True), name="static")
# Serve precomputed JSON (public/items.json) without shadowing app routes
PUBLIC_DIR = BASE_DIR / "public"
if PUBLIC_DIR.exists():
    # e.g. /public/items.json
    app.mount("/public", StaticFiles(directory=str(PUBLIC_DIR), html=False), name="public")

from fastapi.responses import FileResponse

@app.get("/items.json")
def items_json():
    """Convenience route so the page can fetch /items.json directly."""
    path = PUBLIC_DIR / "items.json"
    if not path.exists():
        return JSONResponse({"error": "items.json not found. Run tools/export_json.py first."}, status_code=404)
    return FileResponse(path, media_type="application/json")

TEMPLATES_DIR = BASE_DIR / "summariser" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Make the loader search both the root and the 'account' subdir explicitly
templates.env.loader = ChoiceLoader([
    FileSystemLoader(str(TEMPLATES_DIR)),
    FileSystemLoader(str(TEMPLATES_DIR / "account")),
])

# Helper available in Jinja (used in pagination links)
from urllib.parse import urlencode as _urlencode
templates.env.globals["urlencode"] = _urlencode

# Add urlencode helper for templates (used in pagination)
from urllib.parse import urlencode as _urlencode
templates.env.globals["urlencode"] = _urlencode


# Password hashing context — new hashes use PBKDF2-SHA256 (no 72-byte limit).
# We still accept old bcrypt / bcrypt_sha256 hashes on login.
pwd_ctx = CryptContext(
    schemes=["pbkdf2_sha256", "bcrypt_sha256", "bcrypt"],
    default="pbkdf2_sha256",
    deprecated="auto",
)

def current_user_email(request: Request) -> str:
    # adjust to your session shape if needed
    return (getattr(request, "session", {}).get("user")
            or os.getenv("DEV_USER")
            or "andrewpeat@example.com")


# ---------------------------------------------------------------------------
# SQLite fallback layer (used only if DB wrapper lacks needed methods)
# ---------------------------------------------------------------------------
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

# --- AI summary cache helpers (items.ai_summary column) ---

def _get_cached_ai_summary(guid: str) -> Optional[str]:
    # try guid, then link (because some rows use link as guid)
    row = _sql_one("SELECT ai_summary FROM items WHERE guid = ? LIMIT 1", (guid,))
    if row and row.get("ai_summary"):
        return row["ai_summary"]
    row = _sql_one("SELECT ai_summary FROM items WHERE link = ? LIMIT 1", (guid,))
    if row and row.get("ai_summary"):
        return row["ai_summary"]
    return None

def _set_cached_ai_summary(guid: str, summary: str) -> None:
    # write by guid
    _sql_exec("UPDATE items SET ai_summary = ? WHERE guid = ?", (summary, guid))
    # also try by link (harmless if no match)
    _sql_exec("UPDATE items SET ai_summary = ? WHERE link = ?", (summary, guid))


# -----------------------------
# server.py — replace the folder + saved-items helpers with these

def _list_folders(user_email: str) -> list[dict]:
    return _sql_all(
        "SELECT id, name, created_at FROM folders WHERE user_email = ? ORDER BY LOWER(name)",
        (user_email,),
    )

def _create_folder(user_email: str, name: str) -> int:
    name = (name or "").strip()
    if not name:
        raise HTTPException(400, "Folder name cannot be empty")
    try:
        _sql_exec("INSERT INTO folders (user_email, name, created_at) VALUES (?, ?, datetime('now'))",
                  (user_email, name))
    except Exception:
        existing = _sql_one("SELECT id FROM folders WHERE user_email = ? AND name = ?",
                            (user_email, name))
        if existing:
            raise HTTPException(409, "Folder already exists")
        raise
    row = _sql_one("SELECT id FROM folders WHERE user_email = ? AND name = ?",
                   (user_email, name))
    if not row:
        raise HTTPException(500, "Failed to create folder")
    return int(row["id"])

def _save_item(user_email: str, item_guid: str, folder_id: int | None = None) -> None:
    item_guid = (item_guid or "").strip()
    if not item_guid:
        raise HTTPException(400, "Missing guid")

    if folder_id is not None:
        owner = _sql_one("SELECT id FROM folders WHERE id = ? AND user_email = ?",
                         (folder_id, user_email))
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
    _sql_exec("DELETE FROM saved_items WHERE user_email = ? AND item_guid = ?",
              (user_email, item_guid))

def _list_saved_items(user_email: str, folder_id: int | None = None) -> list[dict]:
    rows = _sql_all(
        "SELECT item_guid, folder_id, created_at FROM saved_items "
        "WHERE user_email = ? AND (? IS NULL OR folder_id = ?) "
        "ORDER BY datetime(created_at) DESC",
        (user_email, folder_id, folder_id),
    )
    items = db.list_items(limit=20000)
    by_guid = {str(e.get("guid") or ""): e for e in items if e.get("guid")}
    out = []
    for r in rows:
        g = str(r["item_guid"])
        e = by_guid.get(g) or {}
        folder_name = None
        if r.get("folder_id"):
            f = _sql_one("SELECT name FROM folders WHERE id = ?", (r["folder_id"],))
            folder_name = f["name"] if f else None
        out.append({
            "guid": g,
            "title": e.get("title", "(item unavailable)"),
            "link": e.get("link", ""),
            "published_at": e.get("published_at", ""),
            "source": e.get("source", ""),
            "folder": folder_name,
        })
    return out


# ---------------------------------------------------------------------------
# Auth tables + helpers
# ---------------------------------------------------------------------------
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


def _get_user_by_email(email: str) -> Optional[dict]:
    return _sql_one("SELECT id, email, password_hash FROM users WHERE email = ?", (email,))

def _create_user(email: str, password_hash: str) -> int:
    _sql_exec("INSERT INTO users (email, password_hash) VALUES (?, ?)", (email, password_hash))
    row = _sql_one("SELECT id FROM users WHERE email = ?", (email,))
    if not row:
        raise RuntimeError("Failed to create user")
    return int(row["id"])

@app.on_event("startup")
def _startup():
    if hasattr(db, "init_auth"):
        try:
            db.init_auth()  # type: ignore[attr-defined]
        except Exception:
            pass
    _ensure_users_tables()

def current_org_id(request: Request) -> int | None:
    try:
        return int(request.session.get("org_id"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Render helper
# ---------------------------------------------------------------------------
def render(request: Request, template_name: str, ctx: dict | None = None):
    ctx = dict(ctx or {})
    try:
        uid = request.session.get("uid")
    except Exception:
        uid = None
    ctx["uid"] = uid
    ctx["request"] = request

    try:
        return templates.TemplateResponse(template_name, ctx)
    except TemplateNotFound:
        # Use Jinja2 environment's search path for error messages
        search_dirs = getattr(templates.env.loader, "searchpath", [])
        searched = ", ".join(search_dirs) if search_dirs else "(unknown)"
        bare = Path(template_name).name
        if bare != template_name:
            try:
                return templates.TemplateResponse(bare, ctx)
            except TemplateNotFound as e2:
                return PlainTextResponse(
                    f"Template not found: '{template_name}' or '{bare}'.\nSearched in: {searched}\n\n{e2}",
                    status_code=500,
                )
        return PlainTextResponse(
            f"Template not found: '{template_name}'.\nSearched in: {searched}",
            status_code=500,
        )
    except Exception as e:
        return PlainTextResponse(f"Template render error in '{template_name}':\n\n{e}", status_code=500)

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
    writer.writerow(["title", "link", "published_at", "tags", "guid", "source", "summary"])
    for r in rows:
        writer.writerow([
            r.get("title", ""),
            r.get("link", ""),
            r.get("published_at", ""),
            r.get("tags", ""),
            r.get("guid", ""),
            r.get("source", ""),
            (r.get("ai_summary") or r.get("summary") or (r.get("content") or "")[:220])
            .replace("\n", " "
            ),
        ])
    out.seek(0)
    return StreamingResponse(
        io.BytesIO(out.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=ofgem_feed.csv"},
    )

# ---------------------------------------------------------------------------
# Summaries UI (search + date + source + topics) with pagination
# ---------------------------------------------------------------------------
@app.get("/summaries", response_class=HTMLResponse)
def summaries_page(
    request: Request,
    q: str = "",
    date_from: str | None = None,
    date_to: str | None = None,
    sources: list[str] = Query(default=[]),
    topics: list[str] = Query(default=[]),
    page: int = 1,
    per_page: int = 25,
):
    # --- org context (variable, not hardcoded) ------------------------------
    org_id = resolve_org_id(request)          # <-- uses query → session → env → DB(singleton)
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    org_name = get_org_name(db, org_id)

    # -----------------------------------------------------------------------
    all_items = db.list_items(limit=20000)

    # helper: date range check
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

    # filter
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

    # sort newest first
    filtered.sort(key=lambda e: e.get("published_at", "") or "", reverse=True)

    # pagination
    page = max(1, int(page))
    per_page = max(1, min(200, int(per_page)))
    total = len(filtered)
    start = (page - 1) * per_page
    end = start + per_page
    page_items = filtered[start:end]
    total_pages = (total + per_page - 1) // per_page if total else 1
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    # sources list & default selection on first load
    all_sources = sorted({(i.get("source") or "").strip() for i in all_items if i.get("source")})
    if not sources and not any([q, date_from, date_to, topics, page != 1]):
        sources = list(all_sources)

    # saved filters only if signed in
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
            "uid": uid,                # used by header for Sign in/out and by UI logic
            "org_id": org_id,          # used by feed JS to call /api/orgs/{org_id}/...
            "org_name": org_name,      # optional: show active org in header
        },
    )

# --- Users ----------------------------------------------------------

from fastapi import Body

def current_user_email(request: Request) -> str:
    return (getattr(request, "session", {}).get("user")
            or os.getenv("DEV_USER")
            or "andrewpeat@example.com")

def resolve_org_id(request: Request) -> int:
    """
    Priority:
    1) ?org_id=... (URL override for deep links)
    2) request.session["org_id"] (sticky selection)
    3) env ORG_ID / DEFAULT_ORG_ID
    4) if DB has exactly one org → that ID
    else raise 400 with a helpful message.
    """
    # 1) URL override
    q = request.query_params.get("org_id")
    if q:
        try:
            oid = int(q)
            request.session["org_id"] = oid
            return oid
        except ValueError:
            pass

    # 2) session
    try:
        if "org_id" in request.session:
            return int(request.session["org_id"])
    except Exception:
        pass

    # 3) env
    for key in ("ORG_ID", "DEFAULT_ORG_ID"):
        v = os.getenv(key)
        if v and v.isdigit():
            oid = int(v)
            request.session["org_id"] = oid
            return oid

    # 4) only org in DB?
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    orgs = db.list_orgs()
    if len(orgs) == 1:
        oid = int(orgs[0]["id"])
        request.session["org_id"] = oid
        return oid

    # No selection possible
    raise HTTPException(
        status_code=400,
        detail="No organisation selected. Visit /orgs/select to pick one."
    )

def get_org_name(db: "DB", org_id: int) -> str:
    with db._conn() as conn:
        cur = conn.execute("SELECT name FROM orgs WHERE id=?", (int(org_id),))
        row = cur.fetchone()
        return row["name"] if row else f"Org {org_id}"

@app.get("/orgs/switch")
def switch_org(request: Request, org_id: int = Query(...), next: str = Query("/summaries")):
    request.session["org_id"] = int(org_id)
    return RedirectResponse(url=next, status_code=303)

@app.get("/orgs/select")
def select_org_page(request: Request):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    orgs = db.list_orgs()
    # super-simple chooser page (no separate template required)
    html = ["<h1>Select organisation</h1><ul>"]
    for o in orgs:
        html.append(
            f'<li><a href="/orgs/switch?org_id={o["id"]}&next=/summaries">{o["name"]}</a></li>'
        )
    html.append("</ul>")
    return HTMLResponse("".join(html))

# --- /saved page (list saved items, optional folder filter) ---
@app.get("/saved")
def saved_page(request: Request, folder_id: int | None = Query(default=None)):
    user_email = current_user_email(request)

    folders = _sql_all("""
      SELECT f.id, f.name,
             (SELECT COUNT(*) FROM saved_items si
              WHERE si.user_email = f.user_email AND si.folder_id = f.id) AS count
      FROM folders f
      WHERE f.user_email = ?
      ORDER BY LOWER(f.name)
    """, (user_email,))

    rows = _sql_all("""
      SELECT si.item_guid, si.folder_id, si.created_at, f.name AS folder
      FROM saved_items si
      LEFT JOIN folders f ON f.id = si.folder_id
      WHERE si.user_email = ? AND (? IS NULL OR si.folder_id = ?)
      ORDER BY datetime(si.created_at) DESC
    """, (user_email, folder_id, folder_id))

    items = []
    by_guid = {str(e.get("guid") or ""): e for e in db.list_items(limit=20000) if e.get("guid")}
    for r in rows:
        e = by_guid.get(str(r["item_guid"])) or {}
        items.append({
            "guid": r["item_guid"],
            "title": e.get("title") or "(item unavailable)",
            "link": e.get("link") or "",
            "published_at": e.get("published_at") or "",
            "folder": r.get("folder") or None,
        })

    active_folder_name = None
    if folder_id is not None:
        row = _sql_one("SELECT name FROM folders WHERE id = ? AND user_email = ?", (folder_id, user_email))
        active_folder_name = row["name"] if row else None

    return templates.TemplateResponse("saved.html", {
        "request": request,
        "items": items,
        "folders": folders,
        "active_folder": folder_id,
        "active_folder_name": active_folder_name,
    })



# --- Create a folder (form POST from the sidebar) ---
@app.post("/folders/new")
async def folders_new(request: Request, name: str = Form(...)):
    user_email = current_user_email(request)
    name = (name or "").strip()
    if not name:
        return RedirectResponse("/saved?error=Folder%20name%20required", status_code=303)

    try:
        _sql_exec(
            "INSERT INTO folders (user_email, name, created_at) VALUES (?, ?, datetime('now'))",
            (user_email, name)
        )
    except Exception:
        # If unique per-user constraint exists, ignore dup nicely
        return RedirectResponse("/saved?error=Folder%20already%20exists", status_code=303)

    return RedirectResponse("/saved", status_code=303)


# --- Remove a saved item (called by saved.html JS) ---
@app.delete("/api/unsave")
def api_unsave(request: Request, guid: str = Query(...)):
    user_email = current_user_email(request)
    guid = (guid or "").strip()
    if not guid:
        raise HTTPException(400, "guid required")

    _sql_exec("DELETE FROM saved_items WHERE user_email = ? AND item_guid = ?", (user_email, guid))
    return {"ok": True}


# --- folders ---
# ---------- FOLDERS ----------
@app.get("/api/folders")
def api_list_folders(request: Request):
    user = current_user_email(request)
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    with db._conn() as conn:
        cur = conn.execute(
            "SELECT id, name, created_at FROM folders WHERE user_email=? ORDER BY name COLLATE NOCASE",
            (user,),
        )
        return JSONResponse([dict(r) for r in cur.fetchall()])

@app.post("/api/folders")
async def api_create_folder(request: Request):
    user = current_user_email(request)
    data = await request.json()
    name = (data.get("name") or "").strip()
    if not name:
        return JSONResponse({"detail": "Folder name required"}, status_code=400)

    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    now = datetime.now(timezone.utc).isoformat()
    with db._conn() as conn:
        try:
            conn.execute(
                "INSERT INTO folders (user_email, name, created_at) VALUES (?,?,?)",
                (user, name, now),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            # unique on (user_email, name)
            pass
        cur = conn.execute(
            "SELECT id, name, created_at FROM folders WHERE user_email=? AND name=?",
            (user, name),
        )
        row = cur.fetchone()
    return JSONResponse(dict(row))

# ---------- SAVE / UNSAVE ITEMS ----------
# FIXED: POST /api/items/save
@app.post("/api/items/save")
async def api_save_item(request: Request):
    user_email = current_user_email(request)
    data = await request.json()
    item_guid = (data.get("guid") or "").strip()
    folder_id = data.get("folder_id")

    if not item_guid:
        return JSONResponse({"detail": "guid required"}, status_code=400)
    try:
        folder_id = int(folder_id) if folder_id not in (None, "", "null") else None
    except (TypeError, ValueError):
        folder_id = None

    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    now = datetime.now(timezone.utc).isoformat()
    with db._conn() as conn:
        # folder must belong to this user (schema: folders.user_email)
        if folder_id is not None:
            cur = conn.execute(
                "SELECT 1 FROM folders WHERE id = ? AND user_email = ?",
                (folder_id, user_email),
            )
            if cur.fetchone() is None:
                return JSONResponse({"detail": "Folder not found"}, status_code=404)

        # upsert by (user_email, item_guid)
        conn.execute(
            """
            INSERT INTO saved_items (user_email, item_guid, folder_id, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_email, item_guid)
            DO UPDATE SET folder_id = excluded.folder_id
            """,
            (user_email, item_guid, folder_id, now),
        )
        conn.commit()
    return JSONResponse({"ok": True})


# --- tag / untag ---
def _tag_item_to_site_or_control(
    user_email: str, item_guid: str, org_id: int, site_id: int | None, org_control_id: int | None
) -> None:
    if not item_guid or not org_id:
        raise HTTPException(400, "Missing guid or org")
    if not site_id and not org_control_id:
        raise HTTPException(400, "Provide site_id or org_control_id")

    # prevent duplicates (the table has UNIQUE index already, this is for friendlier errors)
    existing = _sql_one(
        """SELECT 1 FROM user_item_tags
           WHERE user_email=? AND item_guid=? AND org_id=?
             AND IFNULL(site_id,-1)=IFNULL(?, -1)
             AND IFNULL(org_control_id,-1)=IFNULL(?, -1)""",
        (user_email, item_guid, org_id, site_id, org_control_id),
    )
    if existing:
        raise HTTPException(409, "Already tagged")

    _sql_exec(
        "INSERT INTO user_item_tags (user_email, item_guid, org_id, site_id, org_control_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, datetime('now'))",
        (user_email, item_guid, org_id, site_id, org_control_id),
    )


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
        try: return int(v)
        except (TypeError, ValueError): return None

    site_id = _to_int_or_none(site_id)
    org_control_id = _to_int_or_none(org_control_id)

    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    with db._conn() as conn:
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

# Provide options for the picker (sites + org controls)
@app.get("/orgs/{org_id}/tag-options")
def api_tag_options(request: Request, org_id: int):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    sites = db.list_sites(org_id)
    ctrls = db.list_all_controls_for_org(org_id)
    return {"sites": sites, "controls": ctrls}


# --- Sites Index ------------------------------------------------------------
@app.get("/orgs/{org_id}/sites")
def list_sites_page(request: Request, org_id: int):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    orgs = db.list_orgs()
    org = next((o for o in orgs if o["id"] == org_id), None)
    sites = db.list_sites(org_id)
    return templates.TemplateResponse(
        "sites.html",
        {
            "request": request,
            "uid": request.session.get("uid"),
            "org": org,
            "org_id": org_id,
            "sites": sites,
        },
    )
from fastapi import Form

@app.post("/orgs/create")
def create_org(request: Request, name: str = Form(...)):
    # Temporary stand-in for logged-in user
    current_user = "andrewpeat"

    db.create_org(name, user=current_user)
    return RedirectResponse(url="/orgs", status_code=303)

# --- Organisation & Site overview pages --------------------------------------
from fastapi import Path
from fastapi.responses import HTMLResponse

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
    # Expected columns (adjust to your schema):
    # orgs: id, name, phone, email, head_office_address, website
    row = _sql_one_safe("""
        SELECT id, name,
               COALESCE(phone,'') AS phone,
               COALESCE(email,'') AS email,
               COALESCE(head_office_address,'') AS head_office_address,
               COALESCE(website,'') AS website
        FROM orgs WHERE id = ?
    """, (org_id,))
    return row or {"id": org_id, "name": f"Organisation {org_id}",
                   "phone": "", "email": "", "head_office_address": "", "website": ""}

def _org_sites(org_id: int):
    # sites: id, org_id, name, address, phone, email
    return _sql_all_safe("""
        SELECT id, name,
               COALESCE(address,'') AS address,
               COALESCE(phone,'') AS phone,
               COALESCE(email,'') AS email
        FROM sites WHERE org_id = ?
        ORDER BY LOWER(name)
    """, (org_id,))

def _org_personnel(org_id: int):
    # org_members: id, org_id, name, role, email, is_key_personnel(INT 0/1), is_ultimate_risk_owner(INT 0/1)
    rows = _sql_all_safe("""
        SELECT id, name, COALESCE(role,'') AS role, COALESCE(email,'') AS email,
               COALESCE(is_key_personnel,0) AS is_key_personnel,
               COALESCE(is_ultimate_risk_owner,0) AS is_ultimate_risk_owner
        FROM org_members
        WHERE org_id = ?
        ORDER BY is_ultimate_risk_owner DESC, is_key_personnel DESC, LOWER(name)
    """, (org_id,))
    uro = next((r for r in rows if int(r.get("is_ultimate_risk_owner", 0)) == 1), None)
    keys = [r for r in rows if int(r.get("is_key_personnel", 0)) == 1 or r is uro]
    return keys, uro

def _org_counts(org_id: int):
    # org_controls(id, org_id, ...)
    c_org = _sql_one_safe("SELECT COUNT(*) AS n FROM org_controls WHERE org_id = ?", (org_id,)) or {"n": 0}
    # org_risks(id, org_id, status, ...)
    r_org = _sql_one_safe("SELECT COUNT(*) AS n FROM org_risks WHERE org_id = ?", (org_id,)) or {"n": 0}
    # site counts
    c_site = _sql_one_safe("""
        SELECT COUNT(*) AS n FROM site_controls sc
        JOIN sites s ON s.id = sc.site_id
        WHERE s.org_id = ?
    """, (org_id,)) or {"n": 0}
    r_site = _sql_one_safe("""
        SELECT COUNT(*) AS n FROM site_risks sr
        JOIN sites s ON s.id = sr.site_id
        WHERE s.org_id = ?
    """, (org_id,)) or {"n": 0}
    return {
        "org_controls": c_org["n"],
        "org_risks": r_org["n"],
        "site_controls": c_site["n"],
        "site_risks": r_site["n"],
    }

from fastapi import Form
from fastapi.responses import HTMLResponse, RedirectResponse

def _org_name(org_id:int):
    return (_sql_one("SELECT id,name FROM orgs WHERE id=?", (org_id,)) or {"id":org_id,"name":f"Organisation {org_id}"})

@app.get("/orgs/{org_id}/members", response_class=HTMLResponse)
def org_members_page(request: Request, org_id: int):
    org = _org_name(org_id)
    rows = _sql_all("""
        SELECT id, name, COALESCE(role,'') AS role, COALESCE(email,'') AS email,
               COALESCE(is_key_personnel,0) AS is_key_personnel,
               COALESCE(is_ultimate_risk_owner,0) AS is_ultimate_risk_owner
        FROM org_members
        WHERE org_id=?
        ORDER BY is_ultimate_risk_owner DESC, is_key_personnel DESC, LOWER(name)
    """, (org_id,))
    return templates.TemplateResponse("org_members.html", {
        "request": request,
        "org": org,
        "org_id": org_id,
        "members": rows,
    })

@app.post("/orgs/{org_id}/members/new")
def org_member_create(
    request: Request,
    org_id: int,
    name: str = Form(...),
    role: str = Form(""),
    email: str = Form(""),
    is_key_personnel: int = Form(0),
    is_ultimate_risk_owner: int = Form(0),
):
    _sql_exec("""
        INSERT INTO org_members (org_id, name, role, email, is_key_personnel, is_ultimate_risk_owner)
        VALUES (?,?,?,?,?,?)
    """, (org_id, name.strip(), role.strip(), email.strip(), int(bool(is_key_personnel)), int(bool(is_ultimate_risk_owner))))
    return RedirectResponse(url=f"/orgs/{org_id}/members", status_code=302)

@app.post("/orgs/{org_id}/members/{member_id}/update")
def org_member_update(
    request: Request,
    org_id: int, member_id: int,
    name: str = Form(...),
    role: str = Form(""),
    email: str = Form("")
):
    _sql_exec("""
        UPDATE org_members SET name=?, role=?, email=? WHERE id=? AND org_id=?
    """, (name.strip(), role.strip(), email.strip(), member_id, org_id))
    return RedirectResponse(url=f"/orgs/{org_id}/members", status_code=302)

@app.post("/orgs/{org_id}/members/{member_id}/toggle")
def org_member_toggle(request: Request, org_id: int, member_id: int, field: str = Form(...)):
    if field not in ("is_key_personnel","is_ultimate_risk_owner"):
        raise HTTPException(400, "Invalid field")
    _sql_exec(f"""
        UPDATE org_members
        SET {field} = CASE COALESCE({field},0) WHEN 1 THEN 0 ELSE 1 END
        WHERE id=? AND org_id=?
    """, (member_id, org_id))
    return RedirectResponse(url=f"/orgs/{org_id}/members", status_code=302)

@app.post("/orgs/{org_id}/members/{member_id}/delete")
def org_member_delete(request: Request, org_id: int, member_id: int):
    _sql_exec("DELETE FROM org_members WHERE id=? AND org_id=?", (member_id, org_id))
    return RedirectResponse(url=f"/orgs/{org_id}/members", status_code=302)


from fastapi import Form

def _site_columns():
    rows = _sql_all("PRAGMA table_info(sites)")
    # returns set like {'id','org_id','name','address','phone','email','code','city',...}
    return {r["name"] for r in rows} if rows else set()

@app.get("/orgs/{org_id}/sites/{site_id}/edit", response_class=HTMLResponse)
def site_edit_form(request: Request, org_id: int, site_id: int):
    site = _sql_one("SELECT * FROM sites WHERE id=? AND org_id=?", (site_id, org_id))
    if not site:
        raise HTTPException(404, "Site not found")
    org  = _sql_one("SELECT id, name FROM orgs WHERE id=?", (org_id,)) or {"id": org_id, "name": f"Organisation {org_id}"}

    cols = _site_columns()
    # flags for the template to only show fields that exist
    field_flags = {
        "code": "code" in cols,
        "city": "city" in cols,
        "address": "address" in cols,
        "phone": "phone" in cols,
        "email": "email" in cols,
    }

    return templates.TemplateResponse("site_edit.html", {
        "request": request,
        "org": org,
        "site": site,
        "org_id": org_id,
        "site_id": site_id,
        "fields": field_flags,
    })

@app.post("/orgs/{org_id}/sites/{site_id}/edit", response_class=HTMLResponse)
def site_edit_save(
    request: Request,
    org_id: int,
    site_id: int,
    # Form fields (all optional; we only update those present + existing in DB)
    name: str = Form(None),
    code: str = Form(None),
    city: str = Form(None),
    address: str = Form(None),
    phone: str = Form(None),
    email: str = Form(None),
):
    # ensure site exists and belongs to org
    site = _sql_one("SELECT id FROM sites WHERE id=? AND org_id=?", (site_id, org_id))
    if not site:
        raise HTTPException(404, "Site not found")

    cols = _site_columns()

    # collect desired updates, but only keep keys that actually exist in the table
    updates = {}
    if name is not None:    updates["name"] = name
    if code is not None:    updates["code"] = code
    if city is not None:    updates["city"] = city
    if address is not None: updates["address"] = address
    if phone is not None:   updates["phone"] = phone
    if email is not None:   updates["email"] = email

    # intersect with real columns to avoid "no such column" errors
    updates = {k: v for k, v in updates.items() if k in cols}

    if updates:
        set_sql = ", ".join([f"{k}=?" for k in updates.keys()])
        params = list(updates.values()) + [site_id]
        _sql_exec(f"UPDATE sites SET {set_sql} WHERE id=?", tuple(params))

    # optional: touch updated_at if present
    if "updated_at" in cols:
        _sql_exec("UPDATE sites SET updated_at=datetime('now') WHERE id=?", (site_id,))

    # After save, redirect back to site page (or org overview)
    return RedirectResponse(url=f"/orgs/{org_id}/sites/{site_id}", status_code=302)


def _site_basic(site_id: int):
    # sites: id, org_id, name, address, phone, email
    return _sql_one_safe("""
        SELECT s.id, s.org_id, s.name,
               COALESCE(s.address,'') AS address,
               COALESCE(s.phone,'') AS phone,
               COALESCE(s.email,'') AS email
        FROM sites s WHERE s.id = ?
    """, (site_id,)) or {"id": site_id, "org_id": None, "name": f"Site {site_id}",
                         "address": "", "phone": "", "email": ""}

def _site_personnel(site_id: int):
    # site_members: id, site_id, name, role, email, is_key_personnel
    return _sql_all_safe("""
        SELECT id, name, COALESCE(role,'') AS role, COALESCE(email,'') AS email,
               COALESCE(is_key_personnel,0) AS is_key_personnel
        FROM site_members
        WHERE site_id = ?
        ORDER BY is_key_personnel DESC, LOWER(name)
    """, (site_id,))

def _site_counts(site_id: int):
    c = _sql_one_safe("SELECT COUNT(*) AS n FROM site_controls WHERE site_id = ?", (site_id,)) or {"n": 0}
    r = _sql_one_safe("SELECT COUNT(*) AS n FROM site_risks WHERE site_id = ?", (site_id,)) or {"n": 0}
    return {"controls": c["n"], "risks": r["n"]}

@app.get("/orgs/{org_id}", response_class=HTMLResponse)
def org_overview_page(request: Request, org_id: int = Path(...)):
    org = _org_basic(org_id)
    sites = _org_sites(org_id)
    key_people, uro = _org_personnel(org_id)
    counts = _org_counts(org_id)
    return templates.TemplateResponse("org_overview.html", {
        "request": request,
        "org": org,
        "sites": sites,
        "key_people": key_people,
        "uro": uro,
        "counts": counts,
        # Useful links used by the template
        "org_id": org_id,
    })

@app.get("/orgs/{org_id}/sites/{site_id}", response_class=HTMLResponse)
def site_overview_page(request: Request, org_id: int = Path(...), site_id: int = Path(...)):
    site = _site_basic(site_id)
    # guard mismatch (wrong org in url)
    if site and site.get("org_id") not in (None, org_id):
        raise HTTPException(404, "Site not in organisation")
    people = _site_personnel(site_id)
    counts = _site_counts(site_id)
    # breadcrumb org name (fallback to id)
    org = _org_basic(org_id)
    return templates.TemplateResponse("site_overview.html", {
        "request": request,
        "org": org,
        "site": site,
        "people": people,
        "counts": counts,
        "org_id": org_id,
        "site_id": site_id,
    })
@app.get("/orgs/{org_id}/org-risks", response_class=HTMLResponse)
def org_risks_page(
    request: Request,
    org_id: int,
    status: str | None = Query(None),
    severity: str | None = Query(None),
    category: str | None = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200)
):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))

    # 1️⃣  Basic organisation details
    org = _sql_one("""
        SELECT id, name, head_office_address, phone, email
        FROM orgs WHERE id = ?
    """, (org_id,)) or {"id": org_id, "name": f"Organisation {org_id}"}

    # 2️⃣  Build filtering WHERE clause
    filters = ["org_id = ?"]
    params = [org_id]
    if status:
        filters.append("LOWER(status) = LOWER(?)")
        params.append(status)
    if severity:
        filters.append("LOWER(severity) = LOWER(?)")
        params.append(severity)
    if category:
        filters.append("LOWER(category) = LOWER(?)")
        params.append(category)
    where_clause = "WHERE " + " AND ".join(filters)

    # 3️⃣  Count total
    total_row = _sql_one(f"SELECT COUNT(*) AS n FROM org_risks {where_clause}", tuple(params))
    total = total_row["n"] if total_row else 0

    # 4️⃣  Pagination
    offset = (page - 1) * per_page

    # 5️⃣  Query actual risks
    risks = _sql_all(f"""
        SELECT id, code, title, status, severity, category,
               owner_name, owner_email, updated_at, created_at
        FROM org_risks
        {where_clause}
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, tuple(params + [per_page, offset]))

    # 6️⃣  Attach control counts
    for r in risks:
        row = _sql_one("SELECT COUNT(*) AS n FROM org_controls_risks WHERE org_risk_id = ?", (r["id"],))
        r["controls_count"] = row["n"] if row else 0

    # 7️⃣  Pagination helpers for template
    total_pages = max(1, (total + per_page - 1) // per_page)
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    # 8️⃣  Drop-down filter choices (optional static lists)
    status_choices = ["Open", "In progress", "Mitigated", "Closed"]
    severity_choices = ["Low", "Medium", "High", "Severe"]
    category_choices = sorted({r.get("category") for r in risks if r.get("category")})

    # 9️⃣  Return the template with all context
    return templates.TemplateResponse("org_risks.html", {
        "request": request,
        "org": org,
        "org_id": org_id,
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
    })


# --- Add a new site ---------------------------------------------------------
@app.post("/orgs/{org_id}/sites/new")
def create_site(
    request: Request,
    org_id: int,
    name: str = Form(...),
    code: str | None = Form(None),
    location: str | None = Form(None),
):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    if not name.strip():
        return JSONResponse({"detail": "Name required"}, status_code=400)

    user = current_user(request)
    db.upsert_site(org_id, name, code, location, created_by=user)

    return RedirectResponse(url=f"/orgs/{org_id}/sites", status_code=303)


@app.get("/orgs/{org_id}")
def org_root_redirect(org_id: int):
    # convenience: /orgs/1 -> /orgs/1/controls
    return RedirectResponse(url=f"/orgs/{org_id}/controls", status_code=307)

@app.get("/orgs/{org_id}/controls")
def org_controls_page(request: Request, org_id: int, site: Optional[int] = None):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    orgs = db.list_orgs()
    org = next((o for o in orgs if o["id"] == org_id), None)
    sites = db.list_sites(org_id)

    if site:
        controls = db.list_org_controls(org_id, site_id=site)
        current_site = next((s for s in sites if s["id"] == site), None)
    else:
        controls = db.list_org_controls(org_id)
        current_site = None

    grouped = {}
    for c in controls:
        group = c.get("site_name") or (current_site["name"] if current_site else "Org-wide")
        grouped.setdefault(group, []).append(c)

    return templates.TemplateResponse(
        "org_controls.html",
        {
            "request": request,
            "uid": request.session.get("uid"),
            "org": org,
            "org_id": org_id,
            "sites": sites,
            "grouped": grouped,
            "current_site": current_site,
        },
    )
from fastapi import Request, Form
from fastapi.responses import RedirectResponse, JSONResponse
import os
from storage.db import DB

# simple placeholder until auth is wired
def current_user(request: Request) -> str:
    return (getattr(request, "session", {}).get("user")
            or os.getenv("DEV_USER")
            or "andrewpeat")

@app.get("/orgs/{org_id}/controls/new")
def org_control_new_page(request: Request, org_id: int, site_id: int | None = None):
    return templates.TemplateResponse(
        "org_control_new.html",
        {"request": request, "org_id": org_id, "site_id": site_id}
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
    review_frequency_days: str | None = Form(None),
    next_review_at: str | None = Form(None),
    site_id: int | None = Form(None),
):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    if not title.strip():
        return JSONResponse({"detail": "Title required"}, status_code=400)

    # ✅ Safely handle empty or invalid input for review_frequency_days
    rfd = None
    if review_frequency_days:
        try:
            rfd = int(review_frequency_days)
        except ValueError:
            rfd = None

    user = current_user(request)
    db.upsert_org_control(
        org_id=org_id,
        site_id=site_id,
        code=code,
        title=title,
        description=description,
        owner_email=owner_email,
        tags=tags,  # can be CSV or JSON array; db.py normalises it
        status=status,
        risk=risk,
        review_frequency_days=rfd,  # 👈 use the safely parsed int
        next_review_at=next_review_at,
        created_by=user,
    )

    return RedirectResponse(url=f"/orgs/{org_id}/controls", status_code=303)


@app.post("/orgs/{org_id}/sites/new")
def create_site(
    request: Request,
    org_id: int,
    name: str = Form(...),
    code: str | None = Form(None),
    location: str | None = Form(None),
):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    if not name.strip():
        return JSONResponse({"detail": "Name is required"}, status_code=400)
    db.upsert_site(org_id, name, code, location)
    return RedirectResponse(url=f"/orgs/{org_id}/controls", status_code=303)

@app.get("/orgs/{org_id}/sites/new")
def new_site_form(request: Request, org_id: int):
    return templates.TemplateResponse("site_new.html", {"request": request, "org_id": org_id, "uid": request.session.get("uid")})


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
        query_items: List[tuple[str, str]] = []
        for k, v in params.items():
            if v is None or v == "":
                continue
            if k in ("sources", "topics") and isinstance(v, list):
                for val in v:
                    query_items.append((k, str(val)))
            else:
                query_items.append((k, str(v)))
        query = urlencode(query_items, doseq=True)
        return RedirectResponse(url=f"/summaries?{query}")
    except Exception:
        raise HTTPException(status_code=400, detail="Saved filter has invalid params")

 # from the helper we made earlier

router = APIRouter()
db = DB(os.getenv("DB_PATH", "ofgem.db"))

@app.get("/controls", response_class=HTMLResponse)
def controls_page(request: Request):
    db = DB()
    all_controls = db.list_controls()
    return templates.TemplateResponse(
        "controls.html",
        {
            "request": request,
            "uid": request.session.get("uid"),
            "controls": all_controls,
            "org_id": 1,   # TODO: select the current org; 1 as MVP default
        },
    )

@router.get("/controls/{cid}", response_class=HTMLResponse)
def control_detail(request: Request, cid: int):
    # items linked via the view
    items = db.list_items_for_org_control(cid, limit=100)
    # current org control
    oc = [r for r in db.list_org_controls(1) if r["id"]==cid]
    return templates.TemplateResponse("control_detail.html", {"request": request, "control": oc[0] if oc else None, "items": items})

app.include_router(router)

@router.post("/send", response_class=HTMLResponse)
async def send_article_fragment(guid: str = Form(...), email: str = Form(...)):
    # find the item
    items = [i for i in db.list_items(limit=5000) if i.get("guid") == guid]
    if not items:
        return HTMLResponse(
            "<p class='muted' style='color:#b00;'>❌ Article not found.</p>",
            status_code=404
        )

    item = items[0]
    ok = send_article_email(email, item)

    if ok:
        return HTMLResponse(
            f"<p class='muted'>✅ Sent to <strong>{email}</strong></p>",
            status_code=200
        )
    else:
        return HTMLResponse(
            "<p class='muted' style='color:#b00;'>❌ Failed to send. Please try again.</p>",
            status_code=502
        )

# register the router (if not already)
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
    """Initialise and return an OpenAI client, with debug logging."""
    try:
        from openai import OpenAI
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            print("[AI] ⚠️ No OPENAI_API_KEY found in environment")
            return None
        print("[AI] ✅ OpenAI API key found, creating client")
        return OpenAI(api_key=key)
    except Exception as e:
        print(f"[AI] ❌ Failed to create OpenAI client: {e}")
        return None


# --- Boilerplate cleaner for extracted text ---
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
    """Detects junk/boilerplate summaries that shouldn't be cached."""
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


def _generate_ai_summary(title: str, text: str, limit_words: int = 100, guid: str | None = None) -> str:
    print(f"[AI] 🔎 Generating summary guid={guid} title={title[:60]!r} len={len(text)}")
    """Generate an AI summary using OpenAI; falls back if not available."""
    text = (text or "").strip()
    if not text:
        print("[AI] ⚠️ No text provided to summarise.")
        return "No content available to summarise."

    print(f"[AI] 🔎 Generating summary for: {title[:60]!r} ({len(text)} chars)")
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
                {"role": "system", "content": "You are a precise UK energy regulation analyst."},
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
    except Exception as e:
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
            break  # 15 MB cap
        chunks.append(chunk)
    return b"".join(chunks)

def _pdf_bytes_to_text_pypdf(blob: bytes, max_pages: int = 8) -> str:
    from pypdf import PdfReader
    try:
        reader = PdfReader(io.BytesIO(blob))
        out = []
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

@app.post("/api/ai-summary")
def ai_summary(req: AISummaryReq):
    item = _find_item_by_guid(req.guid)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    # ✅ 1️⃣ Return cached summary if present
    if item.get("ai_summary"):
        print(f"[AI] ♻️ Using cached summary for {req.guid}")
        return JSONResponse({"ok": True, "summary": item["ai_summary"]})

    title = (item.get("title") or "").strip()
    link = (item.get("link") or "").strip()
    text = (item.get("content") or item.get("summary") or "").strip()


    title = (item.get("title") or "").strip()
    link  = (item.get("link")  or "").strip()
    text  = (item.get("content") or item.get("summary") or "").strip()

    # 1) Serve from cache if available
    cached = _get_cached_ai_summary(req.guid)
    if cached:
        return JSONResponse({"ok": True, "summary": cached, "cached": True})

    # 2) Get best source text: fetch PDF if needed
    wants_pdf = ("[PDF document" in text) or _is_pdf_link(link)
    if wants_pdf and link:
        try:
            blob = _fetch_pdf_bytes(link)
            extracted = _pdf_bytes_to_text_pypdf(blob, max_pages=8)
            if extracted:
                text = extracted
            else:
                # No text in PDF — return a helpful message but don't cache this
                return JSONResponse({
                    "ok": True,
                    "summary": "This PDF appears to be image-based or has no extractable text. Please open the document to view."
                })
        except Exception:
            # Network or parsing issue — again, don't cache this
            return JSONResponse({
                "ok": True,
                "summary": "Could not fetch or parse the PDF for summary. Please open the document to view."
            })

    if not text:
        return JSONResponse({"ok": True, "summary": "No content available to summarise."})

    # Clean boilerplate to avoid weird menu text in the summary
    text = _clean_extracted_text(title, text)

    # 3) Generate via OpenAI; if it fails (e.g. 429), fall back without caching
    summary = _generate_ai_summary(title, text, limit_words=120)

    # 🔒 skip caching obviously junk content
    if not _is_boilerplate_summary(summary):
        try:
            _sql_exec("UPDATE items SET ai_summary = ? WHERE guid = ?", (summary, req.guid))
        except Exception as e:
            print(f"[AI] ⚠️ Failed to cache summary: {e}")

    # Heuristic: if we got the fallback (short or clearly not model output), we still cache it
    # so repeated clicks don't keep hitting the API — but you can choose not to cache fallbacks.
    try:
        _set_cached_ai_summary(req.guid, summary)
    except Exception as e:
        # Non-fatal: just log; don’t block the response
        print("[AI] ⚠️ Failed to cache summary:", e)

    return JSONResponse({"ok": True, "summary": summary, "cached": False})

# ---------------------------------------------------------------------------
# Auth helpers & routes
# ---------------------------------------------------------------------------
def get_user_id(request: Request) -> int | None:
    try:
        return request.session.get("uid")
    except Exception:
        return None

def require_user(request: Request) -> int:
    uid = get_user_id(request)
    if not uid:
        raise HTTPException(401, "Login required")
    return uid

# Aliases
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


# Account actions (POST)
@app.post("/account/login")
def account_login_post(request: Request, email: str = Form(...), password: str = Form(...)):
    user = _get_user_by_email(email)
    if not user or not pwd_ctx.verify(password, user["password_hash"]):
        return render(request, "account/login.html", {"error": "Invalid credentials"})
    request.session["uid"] = user["id"]
    request.session["last_activity"] = datetime.now(timezone.utc).isoformat()   # ← add this
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
    request.session["last_activity"] = datetime.now(timezone.utc).isoformat()   # ← add this
    return RedirectResponse(url="/summaries", status_code=302)

@app.post("/account/logout")
def account_logout_post(request: Request):
    request.session.clear()
    return RedirectResponse(url="/account/login", status_code=302)


# ---------------------------------------------------------------------------
# Folders & Saved items
# ---------------------------------------------------------------------------
class FolderIn(BaseModel):
    name: str

@app.get("/api/folders")
def api_folders(request: Request):
    uid = require_user(request)
    return {"folders": _list_folders(uid)}

@app.post("/api/folders")
def api_create_folder(request: Request, payload: FolderIn):
    uid = require_user(request)
    fid = _create_folder(uid, payload.name)
    return {"ok": True, "id": fid}

class SaveIn(BaseModel):
    guid: str
    folder_id: int | None = None

# HTML form to create a folder from /saved sidebar
# Make sure you have:
# from fastapi import Form, HTTPException, Query, Body
# from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

# Pydantic model for /api/save
class SaveIn(BaseModel):
    guid: str
    folder_id: int | None = None

# --- Create folder (form POST from saved.html sidebar) ---
@app.post("/folders/new")
async def folders_new(request: Request, name: str = Form(...)):
    user_email = require_user(request)  # must be an email string
    try:
        _create_folder(user_email, name)  # helper must use folders.user_email
        return RedirectResponse(url="/saved", status_code=303)
    except HTTPException as e:
        folders = _list_folders(user_email)             # uses user_email
        items = _list_saved_items(user_email, None)     # uses user_email + item_guid
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

# --- Save (called from summaries.html) ---
@app.post("/api/save")
def api_save(request: Request, payload: SaveIn):
    user_email = require_user(request)
    _save_item(user_email, payload.guid, payload.folder_id)  # must use (user_email, item_guid)
    return {"ok": True}

# --- Unsave (called from saved.html JS: DELETE /api/unsave?guid=...) ---
@app.api_route("/api/unsave", methods=["DELETE", "POST"])
def api_unsave(request: Request, guid: str | None = Query(None), payload: Dict[str, Any] | None = Body(None)):
    user_email = require_user(request)
    if not guid and payload and "guid" in payload:
        guid = str(payload["guid"])
    if not guid:
        raise HTTPException(400, "Missing guid")
    _unsave_item(user_email, guid)  # must delete by (user_email, item_guid)
    return {"ok": True}

# --- Saved page (lists saved items; optional ?folder_id=) ---
@app.get("/saved", response_class=HTMLResponse)
def saved_page(request: Request, folder_id: int | None = Query(default=None)):
    user_email = require_user(request)

    folders = _list_folders(user_email)

    items = _list_saved_items(user_email, folder_id=folder_id)
    # saved.html expects each item dict to include: guid, link, title, published_at, folder (name)

    active_folder_name = None
    if folder_id is not None:
        # FIXED: use user_email (NOT user_id)
        row = _sql_one("SELECT name FROM folders WHERE id = ? AND user_email = ?", (folder_id, user_email))
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

# Explicit by ID (already in your code)
@app.get("/api/orgs/{org_id}/sites")
def api_org_sites(org_id: int):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    with db._conn() as conn:
        cur = conn.execute("SELECT id, name FROM sites WHERE org_id=? ORDER BY name", (int(org_id),))
        return JSONResponse([{"id": r["id"], "name": r["name"]} for r in cur.fetchall()])

@app.get("/api/orgs/{org_id}/org-controls")
def api_org_controls(org_id: int):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    with db._conn() as conn:
        cur = conn.execute(
            """SELECT oc.id, oc.title, oc.code, oc.site_id, s.name AS site_name
               FROM org_controls oc
               LEFT JOIN sites s ON s.id = oc.site_id
               WHERE oc.org_id=?
               ORDER BY COALESCE(s.name,''), COALESCE(oc.code,''), oc.title""",
            (int(org_id),),
        )
        rows = [{
            "id": r["id"], "title": r["title"], "code": r["code"],
            "site_id": r["site_id"], "site_name": r["site_name"]
        } for r in cur.fetchall()]
        return JSONResponse(rows)

# Session-aware "current" variants (no org_id in path)
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

    # org_id: allow omission → resolve from session/query/env/DB
    org_id = data.get("org_id")
    if org_id in (None, "", "null"):
        try:
            org_id = resolve_org_id(request)
        except HTTPException as hx:
            # bubble up a clear error if no org can be resolved
            return JSONResponse({"detail": hx.detail}, status_code=hx.status_code)
    try:
        org_id = int(org_id)
    except (TypeError, ValueError):
        return JSONResponse({"detail": "org_id must be an integer"}, status_code=400)

    # Normalise optionals (empty string / null → None; else int)
    def _to_int_or_none(v):
        if v in (None, "", "null"):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    site_id = _to_int_or_none(data.get("site_id"))
    org_control_id = _to_int_or_none(data.get("org_control_id"))

    db = DB(os.getenv("DB_PATH", "ofgem.db"))
    now = datetime.now(timezone.utc).isoformat()

    with db._conn() as conn:
        # --- Guard rails: site/control must belong to this org ----------------
        if site_id is not None:
            cur = conn.execute("SELECT 1 FROM sites WHERE id=? AND org_id=?", (site_id, org_id))
            if cur.fetchone() is None:
                return JSONResponse({"detail": "site_id does not belong to this org"}, status_code=400)

        if org_control_id is not None:
            cur = conn.execute("SELECT 1 FROM org_controls WHERE id=? AND org_id=?", (org_control_id, org_id))
            if cur.fetchone() is None:
                return JSONResponse({"detail": "org_control_id does not belong to this org"}, status_code=400)

        # --- Upsert unique tag (user_email, item_guid, org_id, site_id?, control?) ---
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
                """,
                (user, guid, org_id, site_id, org_control_id, now),
            )
            conn.commit()

        # --- Return current tags for this user+item to simplify UI refresh ----
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
            tags.append({
                "id": r["id"],
                "org_id": r["org_id"],
                "site_id": r["site_id"],
                "site_name": r["site_name"],
                "org_control_id": r["org_control_id"],
                "control_code": r["control_code"],
                "control_title": r["control_title"],
            })

    return JSONResponse({"ok": True, "tags": tags})

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

    risks = _sql_all(f"""
        SELECT id, code, title, status, severity, category,
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
    # collect categories from ALL org risks for this org (not just current page)
    cats = _sql_all("SELECT DISTINCT category FROM org_risks WHERE org_id = ? AND category IS NOT NULL AND category != '' ORDER BY LOWER(category)", (org_id,))
    category_choices = [c["category"] for c in cats]

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
    })
@app.get("/orgs/{org_id}/sites/{site_id}/risks", response_class=HTMLResponse)
def site_risks_page(
    request: Request,
    org_id: int,
    site_id: int,
    status: str | None = Query(None),
    severity: str | None = Query(None),
    category: str | None = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
):
    db = DB(os.getenv("DB_PATH", "ofgem.db"))

    org  = _sql_one("SELECT id, name FROM orgs WHERE id = ?", (org_id,)) or {"id": org_id, "name": f"Organisation {org_id}"}
    site = _sql_one("SELECT id, org_id, name, address, phone, email FROM sites WHERE id = ?", (site_id,))
    if not site or site["org_id"] != org_id:
        raise HTTPException(404, "Site not found in this organisation")

    filters, params = ["site_id = ?"], [site_id]
    if status:
        filters.append("LOWER(status) = LOWER(?)"); params.append(status)
    if severity:
        filters.append("LOWER(severity) = LOWER(?)"); params.append(severity)
    if category:
        filters.append("LOWER(category) = LOWER(?)"); params.append(category)
    where_sql = "WHERE " + " AND ".join(filters)

    total = (_sql_one(f"SELECT COUNT(*) AS n FROM site_risks {where_sql}", tuple(params)) or {"n": 0})["n"]
    offset = (page - 1) * per_page

    risks = _sql_all(f"""
        SELECT id, code, title, status, severity, category,
               owner_name, owner_email, created_at, updated_at
        FROM site_risks
        {where_sql}
        ORDER BY COALESCE(updated_at, created_at) DESC
        LIMIT ? OFFSET ?
    """, tuple(params + [per_page, offset]))

    # control counts at site level
    for r in risks:
        row = _sql_one("SELECT COUNT(*) AS n FROM site_controls_risks WHERE site_risk_id = ?", (r["id"],))
        r["controls_count"] = (row or {"n": 0})["n"]

    total_pages = max(1, (total + per_page - 1) // per_page)
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    status_choices   = ["Open", "In progress", "Mitigated", "Closed"]
    severity_choices = ["Low", "Medium", "High", "Severe"]
    cats = _sql_all("SELECT DISTINCT category FROM site_risks WHERE site_id = ? AND category IS NOT NULL AND category != '' ORDER BY LOWER(category)", (site_id,))
    category_choices = [c["category"] for c in cats]

    return templates.TemplateResponse("site_risks.html", {  # create if you don't have it yet
        "request": request,
        "org": org,
        "site": site,
        "org_id": org_id,
        "site_id": site_id,
        "risks": risks,
        "status_choices": status_choices,
        "severity_choices": severity_choices,
        "category_choices": category_choices,
        "active": {"status": status, "severity": severity, "category": category},
        "page": page, "total_pages": total_pages, "page_numbers": page_numbers,
    })



