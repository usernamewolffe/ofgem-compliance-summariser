# api/server.py
from dotenv import load_dotenv
load_dotenv()

import os, csv, io, json, requests, sqlite3, re
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Set, Dict, Any, Tuple
from urllib.parse import urlparse, urlencode

from fastapi import FastAPI, Request, Query, HTTPException, Form, Body
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel
from jinja2 import TemplateNotFound
from passlib.context import CryptContext

from storage.db import DB

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TOPIC_TAGS = [
    "CAF/NIS", "Cyber", "Incident", "Consultation", "Guidance", "Enforcement", "Penalty"
]

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = (BASE_DIR / "ofgem.db").as_posix()

# ---------------------------------------------------------------------------
# App + plumbing
# ---------------------------------------------------------------------------
app = FastAPI(debug=True)

app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSIONS_SECRET", "dev-secret"),
    same_site="lax",
    https_only=False,
)

# Project DB wrapper
db = DB("ofgem.db")

# Static (optional legacy assets)
app.mount("/static", StaticFiles(directory="api/static", html=True), name="static")

# Templates: summariser/templates/summariser
TEMPLATES_DIR = BASE_DIR / "summariser" / "templates" / "summariser"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Password hashing context — new hashes use PBKDF2-SHA256 (no 72-byte limit).
# We still accept old bcrypt / bcrypt_sha256 hashes on login.
pwd_ctx = CryptContext(
    schemes=["pbkdf2_sha256", "bcrypt_sha256", "bcrypt"],
    default="pbkdf2_sha256",
    deprecated="auto",
)

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

# -----------------------------
# Folder helpers (self-contained)
# -----------------------------
def _list_folders(user_id: int) -> List[dict]:
    return _sql_all(
        "SELECT id, name, created_at FROM folders WHERE user_id = ? ORDER BY LOWER(name)",
        (user_id,),
    )

def _create_folder(user_id: int, name: str) -> int:
    name = (name or "").strip()
    if not name:
        raise HTTPException(400, "Folder name cannot be empty")
    if len(name) > 100:
        raise HTTPException(400, "Folder name is too long (max 100 chars)")
    try:
        _sql_exec("INSERT INTO folders (user_id, name) VALUES (?, ?)", (user_id, name))
    except Exception:
        existing = _sql_one(
            "SELECT id FROM folders WHERE user_id = ? AND name = ?",
            (user_id, name),
        )
        if existing:
            raise HTTPException(409, "Folder already exists")
        raise
    row = _sql_one(
        "SELECT id FROM folders WHERE user_id = ? AND name = ?",
        (user_id, name),
    )
    if not row:
        raise HTTPException(500, "Failed to create folder")
    return int(row["id"])

# -----------------------------
# Saved items helpers (self-contained)
# -----------------------------
def _save_item(user_id: int, guid: str, folder_id: int | None = None) -> None:
    guid = (guid or "").strip()
    if not guid:
        raise HTTPException(400, "Missing guid")

    if folder_id is not None:
        owner = _sql_one("SELECT id FROM folders WHERE id = ? AND user_id = ?", (folder_id, user_id))
        if not owner:
            raise HTTPException(400, "Folder not found")

    _sql_exec(
        "INSERT OR IGNORE INTO saved_items (user_id, guid, folder_id) VALUES (?, ?, ?)",
        (user_id, guid, folder_id),
    )

def _unsave_item(user_id: int, guid: str) -> None:
    _sql_exec("DELETE FROM saved_items WHERE user_id = ? AND guid = ?", (user_id, guid))

def _list_saved_items(user_id: int, folder_id: int | None = None) -> List[dict]:
    rows = _sql_all(
        "SELECT id, guid, folder_id, created_at FROM saved_items "
        "WHERE user_id = ? AND (? IS NULL OR folder_id = ?) "
        "ORDER BY datetime(created_at) DESC",
        (user_id, folder_id, folder_id),
    )

    items = db.list_items(limit=20000)  # existing helper
    by_guid = {str(e.get("guid") or ""): e for e in items if e.get("guid")}
    by_link = {str(e.get("link") or ""): e for e in items if e.get("link")}

    out: List[dict] = []
    for r in rows:
        g = str(r.get("guid") or "")
        e = by_guid.get(g) or by_link.get(g)
        if not e:
            e = {"title": "(item unavailable)", "link": "", "published_at": "", "guid": g, "source": ""}
        folder_name = None
        if r.get("folder_id"):
            f = _sql_one("SELECT name FROM folders WHERE id = ?", (r["folder_id"],))
            folder_name = f["name"] if f else None

        out.append({
            "guid": e.get("guid") or g,
            "title": e.get("title", ""),
            "link": e.get("link", ""),
            "published_at": e.get("published_at", ""),
            "source": e.get("source", ""),
            "folder": folder_name,
        })
    return out

# ---------------------------------------------------------------------------
# Auth tables + helpers
# ---------------------------------------------------------------------------
def _ensure_users_tables() -> None:
    _sql_exec("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    _sql_exec("""
        CREATE TABLE IF NOT EXISTS folders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    _sql_exec("""
        CREATE TABLE IF NOT EXISTS saved_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            guid TEXT NOT NULL,
            folder_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    _sql_exec("""
        CREATE TABLE IF NOT EXISTS saved_filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            params_json TEXT NOT NULL,
            cadence TEXT
        )
    """)
    _sql_exec("CREATE UNIQUE INDEX IF NOT EXISTS uq_folders_user_name ON folders(user_id, name COLLATE NOCASE)")
    _sql_exec("CREATE UNIQUE INDEX IF NOT EXISTS uq_saved_items_user_guid ON saved_items(user_id, guid)")

    # Add ai_summary column if it doesn’t exist
    try:
        _sql_exec("ALTER TABLE items ADD COLUMN ai_summary TEXT")
    except Exception:
        pass


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
        bare = Path(template_name).name
        if bare != template_name:
            try:
                return templates.TemplateResponse(bare, ctx)
            except TemplateNotFound as e2:
                return PlainTextResponse(
                    f"Template not found: '{template_name}' or '{bare}'.\n"
                    f"Searched in: {templates.directory}\n\n{e2}",
                    status_code=500,
                )
        return PlainTextResponse(
            f"Template not found: '{template_name}'.\nSearched in: {templates.directory}",
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
            (r.get("summary", "") or "").replace("\n", " "),
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

    q_lower = q.lower().strip()
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

    filtered.sort(key=lambda e: e.get("published_at", ""), reverse=True)

    page = max(1, int(page))
    per_page = max(1, min(200, int(per_page)))
    total = len(filtered)
    start = (page - 1) * per_page
    end = start + per_page
    page_items = filtered[start:end]
    total_pages = (total + per_page - 1) // per_page if total else 1
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    all_sources = sorted({(i.get("source") or "").strip() for i in all_items if i.get("source")})
    if not sources and not any([q, date_from, date_to, topics, page != 1]):
        sources = list(all_sources)
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
        },
    )

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
            return None
        return OpenAI(api_key=key)
    except Exception:
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

def _generate_ai_summary(title: str, text: str, limit_words: int = 100, guid: str | None = None) -> str:
    """Generate or reuse cached AI summary (stores in items.ai_summary)."""
    text = (text or "").strip()
    if not text:
        return "No content available to summarise."

    # Try to use cached summary first
    if guid:
        cached = _sql_one("SELECT ai_summary FROM items WHERE guid = ?", (guid,))
        if cached and cached.get("ai_summary"):
            return cached["ai_summary"]

    client = _openai_client()
    if not client:
        return _fallback_ai_summary(text, limit_words)

    prompt = f"""Summarise the following item in up to {limit_words} words.
Plain UK English, no bullet points, no headings. Cover what it is, who it affects, and likely action/implication.

TITLE: {title}
TEXT:
{text[:6000]}
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise UK energy regulation analyst."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        out = (resp.choices[0].message.content or "").strip()
        words = out.split()
        if len(words) > limit_words:
            out = " ".join(words[:limit_words]) + "…"

        # Cache the summary for next time
        if guid:
            _sql_exec("UPDATE items SET ai_summary = ? WHERE guid = ?", (out, guid))

        return out
    except Exception:
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

@app.post("/api/ai-summary")
def ai_summary(req: AISummaryReq):
    item = _find_item_by_guid(req.guid)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    title = (item.get("title") or "").strip()
    link  = (item.get("link")  or "").strip()
    text  = (item.get("content") or item.get("summary") or "").strip()

    wants_pdf = ("[PDF document" in text) or _is_pdf_link(link)
    if wants_pdf and link:
        try:
            blob = _fetch_pdf_bytes(link)
            extracted = _pdf_bytes_to_text_pypdf(blob, max_pages=8)
            if extracted:
                text = extracted
            else:
                return JSONResponse({
                    "ok": True,
                    "summary": "This PDF appears to be image-based or has no extractable text. Please open the document to view."
                })
        except Exception:
            return JSONResponse({
                "ok": True,
                "summary": "Could not fetch or parse the PDF for summary. Please open the document to view."
            })

    if not text:
        return JSONResponse({"ok": True, "summary": "No content available to summarise."})

    # scrub nav/footer boilerplate etc.
    text = _clean_extracted_text(title, text)

    summary = _generate_ai_summary(title, text, limit_words=120, guid=req.guid)

    return JSONResponse({"ok": True, "summary": summary})

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
@app.get("/login")
def login_alias():
    return RedirectResponse("/account/login", status_code=308)

@app.get("/register")
def register_alias():
    return RedirectResponse("/account/register", status_code=308)

# Account pages (GET)
@app.get("/account/login", response_class=HTMLResponse)
def account_login_get(request: Request):
    return render(request, "account/login.html", {"error": ""})

@app.get("/account/register", response_class=HTMLResponse)
def account_register_get(request: Request):
    return render(request, "account/register.html", {"error": ""})

# Account actions (POST)
@app.post("/account/login")
def account_login_post(request: Request, email: str = Form(...), password: str = Form(...)):
    user = _get_user_by_email(email)
    if not user or not pwd_ctx.verify(password, user["password_hash"]):
        return render(request, "account/login.html", {"error": "Invalid credentials"})
    request.session["uid"] = user["id"]
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
    return RedirectResponse(url="/summaries", status_code=302)

@app.post("/account/logout")
def account_logout_post(request: Request):
    request.session.clear()
    return RedirectResponse(url="/summaries", status_code=302)

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
@app.post("/folders/new")
def folders_new(request: Request, name: str = Form(...)):
    uid = require_user(request)
    try:
        _create_folder(uid, name)
        return RedirectResponse(url="/saved", status_code=302)
    except HTTPException as e:
        folders = _list_folders(uid)
        items = _list_saved_items(uid, folder_id=None)
        return render(request, "saved.html", {
            "folders": folders,
            "items": items,
            "active_folder": None,
            "error": e.detail,
        })

# Save / Unsave
@app.post("/api/save")
def api_save(request: Request, payload: SaveIn):
    uid = require_user(request)
    _save_item(uid, payload.guid, payload.folder_id)
    return {"ok": True}

@app.api_route("/api/unsave", methods=["DELETE", "POST"])
def api_unsave(request: Request, guid: str | None = Query(None), payload: Dict[str, Any] | None = Body(None)):
    uid = require_user(request)
    if not guid and payload and "guid" in payload:
        guid = str(payload["guid"])
    if not guid:
        raise HTTPException(400, "Missing guid")
    _unsave_item(uid, guid)
    return {"ok": True}

@app.get("/saved", response_class=HTMLResponse)
def saved_page(request: Request, folder_id: int | None = None):
    uid = require_user(request)
    folders = _list_folders(uid)
    items = _list_saved_items(uid, folder_id=folder_id)

    active_folder_name = None
    if folder_id is not None:
        row = _sql_one("SELECT name FROM folders WHERE id = ? AND user_id = ?", (folder_id, uid))
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
