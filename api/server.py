# api/server.py
from dotenv import load_dotenv
load_dotenv()

import os, csv, io, json
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Set, Dict, Any
from urllib.parse import urlencode

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from storage.db import DB

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TOPIC_TAGS = [
    "CAF/NIS", "Cyber", "Incident", "Consultation", "Guidance", "Enforcement", "Penalty"
]

# ---------------------------------------------------------------------------
# App + plumbing
# ---------------------------------------------------------------------------
app = FastAPI()
db = DB("ofgem.db")

# Static UI (legacy)
app.mount("/static", StaticFiles(directory="api/static", html=True), name="static")

# Templates
BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "summariser" / "templates" / "summariser"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

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
        # text filter
        text = f"{e.get('title','')} {e.get('content','')} {e.get('summary','')}".lower()
        if q_lower and q_lower not in text:
            continue

        # date filter
        if not in_date_range(e.get("published_at")):
            continue

        # topic tag filter
        tags_raw = e.get("tags") or []
        tags = [t.lower() for t in (tags_raw if isinstance(tags_raw, list) else [])]
        # NOTE: if tags could be a string, you can handle that here if needed.

        if topic_set and not any(t in tags for t in topic_set):
            continue

        # source filter
        if src_set and (e.get("source") not in src_set):
            continue

        filtered.append(e)

    # Sort newest first
    filtered.sort(key=lambda e: e.get("published_at", ""), reverse=True)

    # Pagination
    page = max(1, int(page))
    per_page = max(1, min(200, int(per_page)))
    total = len(filtered)
    start = (page - 1) * per_page
    end = start + per_page
    page_items = filtered[start:end]
    total_pages = (total + per_page - 1) // per_page if total else 1
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

    # Available sources for sidebar
    all_sources = sorted({(i.get("source") or "").strip() for i in all_items if i.get("source")})

    return templates.TemplateResponse(
        "summaries.html",
        {
            "request": request,
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
    params: Dict[str, Any]  # whatever you’d put on /summaries (q/date_from/date_to/sources/topics/per_page)
    cadence: Optional[str] = None  # 'daily', 'weekly', etc. (optional)

@app.get("/api/saved-filters")
def list_saved_filters():
    return {"filters": db.list_saved_filters()}

@app.post("/api/saved-filters")
def create_saved_filter(payload: SavedFilterIn):
    # Ensure multi-select arrays are lists of strings
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
        # Build query string, preserving multi-values for sources/topics
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
# AI Summary API
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

def _generate_ai_summary(title: str, text: str, limit_words: int = 100) -> str:
    text = (text or "").strip()
    if not text:
        return "No content available to summarise."
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
        return out
    except Exception:
        return _fallback_ai_summary(text, limit_words)

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

    # If DB text is a PDF placeholder or the link looks like a PDF, try to extract PDF text on-demand
    wants_pdf = ("[PDF document" in text) or _is_pdf_link(link)
    if wants_pdf and link:
        try:
            blob = _fetch_pdf_bytes(link)
            extracted = _pdf_bytes_to_text_pypdf(blob, max_pages=8)  # or _pdf_bytes_to_text_pdfminer
            # If the PDF is scanned (no text), extracted will be empty
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

    summary = _generate_ai_summary(title, text, limit_words=100)
    return JSONResponse({"ok": True, "summary": summary})


# --- PDF helpers ------------------------------------------------------------
import requests, io
from urllib.parse import urlparse

def _is_pdf_link(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
        return path.endswith(".pdf")
    except Exception:
        return False

def _is_pdf_content_type(head_resp) -> bool:
    ct = (head_resp.headers.get("Content-Type") or "").lower()
    return "pdf" in ct or ct.strip() == "application/octet-stream"

def _fetch_pdf_bytes(url: str, timeout: int = 30) -> bytes:
    # quick HEAD to verify content-type if possible
    try:
        h = requests.head(url, timeout=timeout, allow_redirects=True)
        if h.ok and not _is_pdf_content_type(h):
            # Some servers lie on HEAD; we’ll still try GET next.
            pass
    except Exception:
        pass

    r = requests.get(url, timeout=timeout, allow_redirects=True, stream=True)
    r.raise_for_status()
    # basic guardrail: limit to ~15 MB
    total = 0
    chunks = []
    for chunk in r.iter_content(1024 * 64):
        if not chunk:
            break
        total += len(chunk)
        if total > 15 * 1024 * 1024:
            break  # stop at 15MB to avoid huge downloads
        chunks.append(chunk)
    return b"".join(chunks)

# ---- A) Using pypdf
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

