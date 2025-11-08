# api/server.py
from dotenv import load_dotenv
load_dotenv()
import os
if os.getenv("OPENAI_API_KEY"):
    print("✅ OpenAI key detected — AI summaries enabled")
else:
    print("⚠️ No OpenAI key found — falling back to local summaries")
from pathlib import Path
import os
import csv, io
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from storage.db import DB


app = FastAPI()
db = DB("ofgem.db")

# ---- Static UI (legacy)
app.mount("/static", StaticFiles(directory="api/static", html=True), name="static")

# ---- Templates
BASE_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = BASE_DIR / "summariser" / "templates" / "summariser"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ---------------------------------------------------------------------------
# Routes
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
# Summaries UI with filters
# ---------------------------------------------------------------------------

TOPIC_TAGS = ["CAF/NIS", "Cyber", "Incident", "Consultation", "Guidance", "Enforcement", "Penalty"]

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
    """Render summaries.html with search, date, topic, and source filters."""
    all_items = db.list_items(limit=10000)

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
    src_set = set(sources or [])
    topic_set = {t.lower() for t in (topics or [])}

    filtered: List[dict] = []
    for e in all_items:
        text = f"{e.get('title','')} {e.get('content','')} {e.get('summary','')}".lower()
        tags = [t.lower() for t in (e.get("tags") or [])]
        if q_lower and q_lower not in text:
            continue
        if src_set and e.get("source") not in src_set:
            continue
        if topic_set and not any(t in tags for t in topic_set):
            continue
        if not in_date_range(e.get("published_at")):
            continue
        filtered.append(e)

    # Sort newest first (ISO timestamps sort fine as strings if consistent)
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

    all_sources = sorted({i.get("source") for i in all_items if i.get("source")})

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

    prompt = f"""Summarise the following item in **up to {limit_words} words**.
Be concise, plain UK English, no bullet points, no headers. Focus on what it is, who it affects, and the action or implication.

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
        # Hard cap: trim to ~100 words if model overshoots
        words = out.split()
        if len(words) > limit_words:
            out = " ".join(words[:limit_words]) + "…"
        return out
    except Exception:
        return _fallback_ai_summary(text, limit_words)

def _find_item_by_guid(guid: str) -> Optional[dict]:
    # Avoid changing DB code: scan current list
    items = db.list_items(limit=10000)
    for it in items:
        if (it.get("guid") or it.get("link")) == guid:
            return it
    return None

@app.post("/api/ai-summary")
def ai_summary(req: AISummaryReq):
    item = _find_item_by_guid(req.guid)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    text = item.get("content") or item.get("summary") or ""
    title = item.get("title") or ""
    summary = _generate_ai_summary(title, text, limit_words=100)
    return JSONResponse({"ok": True, "summary": summary})
