# api/server.py
from pathlib import Path
from fastapi import FastAPI, Request, Query
from fastapi.responses import RedirectResponse, StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from storage.db import DB
import csv, io
from datetime import datetime

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
# Dynamic summaries UI with filters
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
    topic_set = set(topics or [])

    filtered = []
    for e in all_items:
        text = f"{e.get('title','')} {e.get('content','')} {e.get('summary','')}".lower()
        tags = [t.lower() for t in (e.get("tags") or [])]
        if q_lower and q_lower not in text:
            continue
        if src_set and e.get("source") not in src_set:
            continue
        if topic_set and not any(t.lower() in tags for t in topic_set):
            continue
        if not in_date_range(e.get("published_at")):
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

    all_sources = sorted({i.get("source") for i in all_items if i.get("source")})

    # Pre-compute pagination range
    page_numbers = list(range(max(1, page - 2), min(total_pages, page + 2) + 1))

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
