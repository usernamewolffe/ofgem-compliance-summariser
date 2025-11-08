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

# ---- Static UI
app.mount("/static", StaticFiles(directory="api/static", html=True), name="static")

# ---- Templates (absolute path so it never misses)
BASE_DIR = Path(__file__).resolve().parent.parent  # project root
TEMPLATES_DIR = BASE_DIR / "summariser" / "templates" / "summariser"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ---- Topic rules (dropdown)
# Matches if ANY keyword is present in title/content/summary OR in tags.
TOPIC_RULES = {
    "Compliance": [
        "compliance", "guidance", "how to comply", "audit", "assurance",
        "good practice", "licence condition", "licence conditions"
    ],
    "Incidents": [
        "incident", "outage", "compromise", "breach", "report within 72",
        "service interruption", "major incident"
    ],
    "Cyber": [
        "cyber", "malware", "vulnerability", "threat", "phishing",
        "ransomware", "cve-", "patch tuesday"
    ],
    "Consultation": [
        "consultation", "call for evidence", "seeking views", "proposal"
    ],
    "Enforcement": [
        "enforcement", "investigation", "compliance case", "notice", "decision"
    ],
    "Penalty": [
        "penalty", "fine", "sanction"
    ],
    # Add/rename as you like
}
TOPIC_ORDER = ["", "Compliance", "Incidents", "Cyber", "Consultation", "Enforcement", "Penalty"]  # "" = All

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
# Dynamic summaries UI with filters (search, date, source, topic + pagination)
# ---------------------------------------------------------------------------

@app.get("/summaries", response_class=HTMLResponse)
def summaries_page(
    request: Request,
    q: str = "",
    date_from: str | None = None,
    date_to: str | None = None,
    sources: list[str] = Query(default=[]),
    topic: str = "",                    # <-- NEW: dropdown topic
    page: int = 1,
    per_page: int = 25,
):
    all_items = db.list_items(limit=10000)

    # helpers
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

    def norm_tags(v):
        # tags may be a list or a comma-separated string
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        if isinstance(v, str):
            return [t.strip() for t in v.split(",") if t.strip()]
        return []

    def matches_topic(e, topic_name: str) -> bool:
        if not topic_name or topic_name not in TOPIC_RULES:
            return True
        needles = TOPIC_RULES[topic_name]
        blob = f"{e.get('title','')} {e.get('content','')} {e.get('summary','')}".lower()
        if any(n.lower() in blob for n in needles):
            return True
        tags_low = [t.lower() for t in norm_tags(e.get("tags"))]
        return any(any(n.lower() in t for t in tags_low) for n in needles)

    # derive distinct sources for sidebar
    all_sources = sorted({i.get("source") for i in all_items if i.get("source")})

    # filtering
    q_lower = q.lower().strip()
    src_set = set(sources or [])

    filtered = []
    for e in all_items:
        text = f"{e.get('title','')} {e.get('content','')} {e.get('summary','')}".lower()

        if q_lower and q_lower not in text:
            continue
        if src_set and e.get("source") not in src_set:
            continue
        if not in_date_range(e.get("published_at")):
            continue
        if not matches_topic(e, topic):
            continue

        filtered.append(e)

    # sort newest first
    def sort_key(e):
        v = e.get("published_at")
        try:
            return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except Exception:
            return datetime.min
    filtered.sort(key=sort_key, reverse=True)

    # pagination
    page = max(1, int(page))
    per_page = max(1, min(200, int(per_page)))
    total = len(filtered)
    start = (page - 1) * per_page
    end = start + per_page
    page_items = filtered[start:end]
    total_pages = (total + per_page - 1) // per_page if total else 1

    # windowed page numbers (do this in Python, not in Jinja)
    start_page = 1 if page - 2 < 1 else page - 2
    end_page = total_pages if page + 2 > total_pages else page + 2
    page_numbers = list(range(start_page, end_page + 1))

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
                "topic": topic,                # <-- NEW
                "per_page": per_page,
            },
            "all_sources": all_sources,
            "topic_order": TOPIC_ORDER,       # <-- NEW (for dropdown)
        },
    )
