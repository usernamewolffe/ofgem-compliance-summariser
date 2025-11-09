# tools/precompute_summaries.py
import os
from datetime import datetime, timedelta
from storage.db import DB
import sqlite3
from pathlib import Path

# --- Direct SQL helper fallback ---
def sql_exec(sql: str, params=()):
    """Run SQL safely even if DB wrapper lacks exec()."""
    if hasattr(db, "exec"):
        return db.exec(sql, params)
    if hasattr(db, "execute"):
        return db.execute(sql, params)
    path = Path(DB_PATH)
    conn = sqlite3.connect(path)
    with conn:
        conn.execute(sql, params)

from tools.ai_utils import (
    clean_extracted_text, is_pdf_link, fetch_pdf_bytes, pdf_to_text, generate_ai_summary
)

DB_PATH = os.getenv("DB_PATH", "ofgem.db")
db = DB(DB_PATH)

# Tweak these if you want:
DAYS_BACK = int(os.getenv("PRECOMPUTE_DAYS_BACK", "365"))
LIMIT_WORDS = int(os.getenv("PRECOMPUTE_LIMIT_WORDS", "100"))
ONLY_EMPTY = os.getenv("PRECOMPUTE_ONLY_EMPTY", "1") == "1"  # skip items that already have ai_summary

def iter_items():
    # Pull a lot; filter here.
    for it in db.list_items(limit=20000):
        yield it

def needs_summary(it):
    if ONLY_EMPTY and (it.get("ai_summary") or it.get("ai_summary", None)):
        return False
    # optional date filter
    try:
        p = it.get("published_at") or ""
        if not p:
            return True
        dt = datetime.fromisoformat(p.replace("Z", "+00:00"))
        return dt >= datetime.utcnow() - timedelta(days=DAYS_BACK)
    except Exception:
        return True

def main():
    count = 0
    for item in iter_items():
        if not needs_summary(item):
            continue

        title = (item.get("title") or "").strip()
        link  = (item.get("link")  or "").strip()
        text  = (item.get("content") or item.get("summary") or "").strip()

        # Best-effort PDF text
        if not text and is_pdf_link(link):
            try:
                blob = fetch_pdf_bytes(link)
                text = pdf_to_text(blob, max_pages=8)
            except Exception:
                text = text  # leave as-is

        text = clean_extracted_text(title, text)
        if not text:
            # still nothing — skip
            continue

        summ = generate_ai_summary(title, text, limit_words=LIMIT_WORDS)

        # Persist
        sql_exec(
            "UPDATE items SET ai_summary = ?, ai_summary_updated_at = datetime('now') WHERE guid = ?",
            (summ, item.get("guid") or item.get("link")),
        )

        count += 1
        print(f"📦 cached: {title[:80]!r}")

    print(f"✅ done: {count} summaries cached.")

if __name__ == "__main__":
    main()
