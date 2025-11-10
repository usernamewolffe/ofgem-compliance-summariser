# tools/precompute_summaries.py
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

# App/DB wrapper (if present)
from storage.db import DB

# AI / PDF helpers
from tools.ai_utils import (
    clean_extracted_text,
    is_pdf_link,
    fetch_pdf_bytes,
    pdf_to_text,
    generate_ai_summary,
)

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "ofgem.db")
DAYS_BACK = int(os.getenv("PRECOMPUTE_DAYS_BACK", "365"))
LIMIT_WORDS = int(os.getenv("PRECOMPUTE_LIMIT_WORDS", "100"))
ONLY_EMPTY = os.getenv("PRECOMPUTE_ONLY_EMPTY", "1") == "1"  # skip rows already summarised

# ------------------------------------------------------------------------------
# Small helpers
# ------------------------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def rget(row: Any, key: str, default: Any = None) -> Any:
    """Uniform getter for dict or sqlite3.Row."""
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default

def has_table(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None

def table_columns(conn: sqlite3.Connection, table: str) -> Dict[str, Dict[str, Any]]:
    cols: Dict[str, Dict[str, Any]] = {}
    for r in conn.execute(f"PRAGMA table_info({table})"):
        cols[r["name"]] = {"cid": r["cid"], "type": r["type"], "notnull": r["notnull"], "pk": r["pk"]}
    return cols

def has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return col in table_columns(conn, table)

def ensure_columns(conn: sqlite3.Connection, table: str, to_add: Dict[str, str]) -> None:
    existing = table_columns(conn, table)
    for col, decl in to_add.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

def ensure_min_schema(conn: sqlite3.Connection, table: str) -> None:
    """Create a minimal table if missing and ensure ai_summary columns exist."""
    if not has_table(conn, table):
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INTEGER PRIMARY KEY,
                guid TEXT UNIQUE,
                source TEXT,
                link TEXT,
                title TEXT,
                content TEXT,
                tags TEXT,
                published_at TEXT
            )
            """
        )
    ensure_columns(conn, table, {
        "ai_summary": "TEXT",
        "ai_summary_updated_at": "TEXT",
    })

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def choose_table(conn: sqlite3.Connection) -> str:
    if has_table(conn, "items"):
        return "items"
    if has_table(conn, "entries"):
        return "entries"
    return "items"  # default if none exist

def primary_key_for_update(conn: sqlite3.Connection, table: str) -> str:
    return "guid" if has_column(conn, table, "guid") else "id"

def parse_when(published_at: Optional[str]) -> Optional[datetime]:
    if not published_at:
        return None
    s = published_at.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def is_recent(published_at: Optional[str]) -> bool:
    dt = parse_when(published_at)
    if dt is None:
        return True
    return dt >= utc_now() - timedelta(days=DAYS_BACK)

# ------------------------------------------------------------------------------
# Data access
# ------------------------------------------------------------------------------
def fetch_rows(conn: sqlite3.Connection, table: str) -> Iterable[Any]:
    """Use DB.list_items() if available; otherwise SELECT from the table."""
    db = DB(DB_PATH)
    if hasattr(db, "list_items"):
        # list_items returns dict-like rows
        for it in db.list_items(limit=20000):
            yield it
        return

    # Fallback: generic SELECT
    cols = table_columns(conn, table)
    wanted = [c for c in ("id", "guid", "title", "content", "summary", "link", "published_at", "ai_summary") if c in cols]
    sql = f"SELECT {', '.join(wanted)} FROM {table}"
    for r in conn.execute(sql):
        yield r

def update_summary(conn: sqlite3.Connection, table: str, pk_col: str, pk_val: Any, summary: str) -> None:
    conn.execute(
        f"""
        UPDATE {table}
           SET ai_summary = ?,
               ai_summary_updated_at = ?
         WHERE {pk_col} = ?
        """,
        (summary, utc_now().isoformat(), pk_val),
    )

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = connect(DB_PATH)

    try:
        table = choose_table(conn)
        ensure_min_schema(conn, table)
        conn.commit()

        pk_col = primary_key_for_update(conn, table)
        have_ai_summary = has_column(conn, table, "ai_summary")

        updated = 0

        for row in fetch_rows(conn, table):
            # Skip if ONLY_EMPTY and already has a summary
            if ONLY_EMPTY and have_ai_summary and rget(row, "ai_summary"):
                continue

            if not is_recent(rget(row, "published_at")):
                continue

            title = (rget(row, "title") or "").strip()
            link = (rget(row, "link") or "").strip()

            text = (rget(row, "content") or rget(row, "summary") or "").strip()
            if not text and link and is_pdf_link(link):
                try:
                    blob = fetch_pdf_bytes(link)
                    text = pdf_to_text(blob, max_pages=8)
                except Exception:
                    pass

            text = clean_extracted_text(title, text)
            if not text:
                continue

            summary = generate_ai_summary(title, text, limit_words=LIMIT_WORDS)

            # Determine key to update by
            if pk_col == "guid":
                pk_val = rget(row, "guid") or rget(row, "link")
                if not pk_val:
                    continue
            else:
                pk_val = rget(row, "id")
                if pk_val is None:
                    continue

            update_summary(conn, table, pk_col, pk_val, summary)
            updated += 1
            print(f"📦 cached: {title[:80]!r}")

        conn.commit()
        print(f"✅ done: {updated} summaries cached.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
