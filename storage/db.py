# storage/db.py
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from typing import Any, Dict, Iterable, List, Optional


class DB:
    def __init__(self, path: str = "ofgem.db") -> None:
        self.path = path
        # Create parent directory if a subpath is used
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._init_schema()

    # --- connections --------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    # --- schema -------------------------------------------------------------
    def _init_schema(self) -> None:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS items (
                    guid TEXT PRIMARY KEY,
                    source TEXT,
                    title TEXT,
                    link TEXT,
                    content TEXT,
                    summary TEXT,
                    published_at TEXT,
                    tags TEXT -- JSON-encoded list of strings, e.g. ["Cyber","Guidance"]
                )
                """
            )
            # Backwards-compatible column adds (no-op if present)
            cols = {r[1] for r in cur.execute("PRAGMA table_info(items)").fetchall()}
            if "content" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN content TEXT")
            if "tags" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN tags TEXT")
            if "published_at" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN published_at TEXT")

            # Helpful indexes (guid already primary key, keep others for query perf)
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_guid ON items(guid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_items_published ON items(published_at)")
            conn.commit()

    # --- convenience --------------------------------------------------------
    def exists(self, guid_or_link: str) -> bool:
        """True if an item with this guid (or same link) already exists."""
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT 1 FROM items WHERE guid = ? OR link = ? LIMIT 1",
                (guid_or_link, guid_or_link),
            )
            return cur.fetchone() is not None

    # --- tags helpers -------------------------------------------------------
    @staticmethod
    def _dump_tags(tags: Optional[Iterable[str] | str]) -> str:
        """
        Always return a JSON array string. Accepts:
        - list/tuple/set of strings
        - comma-separated string
        - JSON array string
        - None
        """
        if tags is None:
            return "[]"

        if isinstance(tags, (list, tuple, set)):
            return json.dumps([str(t).strip() for t in tags if str(t).strip()], ensure_ascii=False)

        s = str(tags).strip()
        if not s:
            return "[]"

        try:
            maybe = json.loads(s)
            if isinstance(maybe, list):
                return json.dumps([str(t).strip() for t in maybe if str(t).strip()], ensure_ascii=False)
        except json.JSONDecodeError:
            pass

        parts = [p.strip() for p in s.split(",") if p.strip()]
        return json.dumps(parts, ensure_ascii=False)

    @staticmethod
    def _load_tags(raw: Optional[str]) -> List[str]:
        """
        Always return a Python list of strings. Handles legacy formats like:
        - "['Cyber', 'Guidance']"
        - "Cyber, Guidance"
        - JSON array string
        """
        if not raw:
            return []

        s = str(raw).strip()
        if not s:
            return []

        try:
            value = json.loads(s)
            if isinstance(value, list):
                return [str(t).strip() for t in value if str(t).strip()]
        except json.JSONDecodeError:
            pass

        if s.startswith("[") and s.endswith("]"):
            inner = s[1:-1].replace('"', "").replace("'", "")
            return [p.strip() for p in inner.split(",") if p.strip()]

        return [p.strip() for p in s.split(",") if p.strip()]

    # --- public API ---------------------------------------------------------
    def upsert_item(self, item: Dict[str, Any]) -> None:
        """
        Upsert an item.
        Expected keys:
          guid, source, title, link, content, summary, published_at, tags (list[str] or str)
        """
        payload: Dict[str, Any] = {
            "guid": item.get("guid") or item.get("link"),
            "source": item.get("source") or "",
            "title": item.get("title") or "",
            "link": item.get("link") or "",
            "content": item.get("content") or "",
            "summary": item.get("summary") or "",
            "published_at": item.get("published_at") or "",
            "tags": self._dump_tags(item.get("tags")),
        }

        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                INSERT INTO items (guid, source, title, link, content, summary, published_at, tags)
                VALUES (:guid, :source, :title, :link, :content, :summary, :published_at, :tags)
                ON CONFLICT(guid) DO UPDATE SET
                  source=excluded.source,
                  title=excluded.title,
                  link=excluded.link,
                  content=excluded.content,
                  summary=excluded.summary,
                  published_at=excluded.published_at,
                  tags=excluded.tags
                """,
                payload,
            )
            conn.commit()
    # --- compatibility aliases ---------------------------------------------
    def insert_item(self, item: Dict[str, Any]) -> None:
        """Legacy alias used by older scrapers. Now routes to upsert_item()."""
        return self.upsert_item(item)

    def save_item(self, item: Dict[str, Any]) -> None:
        """Another legacy alias some scripts used; routes to upsert_item()."""
        return self.upsert_item(item)

    def list_items(self, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                SELECT guid, source, title, link, content, summary, published_at, tags
                FROM items
                ORDER BY datetime(COALESCE(published_at, '1970-01-01T00:00:00Z')) DESC, rowid DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            rows = cur.fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["tags"] = self._load_tags(d.get("tags"))
            out.append(d)
        return out
