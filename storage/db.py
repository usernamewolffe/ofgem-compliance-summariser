# storage/db.py
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from typing import Any, Dict, Iterable, List, Optional


class DB:
    def __init__(self, path: str) -> None:
        self.path = path
        self._init()

    # --- connections --------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
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
                    tags TEXT -- JSON-encoded list of strings (e.g. ["Cyber","Guidance"])
                )
                """
            )
            conn.commit()

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

        # If it's already a container of strings
        if isinstance(tags, (list, tuple, set)):
            return json.dumps([str(t).strip() for t in tags if str(t).strip()], ensure_ascii=False)

        # From here, treat as string
        s = str(tags).strip()
        if not s:
            return "[]"

        # Try to parse as JSON array
        try:
            maybe = json.loads(s)
            if isinstance(maybe, list):
                return json.dumps([str(t).strip() for t in maybe if str(t).strip()], ensure_ascii=False)
        except json.JSONDecodeError:
            pass  # fall through

        # Fallback: comma-separated
        parts = [p.strip() for p in s.split(",") if p.strip()]
        return json.dumps(parts, ensure_ascii=False)

    def exists(self, guid_or_link: str) -> bool:
        """True if an item with this guid (or link fallback) already exists."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT 1 FROM items WHERE guid = ? OR link = ? LIMIT 1",
            (guid_or_link, guid_or_link),
        )
        return cur.fetchone() is not None

    @staticmethod
    def _load_tags(raw: Optional[str]) -> List[str]:
        """
        Always return a Python list of strings.
        Handles legacy formats like:
        - "['Cyber', 'Guidance']"
        - "Cyber, Guidance"
        - JSON array string
        """
        if not raw:
            return []

        s = str(raw).strip()
        if not s:
            return []

        # JSON array
        try:
            value = json.loads(s)
            if isinstance(value, list):
                return [str(t).strip() for t in value if str(t).strip()]
        except json.JSONDecodeError:
            pass

        # Legacy Python repr: ['Cyber', 'Guidance']
        if s.startswith("[") and s.endswith("]"):
            inner = s[1:-1].replace('"', "").replace("'", "")
            return [p.strip() for p in inner.split(",") if p.strip()]

        # Fallback: comma-separated
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
            "source": item.get("source"),
            "title": item.get("title"),
            "link": item.get("link"),
            "content": item.get("content"),
            "summary": item.get("summary"),
            "published_at": item.get("published_at"),
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

    def list_items(self, limit: int = 1000) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            # Note: PyCharm shows "No data sources configured..." because it can't introspect
            # this SQLite file automatically. It's safe to ignore or configure a Data Source.
            cur.execute(
                """
                SELECT guid, source, title, link, content, summary, published_at, tags
                FROM items
                ORDER BY datetime(published_at) DESC, rowid DESC
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
