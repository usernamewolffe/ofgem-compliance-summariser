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
        """Create/upgrade tables and indexes (idempotent)."""
        with self._conn() as conn, closing(conn.cursor()) as cur:
            # Core items table
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

            # Helpful indexes (guid already PK)
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_guid ON items(guid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_items_published ON items(published_at)")

            # --- NEW: saved_filters table -----------------------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS saved_filters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    params_json TEXT NOT NULL,   -- serialized query params for /summaries
                    cadence TEXT,                -- e.g. 'daily', 'weekly', or NULL
                    created_at TEXT NOT NULL     -- ISO 8601 timestamp
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_saved_filters_created ON saved_filters(created_at)")

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

        # JSON array string?
        try:
            maybe = json.loads(s)
            if isinstance(maybe, list):
                return json.dumps([str(t).strip() for t in maybe if str(t).strip()], ensure_ascii=False)
        except json.JSONDecodeError:
            pass

        # Fallback: comma-separated
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

        # JSON array
        try:
            value = json.loads(s)
            if isinstance(value, list):
                return [str(t).strip() for t in value if str(t).strip()]
        except json.JSONDecodeError:
            pass

        # Legacy Python repr
        if s.startswith("[") and s.endswith("]"):
            inner = s[1:-1].replace('"', "").replace("'", "")
            return [p.strip() for p in inner.split(",") if p.strip()]

        # Fallback: comma-separated
        return [p.strip() for p in s.split(",") if p.strip()]

    # --- public API (items) -------------------------------------------------
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

    # Compatibility aliases used by older scrapers
    def insert_item(self, item: Dict[str, Any]) -> None:
        return self.upsert_item(item)

    def save_item(self, item: Dict[str, Any]) -> None:
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

    # --- public API (saved filters) ----------------------------------------
    def create_saved_filter(self, name: str, params_json: str, cadence: str | None = None) -> int:
        """Create a saved filter and return its id."""
        from datetime import datetime, timezone
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO saved_filters (name, params_json, cadence, created_at) VALUES (?,?,?,?)",
                (name.strip(), params_json, cadence, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            return int(cur.lastrowid)

    def list_saved_filters(self) -> List[Dict[str, Any]]:
        """Return all saved filters newest first."""
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT id, name, params_json, cadence, created_at FROM saved_filters ORDER BY id DESC"
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def get_saved_filter(self, filter_id: int) -> Optional[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT id, name, params_json, cadence, created_at FROM saved_filters WHERE id = ?",
                (int(filter_id),),
            )
            row = cur.fetchone()
            return dict(row) if row else None

    def delete_saved_filter(self, filter_id: int) -> None:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute("DELETE FROM saved_filters WHERE id = ?", (int(filter_id),))
            conn.commit()

def init_auth(self):
    with self.conn:
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS folders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            UNIQUE(user_id, name),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        )""")
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS saved_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            folder_id INTEGER,
            guid TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, guid),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE SET NULL
        )""")


def get_user_by_email(self, email: str):
    cur = self.conn.execute("SELECT * FROM users WHERE email=?", (email.lower().strip(),))
    r = cur.fetchone()
    return dict(r) if r else None

def create_user(self, email: str, password_hash: str) -> int:
    cur = self.conn.execute("INSERT INTO users (email, password_hash) VALUES (?,?)", (email.lower().strip(), password_hash))
    return cur.lastrowid

def get_user(self, uid: int):
    cur = self.conn.execute("SELECT * FROM users WHERE id=?", (uid,))
    r = cur.fetchone()
    return dict(r) if r else None


def list_folders(self, user_id: int):
    cur = self.conn.execute("SELECT id, name FROM folders WHERE user_id=? ORDER BY name", (user_id,))
    return [dict(r) for r in cur.fetchall()]

def create_folder(self, user_id: int, name: str) -> int:
    name = name.strip()
    if not name:
        return 0
    cur = self.conn.execute("INSERT OR IGNORE INTO folders (user_id, name) VALUES (?,?)", (user_id, name))
    if cur.lastrowid:
        return cur.lastrowid
    cur = self.conn.execute("SELECT id FROM folders WHERE user_id=? AND name=?", (user_id, name))
    row = cur.fetchone()
    return row[0] if row else 0

def delete_folder(self, user_id: int, folder_id: int):
    self.conn.execute("DELETE FROM folders WHERE id=? AND user_id=?", (folder_id, user_id))

