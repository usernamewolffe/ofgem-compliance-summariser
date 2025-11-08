import sqlite3
from typing import Dict, Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guid TEXT UNIQUE,
    title TEXT,
    link TEXT,
    source TEXT,
    published_at TEXT,
    summary TEXT,
    tags TEXT
);
"""

class DB:
    def __init__(self, path: str = "ofgem.db"):
        self.path = path
        self._init()

    def _init(self):
        with sqlite3.connect(self.path) as con:
            con.execute(SCHEMA)
            con.commit()

    def exists(self, guid: str) -> bool:
        with sqlite3.connect(self.path) as con:
            cur = con.execute("SELECT 1 FROM items WHERE guid = ?", (guid,))
            return cur.fetchone() is not None

    def insert_item(self, item: Dict[str, Any]):
        with sqlite3.connect(self.path) as con:
            con.execute(
                "INSERT OR IGNORE INTO items (guid,title,link,source,published_at,summary,tags) VALUES (?,?,?,?,?,?,?)",
                (
                    item.get("guid"),
                    item.get("title"),
                    item.get("link"),
                    item.get("source"),
                    item.get("published_at"),
                    item.get("summary"),
                    item.get("tags"),
                ),
            )
            con.commit()

    def list_items(self, limit: int = 50):
        with sqlite3.connect(self.path) as con:
            cur = con.execute(
                "SELECT id,guid,title,link,source,published_at,summary,tags FROM items ORDER BY published_at DESC NULLS LAST, id DESC LIMIT ?",
                (limit,),
            )
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
