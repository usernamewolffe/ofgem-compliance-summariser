# storage/db.py
from __future__ import annotations

import json
import os
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


class DB:
    def __init__(self, path: str = "ofgem.db") -> None:
        self.path = path
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._init_schema()

    # --- connections --------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        # Enforce FK constraints
        with conn:
            conn.execute("PRAGMA foreign_keys=ON")
        return conn

    # --- schema -------------------------------------------------------------
    def _has_column(self, cur: sqlite3.Cursor, table: str, col: str) -> bool:
        cols = {r[1] for r in cur.execute(f"PRAGMA table_info({table})").fetchall()}
        return col in cols

    def _init_schema(self) -> None:
        """Create or upgrade tables and indexes (idempotent)."""
        with self._conn() as conn, closing(conn.cursor()) as cur:
            # ------------------- core items -------------------
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
                    tags TEXT,
                    ai_summary TEXT,
                    ai_summary_updated_at TEXT
                )
                """
            )
            cols = {r[1] for r in cur.execute("PRAGMA table_info(items)").fetchall()}
            if "content" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN content TEXT")
            if "tags" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN tags TEXT")
            if "published_at" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN published_at TEXT")
            if "ai_summary" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN ai_summary TEXT")
            if "ai_summary_updated_at" not in cols:
                cur.execute("ALTER TABLE items ADD COLUMN ai_summary_updated_at TEXT")

            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_guid ON items(guid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_items_published ON items(published_at)")

            # ------------------- saved filters -------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS saved_filters (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    cadence TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_saved_filters_created ON saved_filters(created_at)")

            # ------------------- framework controls -------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS controls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ref TEXT NOT NULL UNIQUE,      -- e.g. 'CAF-D1' or '27019-5.1'
                    name TEXT NOT NULL,
                    description TEXT,
                    themes TEXT,
                    keywords TEXT,                 -- JSON array of strings
                    framework TEXT,                -- e.g. 'CAF', 'ISO27001', 'ISO27019'
                    version TEXT                   -- e.g. 'v3', '2022'
                )
                """
            )
            c_cols = {r[1] for r in cur.execute("PRAGMA table_info(controls)").fetchall()}
            if "framework" not in c_cols:
                cur.execute("ALTER TABLE controls ADD COLUMN framework TEXT")
            if "version" not in c_cols:
                cur.execute("ALTER TABLE controls ADD COLUMN version TEXT")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_controls_ref ON controls(ref)")

            # ------------------- item ↔ control links -------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS item_control_links (
                    item_guid TEXT NOT NULL,
                    control_id INTEGER NOT NULL,
                    relevance REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (item_guid, control_id),
                    FOREIGN KEY (control_id) REFERENCES controls(id) ON DELETE CASCADE
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_icl_item ON item_control_links(item_guid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_icl_control ON item_control_links(control_id)")

            # ------------------- organisations & sites -------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orgs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    created_by TEXT
                )
                """
            )
            # backfill for older DBs
            if not self._has_column(cur, "orgs", "created_at"):
                cur.execute("ALTER TABLE orgs ADD COLUMN created_at TEXT")
                cur.execute("UPDATE orgs SET created_at = datetime('now') WHERE created_at IS NULL")
            if not self._has_column(cur, "orgs", "created_by"):
                cur.execute("ALTER TABLE orgs ADD COLUMN created_by TEXT")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    org_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    code TEXT,
                    location TEXT,
                    created_at TEXT NOT NULL,
                    created_by TEXT,
                    UNIQUE(org_id, name),
                    FOREIGN KEY(org_id) REFERENCES orgs(id) ON DELETE CASCADE
                )
                """
            )
            if not self._has_column(cur, "sites", "created_at"):
                cur.execute("ALTER TABLE sites ADD COLUMN created_at TEXT")
                cur.execute("UPDATE sites SET created_at = datetime('now') WHERE created_at IS NULL")
            if not self._has_column(cur, "sites", "created_by"):
                cur.execute("ALTER TABLE sites ADD COLUMN created_by TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sites_org ON sites(org_id)")

            # ------------------- org controls (with optional site) -------------------
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS org_controls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    org_id INTEGER NOT NULL,
                    site_id INTEGER,                 -- NULL for org-wide
                    code TEXT,
                    title TEXT NOT NULL,
                    description TEXT,
                    owner_email TEXT,
                    tags TEXT,
                    status TEXT,
                    risk TEXT,
                    review_frequency_days INTEGER,
                    next_review_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    created_by TEXT,
                    UNIQUE(org_id, site_id, code),
                    FOREIGN KEY(org_id) REFERENCES orgs(id) ON DELETE CASCADE,
                    FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE SET NULL
                )
                """
            )
            oc_cols = {r[1] for r in cur.execute("PRAGMA table_info(org_controls)").fetchall()}
            if "site_id" not in oc_cols:
                cur.execute("ALTER TABLE org_controls ADD COLUMN site_id INTEGER")
            if "created_at" not in oc_cols:
                cur.execute("ALTER TABLE org_controls ADD COLUMN created_at TEXT")
                cur.execute("UPDATE org_controls SET created_at = datetime('now') WHERE created_at IS NULL")
            if "updated_at" not in oc_cols:
                cur.execute("ALTER TABLE org_controls ADD COLUMN updated_at TEXT")
                cur.execute("UPDATE org_controls SET updated_at = created_at WHERE updated_at IS NULL")
            if "created_by" not in oc_cols:
                cur.execute("ALTER TABLE org_controls ADD COLUMN created_by TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_org_controls_org ON org_controls(org_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_org_controls_title ON org_controls(title)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_org_controls_site ON org_controls(site_id)")

            # mapping: org controls ↔ framework controls (+ provenance)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS org_control_map (
                    org_control_id INTEGER NOT NULL,
                    control_id INTEGER NOT NULL,
                    created_at TEXT,
                    created_by TEXT,
                    PRIMARY KEY (org_control_id, control_id),
                    FOREIGN KEY (org_control_id) REFERENCES org_controls(id) ON DELETE CASCADE,
                    FOREIGN KEY (control_id) REFERENCES controls(id) ON DELETE CASCADE
                )
                """
            )
            ocm_cols = {r[1] for r in cur.execute("PRAGMA table_info(org_control_map)").fetchall()}
            if "created_at" not in ocm_cols:
                cur.execute("ALTER TABLE org_control_map ADD COLUMN created_at TEXT")
                cur.execute("UPDATE org_control_map SET created_at = datetime('now') WHERE created_at IS NULL")
            if "created_by" not in ocm_cols:
                cur.execute("ALTER TABLE org_control_map ADD COLUMN created_by TEXT")

            # projection: items -> org controls via framework links
            cur.execute("DROP VIEW IF EXISTS v_item_org_control_links")
            cur.execute(
                """
                CREATE VIEW v_item_org_control_links AS
                SELECT l.item_guid,
                       oc.id AS org_control_id,
                       MAX(l.relevance) AS relevance
                FROM item_control_links l
                JOIN org_control_map m ON m.control_id = l.control_id
                JOIN org_controls     oc ON oc.id = m.org_control_id
                GROUP BY l.item_guid, oc.id
                """
            )

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
        """Always return a JSON array string for tags."""
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
        """Return a Python list of strings from flexible tag formats."""
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

    def _json_array(self, val) -> str:
        """Generic list→JSON helper."""
        if not val:
            return "[]"
        if isinstance(val, (list, tuple, set)):
            return json.dumps([str(x).strip() for x in val if str(x).strip()], ensure_ascii=False)
        s = str(val).strip()
        if not s:
            return "[]"
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return json.dumps([str(x).strip() for x in arr if str(x).strip()], ensure_ascii=False)
        except json.JSONDecodeError:
            pass
        return json.dumps([p.strip() for p in s.split(",") if p.strip()], ensure_ascii=False)

    # --- public API (items) -------------------------------------------------
    def upsert_item(self, item: Dict[str, Any]) -> None:
        """Upsert an item."""
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

    def insert_item(self, item: Dict[str, Any]) -> None:
        return self.upsert_item(item)

    def save_item(self, item: Dict[str, Any]) -> None:
        return self.upsert_item(item)

    def list_items(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Return items plus `controls` list (refs) for each item."""
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                SELECT guid, source, title, link, content, summary, published_at, tags,
                       ai_summary, ai_summary_updated_at
                FROM items
                ORDER BY datetime(COALESCE(published_at, '1970-01-01T00:00:00Z')) DESC, rowid DESC
                LIMIT ?
                """,
                (int(limit),),
            )
            rows = cur.fetchall()

            guids = [r["guid"] for r in rows]
            links: dict[str, list[str]] = {}
            if guids:
                qmarks = ",".join("?" * len(guids))
                cur.execute(
                    f"""
                    SELECT l.item_guid, c.ref
                    FROM item_control_links l
                    JOIN controls c ON c.id = l.control_id
                    WHERE l.item_guid IN ({qmarks})
                    ORDER BY l.relevance DESC
                    """,
                    guids,
                )
                for r in cur.fetchall():
                    links.setdefault(r["item_guid"], []).append(r["ref"])

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["tags"] = self._load_tags(d.get("tags"))
            d["tags"] = list(dict.fromkeys(d["tags"]))  # de-dupe
            d["controls"] = list(dict.fromkeys(links.get(d["guid"], [])))
            out.append(d)
        return out

    # --- saved filters ------------------------------------------------------
    def create_saved_filter(self, name: str, params_json: str, cadence: str | None = None) -> int:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "INSERT INTO saved_filters (name, params_json, cadence, created_at) VALUES (?,?,?,?)",
                (name.strip(), params_json, cadence, datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()
            return int(cur.lastrowid)

    def list_saved_filters(self) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT id, name, params_json, cadence, created_at FROM saved_filters ORDER BY id DESC"
            )
            return [dict(r) for r in cur.fetchall()]

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

    # --------- folders ----------
    def list_folders(self, user_email: str) -> list[dict]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT id, name, created_at FROM folders WHERE user_email=? ORDER BY name",
                (user_email,),
            )
            return [dict(r) for r in cur.fetchall()]

    def create_folder(self, user_email: str, name: str) -> int:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            now = datetime.now(timezone.utc).isoformat()
            try:
                cur.execute(
                    "INSERT INTO folders (user_email, name, created_at) VALUES (?,?,?)",
                    (user_email, name.strip(), now),
                )
                conn.commit()
                return int(cur.lastrowid)
            except sqlite3.IntegrityError:
                cur.execute(
                    "SELECT id FROM folders WHERE user_email=? AND name=?",
                    (user_email, name.strip()),
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0

    # --------- saved items ----------
    def save_item_for_user(self, user_email: str, item_guid: str, folder_id: int | None = None,
                           note: str | None = None):
        with self._conn() as conn, closing(conn.cursor()) as cur:
            now = datetime.now(timezone.utc).isoformat()
            cur.execute(
                """
                INSERT INTO saved_items (user_email, item_guid, folder_id, note, created_at)
                VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_email, item_guid) DO
                UPDATE SET
                    folder_id = excluded.folder_id,
                    note = COALESCE (excluded.note, saved_items.note)
                """,
                (user_email, item_guid, folder_id, note, now),
            )
            conn.commit()

    def unsave_item_for_user(self, user_email: str, item_guid: str):
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute("DELETE FROM saved_items WHERE user_email=? AND item_guid=?", (user_email, item_guid))
            conn.commit()

    def list_saved_items(self, user_email: str, folder_id: int | None = None, limit: int = 200) -> list[dict]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            if folder_id:
                cur.execute(
                    """
                    SELECT i.guid, i.title, i.link, i.source, i.published_at, s.created_at AS saved_at, s.note
                    FROM saved_items s
                             JOIN items i ON i.guid = s.item_guid
                    WHERE s.user_email = ?
                      AND s.folder_id = ?
                    ORDER BY datetime(COALESCE(i.published_at, '1970-01-01')) DESC LIMIT ?
                    """,
                    (user_email, int(folder_id), int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT i.guid, i.title, i.link, i.source, i.published_at, s.created_at AS saved_at, s.note
                    FROM saved_items s
                             JOIN items i ON i.guid = s.item_guid
                    WHERE s.user_email = ?
                    ORDER BY datetime(COALESCE(i.published_at, '1970-01-01')) DESC LIMIT ?
                    """,
                    (user_email, int(limit)),
                )
            return [dict(r) for r in cur.fetchall()]

    # --------- user tags (site / control) ----------
    def tag_item_site(self, user_email: str, item_guid: str, org_id: int, site_id: int):
        with self._conn() as conn, closing(conn.cursor()) as cur:
            now = datetime.now(timezone.utc).isoformat()
            cur.execute(
                """
                INSERT
                OR IGNORE INTO user_item_tags (user_email, item_guid, org_id, site_id, org_control_id, created_at)
                VALUES (?,?,?,?,NULL,?)
                """,
                (user_email, item_guid, int(org_id), int(site_id), now),
            )
            conn.commit()

    def tag_item_control(self, user_email: str, item_guid: str, org_id: int, org_control_id: int):
        with self._conn() as conn, closing(conn.cursor()) as cur:
            now = datetime.now(timezone.utc).isoformat()
            cur.execute(
                """
                INSERT
                OR IGNORE INTO user_item_tags (user_email, item_guid, org_id, site_id, org_control_id, created_at)
                VALUES (?,?,?,NULL,?,?)
                """,
                (user_email, item_guid, int(org_id), int(org_control_id), now),
            )
            conn.commit()

    def untag_item_site(self, user_email: str, item_guid: str, site_id: int):
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "DELETE FROM user_item_tags WHERE user_email=? AND item_guid=? AND site_id=?",
                (user_email, item_guid, int(site_id)),
            )
            conn.commit()

    def untag_item_control(self, user_email: str, item_guid: str, org_control_id: int):
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "DELETE FROM user_item_tags WHERE user_email=? AND item_guid=? AND org_control_id=?",
                (user_email, item_guid, int(org_control_id)),
            )
            conn.commit()

    def list_item_user_tags(self, user_email: str, item_guid: str) -> dict:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            out = {"sites": [], "controls": []}
            # sites
            cur.execute(
                """
                SELECT t.site_id, s.name
                FROM user_item_tags t
                         JOIN sites s ON s.id = t.site_id
                WHERE t.user_email = ?
                  AND t.item_guid = ?
                  AND t.site_id IS NOT NULL
                ORDER BY s.name
                """,
                (user_email, item_guid),
            )
            out["sites"] = [dict(r) for r in cur.fetchall()]
            # controls
            cur.execute(
                """
                SELECT t.org_control_id AS id, oc.title, oc.code
                FROM user_item_tags t
                         JOIN org_controls oc ON oc.id = t.org_control_id
                WHERE t.user_email = ?
                  AND t.item_guid = ?
                  AND t.org_control_id IS NOT NULL
                ORDER BY oc.title
                """,
                (user_email, item_guid),
            )
            out["controls"] = [dict(r) for r in cur.fetchall()]
            return out

    # --- controls (framework) ----------------------------------------------
    def upsert_control(
        self,
        ref: str,
        name: str,
        description: str = "",
        themes: str | List[str] | None = None,
        keywords: str | List[str] | None = None,
        framework: str | None = None,
        version: str | None = None,
    ) -> int:
        payload = {
            "ref": ref.strip(),
            "name": name.strip(),
            "description": (description or "").strip(),
            "themes": (json.dumps(themes) if isinstance(themes, (list, tuple)) else (themes or "")).strip(),
            "keywords": self._json_array(keywords),
            "framework": (framework or "").strip() if framework else None,
            "version": (version or "").strip() if version else None,
        }
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                INSERT INTO controls (ref, name, description, themes, keywords, framework, version)
                VALUES (:ref, :name, :description, :themes, :keywords, :framework, :version)
                ON CONFLICT(ref) DO UPDATE SET
                  name=excluded.name,
                  description=excluded.description,
                  themes=excluded.themes,
                  keywords=excluded.keywords,
                  framework=COALESCE(excluded.framework, controls.framework),
                  version=COALESCE(excluded.version, controls.version)
                """,
                payload,
            )
            conn.commit()
            cur.execute("SELECT id FROM controls WHERE ref=?", (payload["ref"],))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def list_controls(self) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT id, ref, name, description, themes, keywords, framework, version FROM controls ORDER BY ref"
            )
            return [dict(r) for r in cur.fetchall()]

    # --- linkage: items → controls -----------------------------------------
    _WORD = re.compile(r"[A-Za-z0-9]{3,}")

    def _tokenize(self, text: str) -> set[str]:
        return {w.lower() for w in self._WORD.findall(text or "")}

    def _score_item_against_control(self, item_text: str, ctrl_keywords_json: str) -> float:
        try:
            kws = json.loads(ctrl_keywords_json or "[]")
        except json.JSONDecodeError:
            kws = []
        kws = [k for k in (kws or []) if isinstance(k, str) and k.strip()]
        if not kws:
            return 0.0

        text = (item_text or "").lower()
        words = self._tokenize(text)
        overlap = 0
        phrase_boost = 0.0
        for kw in kws:
            k = kw.strip().lower()
            if " " in k and k in text:
                phrase_boost += 0.5
            elif k in words:
                overlap += 1
        base = overlap / max(1, len([k for k in kws if " " not in k]))
        score = min(1.0, base + min(1.0, phrase_boost / 2.0))
        return float(score)

    def relink_item_controls(self, item: dict, min_relevance: float = 0.35) -> List[tuple[str, float]]:
        """Compute and store control links for a single item."""
        text = " ".join(
            [
                (item.get("title") or "").strip(),
                (item.get("ai_summary") or item.get("summary") or item.get("content") or "").strip(),
            ]
        ).strip()
        if not text:
            return []

        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute("SELECT id, ref, name, keywords FROM controls")
            ctrls = cur.fetchall()
            if not ctrls:
                return []

            scored: List[tuple[int, str, float]] = []
            lowtext = text.lower()
            for c in ctrls:
                score = self._score_item_against_control(text, c["keywords"])
                # small bonus if control name terms appear in text
                name_tokens = self._tokenize(c["name"])
                if any(tok in lowtext for tok in name_tokens):
                    score += 0.1
                score = min(1.0, score)
                if score >= float(min_relevance):
                    scored.append((c["id"], c["ref"], score))

            if scored:
                cur.execute("DELETE FROM item_control_links WHERE item_guid=?", (item["guid"],))
                now = datetime.now(timezone.utc).isoformat()
                cur.executemany(
                    """
                    INSERT INTO item_control_links (item_guid, control_id, relevance, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    [(item["guid"], cid, rel, now) for (cid, _ref, rel) in scored],
                )
                conn.commit()

            return [(ref, rel) for (_cid, ref, rel) in scored]

    def list_item_links(self, item_guid: str) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                SELECT c.ref, c.name, l.relevance, l.created_at
                FROM item_control_links l
                JOIN controls c ON c.id = l.control_id
                WHERE l.item_guid = ?
                ORDER BY l.relevance DESC
                """,
                (item_guid,),
            )
            return [dict(r) for r in cur.fetchall()]

    # --- organisations / sites ---------------------------------------------
    def upsert_org(self, name: str, created_by: Optional[str] = None) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "INSERT OR IGNORE INTO orgs (name, created_at, created_by) VALUES (?,?,?)",
                (name.strip(), now, created_by),
            )
            if cur.lastrowid:
                conn.commit()
                return int(cur.lastrowid)
            cur.execute("SELECT id FROM orgs WHERE name=?", (name.strip(),))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def list_orgs(self) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute("SELECT id, name, created_at, created_by FROM orgs ORDER BY name")
            return [dict(r) for r in cur.fetchall()]

    def upsert_site(
        self,
        org_id: int,
        name: str,
        code: str | None = None,
        location: str | None = None,
        created_by: Optional[str] = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        payload = (
            int(org_id),
            name.strip(),
            (code or "").strip() or None,
            (location or "").strip() or None,
            now,
            created_by,
        )
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "INSERT OR IGNORE INTO sites (org_id, name, code, location, created_at, created_by) VALUES (?,?,?,?,?,?)",
                payload,
            )
            if cur.lastrowid:
                conn.commit()
                return int(cur.lastrowid)
            cur.execute("SELECT id FROM sites WHERE org_id=? AND name=?", (int(org_id), name.strip()))
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def list_sites(self, org_id: int) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT id, org_id, name, code, location, created_at, created_by FROM sites WHERE org_id=? ORDER BY name",
                (int(org_id),),
            )
            return [dict(r) for r in cur.fetchall()]

    # Creator-scoped variants (do not break existing callers)
    def list_sites_for_user(self, org_id: int, user: str) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                SELECT id, org_id, name, code, location, created_at, created_by
                FROM sites
                WHERE org_id = ? AND (created_by = ? OR created_by IS NULL)
                ORDER BY name
                """,
                (int(org_id), user),
            )
            return [dict(r) for r in cur.fetchall()]

    # --- org controls (user-owned) -----------------------------------------
    def upsert_org_control(
        self,
        org_id: int,
        title: str,
        code: str = "",
        description: str = "",
        owner_email: str = "",
        tags: Optional[Iterable[str] | str] = None,
        status: str = "Active",
        risk: str = "",
        review_frequency_days: int | None = None,
        next_review_at: str | None = None,
        site_id: int | None = None,
        created_by: Optional[str] = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "org_id": int(org_id),
            "site_id": int(site_id) if site_id is not None else None,
            "code": code.strip(),
            "title": title.strip(),
            "description": (description or "").strip(),
            "owner_email": (owner_email or "").strip(),
            "tags": self._json_array(tags),
            "status": (status or "").strip(),
            "risk": (risk or "").strip(),
            "review_frequency_days": review_frequency_days,
            "next_review_at": next_review_at,
            "created_at": now,
            "updated_at": now,
            "created_by": created_by,
        }
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                INSERT INTO org_controls
                  (org_id, site_id, code, title, description, owner_email, tags, status, risk,
                   review_frequency_days, next_review_at, created_at, updated_at, created_by)
                VALUES
                  (:org_id, :site_id, :code, :title, :description, :owner_email, :tags, :status, :risk,
                   :review_frequency_days, :next_review_at, :created_at, :updated_at, :created_by)
                """,
                payload,
            )
            conn.commit()
            return int(cur.lastrowid)

    def list_org_controls(self, org_id: int, site_id: int | None = None) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            if site_id is None:
                cur.execute(
                    "SELECT * FROM org_controls WHERE org_id=? AND site_id IS NULL ORDER BY title",
                    (int(org_id),),
                )
            else:
                cur.execute(
                    "SELECT * FROM org_controls WHERE org_id=? AND site_id=? ORDER BY title",
                    (int(org_id), int(site_id)),
                )
            rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["tags"] = self._load_tags(d.get("tags"))
            out.append(d)
        return out

    def list_org_controls_for_user(self, org_id: int, user: str, site_id: int | None = None) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            if site_id is None:
                cur.execute(
                    """
                    SELECT * FROM org_controls
                    WHERE org_id=? AND site_id IS NULL
                      AND (created_by = ? OR created_by IS NULL)
                    ORDER BY title
                    """,
                    (int(org_id), user),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM org_controls
                    WHERE org_id=? AND site_id=?
                      AND (created_by = ? OR created_by IS NULL)
                    ORDER BY title
                    """,
                    (int(org_id), int(site_id), user),
                )
            rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["tags"] = self._load_tags(d.get("tags"))
            out.append(d)
        return out

    def list_all_controls_for_org(self, org_id: int) -> List[Dict[str, Any]]:
        """Org-level + per-site in one list, joined with site name."""
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                SELECT oc.id, oc.org_id, oc.site_id, oc.code, oc.title, oc.description,
                       oc.status, oc.risk, oc.owner_email, oc.tags, oc.review_frequency_days,
                       oc.next_review_at, oc.created_at, oc.updated_at, oc.created_by,
                       s.name AS site_name
                FROM org_controls oc
                LEFT JOIN sites s ON s.id = oc.site_id
                WHERE oc.org_id=?
                ORDER BY COALESCE(s.name, ''), oc.code
                """,
                (int(org_id),),
            )
            rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["tags"] = self._load_tags(d.get("tags"))
            out.append(d)
        return out

    def map_org_control_to_controls(self, org_control_id: int, control_ids: List[int], created_by: Optional[str] = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute("DELETE FROM org_control_map WHERE org_control_id=?", (int(org_control_id),))
            cur.executemany(
                "INSERT OR IGNORE INTO org_control_map (org_control_id, control_id, created_at, created_by) VALUES (?,?,?,?)",
                [(int(org_control_id), int(cid), now, created_by) for cid in control_ids],
            )
            conn.commit()

    def list_items_for_org_control(self, org_control_id: int, limit: int = 200) -> List[Dict[str, Any]]:
        with self._conn() as conn, closing(conn.cursor()) as cur:
            cur.execute(
                """
                SELECT i.guid, i.title, i.link, i.published_at, i.source, i.ai_summary
                FROM v_item_org_control_links v
                JOIN items i ON i.guid = v.item_guid
                WHERE v.org_control_id=?
                ORDER BY datetime(COALESCE(i.published_at,'1970-01-01T00:00:00Z')) DESC
                LIMIT ?
                """,
                (int(org_control_id), int(limit)),
            )
            return [dict(r) for r in cur.fetchall()]
