# tools/backfill_risk_refs.py
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone

from storage.db import DB

DB_PATH = os.getenv("DB_PATH", "ofgem.db")
db = DB(DB_PATH)

with db._conn() as conn, closing(conn.cursor()) as cur:
    # backup table for old codes (if not already there)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS org_risks_old_codes (
            org_id      INTEGER NOT NULL,
            risk_id     INTEGER NOT NULL,
            old_code    TEXT,
            backed_up_at TEXT NOT NULL
        )
        """
    )
    conn.commit()

    # all orgs
    cur.execute("SELECT id, name FROM orgs ORDER BY id")
    orgs = cur.fetchall()

    for org in orgs:
        org_id = org["id"]
        org_name = org["name"]
        print(f"\nBackfilling codes for org {org_id} – {org_name}")

        # get ALL risks for this org (corporate + all sites)
        cur.execute(
            """
            SELECT id, code, created_at
            FROM org_risks
            WHERE org_id = ?
            ORDER BY datetime(COALESCE(created_at, '1970-01-01T00:00:00Z')) ASC,
                     id ASC
            """,
            (org_id,),
        )
        risks = cur.fetchall()
        if not risks:
            print(f"  No risks for org {org_id}")
            continue

        updated_count = 0
        now = datetime.now(timezone.utc).isoformat()

        for idx, r in enumerate(risks, start=1):
            new_code = f"R-{idx:03d}"
            old_code = r["code"]

            # only touch if code is different / missing
            if old_code == new_code:
                continue

            # backup old code
            cur.execute(
                """
                INSERT INTO org_risks_old_codes (org_id, risk_id, old_code, backed_up_at)
                VALUES (?, ?, ?, ?)
                """,
                (org_id, r["id"], old_code, now),
            )

            # update to new unified code
            cur.execute(
                """
                UPDATE org_risks
                SET code = ?, updated_at = ?
                WHERE id = ? AND org_id = ?
                """,
                (new_code, now, r["id"], org_id),
            )
            updated_count += 1

        conn.commit()
        print(f"  Updated {updated_count} risk(s) for org {org_id}")
