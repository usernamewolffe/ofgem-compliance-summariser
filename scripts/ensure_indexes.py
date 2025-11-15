import sqlite3, os

DB_PATH = os.getenv("DB_PATH", "ofgem.db")

INDEXES = {
    "idx_org_risks_org_status_sev_cat": (
        "CREATE INDEX IF NOT EXISTS idx_org_risks_org_status_sev_cat "
        "ON org_risks(org_id, status, severity, category)"
    ),
    "idx_site_risks_site_status_sev_cat": (
        "CREATE INDEX IF NOT EXISTS idx_site_risks_site_status_sev_cat "
        "ON site_risks(site_id, status, severity, category)"
    ),
}

def index_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name=?",
        (name,),
    ).fetchone()
    return row is not None

with sqlite3.connect(DB_PATH) as conn:
    created = []
    for name, sql in INDEXES.items():
        if not index_exists(conn, name):
            print(f"[schema] creating {name}")
            conn.execute(sql)
            created.append(name)
        else:
            print(f"[schema] {name} already exists")
    conn.commit()

print("\n✅ Done —", "created:" if created else "no new indexes created.", created or "")

