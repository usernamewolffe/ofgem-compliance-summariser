# scripts/normalise_tags.py
from storage.db import DB

db = DB("ofgem.db")
items = db.list_items(limit=100000)

for it in items:
    # list_items already returns tags as a list; upsert will re-store as JSON
    db.upsert_item(it)

print(f"Normalised {len(items)} rows.")
