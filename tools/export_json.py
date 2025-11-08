# tools/export_json.py
import json, pathlib
from storage.db import DB

def main():
    db = DB("ofgem.db")
    items = db.list_items(limit=5000)  # export plenty
    # Optional: slim fields
    keep = {"title","link","published_at","summary","tags","guid","source"}
    items = [{k:v for k,v in it.items() if k in keep} for it in items]
    out = pathlib.Path("public")
    out.mkdir(exist_ok=True)
    (out / "items.json").write_text(json.dumps(items, indent=2), encoding="utf-8")
    print(f"Wrote {len(items)} items to public/items.json")

if __name__ == "__main__":
    main()
