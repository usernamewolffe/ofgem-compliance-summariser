# tools/link_controls.py
from storage.db import DB

def main():
    db = DB("ofgem.db")
    items = db.list_items(limit=20000)
    if not items:
        print("No items found.")
        return

    linked = 0
    for it in items:
        res = db.relink_item_controls(it, min_relevance=0.25)
        if res:
            linked += 1
            print(f"[{it['guid'][:8]}] {it.get('title','')[:60]} -> {', '.join(f'{r}:{s:.2f}' for r,s in res)}")

    print(f"✅ linked {linked}/{len(items)} items")

if __name__ == "__main__":
    main()
