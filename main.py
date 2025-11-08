from scraper.ofgem import collect_items
from summariser.model import summarise_and_tag
from storage.db import DB

def run():
    db = DB("ofgem.db")
    for item in collect_items():
        # Skip if already seen
        if db.exists(item.get("guid") or item["link"]):
            continue
        fulltext = item.get("content") or ""
        summary, tags = summarise_and_tag(fulltext, title=item["title"])
        item["summary"] = summary
        item["tags"] = ",".join(tags)
        db.insert_item(item)
        print(f"+ Saved: {item['title']}")

if __name__ == "__main__":
    run()
