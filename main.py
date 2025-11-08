# main.py
import argparse
from datetime import datetime, timezone, timedelta
from scraper.ofgem import collect_items
from summariser.model import summarise_and_tag
from storage.db import DB

def run(days_since: int | None = None):
    db = DB("ofgem.db")
    cutoff = None
    if days_since:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_since)
        print(f"Only saving items published after {cutoff.isoformat()}")

    saved = skipped = failed = 0

    for item in collect_items():
        guid = item.get("guid") or item.get("link")
        if not guid:
            skipped += 1
            continue

        # Filter by age
        if cutoff and item.get("published_at"):
            try:
                pub = datetime.fromisoformat(item["published_at"].replace("Z", "+00:00"))
                if pub < cutoff:
                    skipped += 1
                    continue
            except Exception:
                pass  # bad date → keep

        # Skip if already seen
        if db.exists(guid):
            skipped += 1
            continue

        title = item.get("title") or ""
        source = (item.get("source") or "").strip()
        fulltext = item.get("content") or ""

        # Summarise + tag
        summary, tags = summarise_and_tag(
            fulltext,
            title=title,
            source=source,
        )
        tagset = set(t.strip() for t in (tags or []) if t)
        if source:
            tagset.add(source.upper())

        # Normalise fields
        item["guid"] = guid
        item["title"] = title
        item["source"] = source or "unknown"
        item["summary"] = summary
        item["tags"] = ",".join(sorted(tagset))
        item["published_at"] = item.get("published_at") or datetime.now(timezone.utc).isoformat()
        item["content"] = fulltext

        try:
            db.insert_item(item)
            saved += 1
            print(f"+ Saved: {title[:90]}")
        except Exception as e:
            failed += 1
            print(f"! Failed to save '{title[:90]}': {e}")

    print(f"\nDone. Saved: {saved} · Skipped: {skipped} · Failed: {failed}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and summarise Ofgem/DESNZ/NCSC/ICO feeds")
    parser.add_argument("--since", type=int, help="Only save items published in the last N days")
    args = parser.parse_args()
    run(days_since=args.since)
