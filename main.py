# main.py
from __future__ import annotations

import argparse
from datetime import datetime, timezone, timedelta

from storage.db import DB
from summariser.model import summarise_and_tag
from scraper.ofgem import collect_items
from scraper.ofgem_publications import scrape_ofgem_publications


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(days_since: int | None = None) -> None:
    db = DB("ofgem.db")

    since_dt = None
    if days_since:
        since_dt = datetime.now(timezone.utc) - timedelta(days=days_since)
        print(f"Only saving items published after {since_dt.isoformat()}")

    saved = skipped = failed = 0

    # ---------------------------
    # 1) Existing scraper (RSS/GOV.UK style)
    # ---------------------------
    print("[ofgem] collecting items…")
    for item in collect_items():
        guid = item.get("guid") or item.get("link")
        if not guid:
            skipped += 1
            continue

        # Skip if older than cutoff
        if since_dt and item.get("published_at"):
            try:
                pub = datetime.fromisoformat(str(item["published_at"]).replace("Z", "+00:00"))
                if pub < since_dt:
                    skipped += 1
                    continue
            except Exception:
                pass  # keep if date is malformed

        # Skip if already saved
        if db.exists(guid):
            skipped += 1
            continue

        title = (item.get("title") or "").strip()
        source = (item.get("source") or "").strip()
        fulltext = item.get("content") or ""

        # Summarise + tag (returns summary text and a list[str] of tags)
        try:
            summary, tags = summarise_and_tag(fulltext, title=title, source=source)
        except Exception as e:
            # Be resilient
            summary, tags = "", []

        # Normalise payload and keep tags as a LIST (important!)
        payload = {
            "guid": guid,
            "source": source or "UNKNOWN",
            "title": title,
            "link": item.get("link") or "",
            "content": fulltext,
            "summary": summary,
            "published_at": item.get("published_at") or _iso_now(),
            "tags": list(sorted({t.strip() for t in (tags or []) if t.strip()})),
        }

        try:
            db.upsert_item(payload)
            saved += 1
            print(f"+ Saved: {title[:90]}")
        except Exception as e:
            failed += 1
            print(f"! Failed to save '{title[:90]}': {e}")

    # ---------------------------
    # 2) New Ofgem publications library crawler
    # ---------------------------
    print("[ofgem_publications] crawling library pages…")
    k, s = scrape_ofgem_publications(db, since=since_dt)
    print(f"[ofgem_publications] kept {k} · skipped {s}")
    saved += k
    skipped += s

    print(f"\nDone. Saved: {saved} · Skipped: {skipped} · Failed: {failed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and summarise Ofgem/energy sector feeds")
    parser.add_argument("--since", type=int, help="Only save items published in the last N days")
    args = parser.parse_args()
    run(days_since=args.since)
