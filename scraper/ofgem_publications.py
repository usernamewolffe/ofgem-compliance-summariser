# scraper/ofgem_publications.py
from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Iterable, Optional, Tuple, Dict, Any
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

# Default entry points (you can add more later)
DEFAULT_START_URLS = [
    # Small-scale electricity generation publications
    "https://www.ofgem.gov.uk/electricity-generation/"
    "small-scale-electricity-generation/"
    "small-scale-electricity-generation-publications",
    # You can add other libraries here, e.g. RO/REGO/FIT sections
]


def _get(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


def _clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _parse_date(text_or_attr: str) -> Optional[str]:
    """
    Try a few formats and return ISO 8601 UTC string (YYYY-MM-DDTHH:MM:SSZ),
    or None if we can't parse.
    """
    s = (text_or_attr or "").strip()
    if not s:
        return None

    # Try datetime attribute (already ISO)
    # Accepts e.g. 2025-10-09T00:00:00Z or without Z
    m = re.match(r"(\d{4}-\d{2}-\d{2})([ T]\d{2}:\d{2}:\d{2})?Z?", s)
    if m:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass

    # Try common textual formats like: "9 October 2025"
    for fmt in ("%d %B %Y", "%d %b %Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT00:00:00Z")
        except Exception:
            continue
    return None


def _should_keep(published_iso: Optional[str], since_dt: Optional[datetime]) -> bool:
    if not since_dt:
        return True
    if not published_iso:
        # If no date, be conservative and keep
        return True
    try:
        dt = datetime.fromisoformat(published_iso.replace("Z", "+00:00"))
        return dt >= since_dt
    except Exception:
        return True


def _normalize_url(base: str, href: str) -> str:
    return urljoin(base, href or "")


def _add_or_set_query(url: str, **params) -> str:
    parts = list(urlparse(url))
    query = parse_qs(parts[4])
    for k, v in params.items():
        query[k] = [str(v)]
    parts[4] = urlencode(query, doseq=True)
    return urlunparse(parts)


def _extract_cards(soup: BeautifulSoup, page_url: str) -> Iterable[Dict[str, Any]]:
    """
    Ofgem 'publications library' pages typically render cards with:
      - <a href="...">Title</a>
      - <time datetime="YYYY-MM-DD"> or date text nearby
      - type label (e.g., Guidance, Decision, Consultation)
    The exact markup varies, so we try a few patterns.
    """
    # Try common card containers
    candidates = []
    candidates += soup.select("[data-component='publication-card'], .publication-card, article, li")

    seen = set()
    for node in candidates:
        # title/link
        a = node.select_one("a[href]")
        if not a:
            continue
        title = _clean_space(a.get_text())
        href = _normalize_url(page_url, a.get("href"))

        if not title or href in seen:
            continue

        # published date
        t = node.select_one("time[datetime]") or node.find("time")
        published_iso = None
        if t and t.get("datetime"):
            published_iso = _parse_date(t.get("datetime"))
        if not published_iso and t:
            published_iso = _parse_date(t.get_text())

        # type label (badge/pill)
        type_label = None
        # Try common label selectors
        label_el = (
            node.select_one(".ofgem-badge, .badge, .label, .tag, [data-component='tag']") or
            node.find(lambda x: x and x.name in ("span", "div") and "type" in " ".join(x.get("class", [])))
        )
        if label_el:
            type_label = _clean_space(label_el.get_text())

        # Fallback: try to infer type from snippets
        snippet = _clean_space(node.get_text())
        if not type_label:
            for guess in ("Guidance", "Consultation", "Decision", "Call for evidence", "Report"):
                if re.search(rf"\b{re.escape(guess)}\b", snippet, flags=re.I):
                    type_label = guess
                    break

        seen.add(href)
        yield {
            "title": title,
            "link": href,
            "published_at": published_iso,
            "type": type_label or "",
        }


def _find_next_page(soup: BeautifulSoup, current_url: str, page_num: int) -> Optional[str]:
    """
    Try to locate a 'next' link or fall back to incrementing ?page=.
    """
    # Look for an explicit next pagination link
    next_link = soup.find("a", string=re.compile(r"next", re.I)) or soup.select_one("a[rel='next']")
    if next_link and next_link.get("href"):
        return _normalize_url(current_url, next_link["href"])

    # Fallback: try page=N+1
    # Only do this up to a reasonable bound (we'll stop when no cards are found).
    return _add_or_set_query(current_url, page=page_num + 1)


def scrape_ofgem_publications(db, since: Optional[datetime] = None,
                              start_urls: Optional[Iterable[str]] = None,
                              delay_seconds: float = 0.8) -> Tuple[int, int]:
    """
    Crawl Ofgem 'publications library' pages and upsert items into DB.

    Returns (kept_count, skipped_count).
    """
    start_urls = list(start_urls or DEFAULT_START_URLS)
    kept, skipped = 0, 0

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html"})

    for root in start_urls:
        page = 1
        url = root
        while True:
            try:
                soup = _get(session, url)
            except requests.HTTPError as e:
                print(f"[ofgem_pubs] HTTP {e.response.status_code} for {url}")
                break
            except Exception as e:
                print(f"[ofgem_pubs] Error fetching {url}: {e}")
                break

            cards = list(_extract_cards(soup, url))
            if not cards:
                # No results on this page; we’re done with this root
                break

            for c in cards:
                published_iso = c["published_at"]
                if not _should_keep(published_iso, since):
                    skipped += 1
                    continue

                title = c["title"]
                link = c["link"]
                pub_type = c["type"]

                # Derive basic tags
                tags = []
                if pub_type:
                    tags.append(pub_type)
                # You can also add topic hints based on the root URL
                if "small-scale" in root:
                    tags.append("Small-scale generation")

                # Minimal payload; leave summary/content blank (you can enrich later)
                item = {
                    "guid": link,
                    "source": "OFGEM",
                    "title": title,
                    "link": link,
                    "content": "",
                    "summary": "",
                    "published_at": published_iso or "",
                    "tags": tags,
                }

                try:
                    db.upsert_item(item)
                    kept += 1
                except Exception as e:
                    print(f"! Failed to save '{title}': {e}")
                    skipped += 1

            # Next page
            next_url = _find_next_page(soup, url, page)
            # Stop if next page appears to loop to the same URL or we’ve obviously gone too far
            if not next_url or next_url == url or page > 50:
                break
            page += 1
            url = next_url
            time.sleep(delay_seconds)

    return kept, skipped
