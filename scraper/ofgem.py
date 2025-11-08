# scraper/ofgem.py
import os
import re
import time
import requests
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from urllib.parse import urljoin, urlparse

# --- Sources (left key becomes the publisher tag) ----------------------------
SOURCES = [
    # Core regulators / policy
    ("ofgem",  "https://www.gov.uk/government/organisations/ofgem.atom"),
    ("desnz",  "https://www.gov.uk/government/organisations/department-for-energy-security-and-net-zero.atom"),
    ("ea",     "https://www.gov.uk/government/organisations/environment-agency.atom"),
    ("hse",    "https://press.hse.gov.uk/feed/"),  # HSE media centre

    # Grid / markets / codes
    ("elexon", "https://www.elexon.co.uk/feed/"),
    ("dcode_mods_html",     "https://dcode.org.uk/dcode-modifications/"),
    ("dcode_consults_html", "https://dcode.org.uk/consultations/open-consultations/"),
    ("ena_html",            "https://www.energynetworks.org/all-news-and-updates"),
    ("neso_html",           "https://www.neso.energy/news-and-events/media-centre"),  # NESO = ESO successor

    # Cyber / data
    ("ncsc",     "https://www.ncsc.gov.uk/api/1/services/v1/all-rss-feed.xml"),
    ("ico",      "https://www.gov.uk/government/organisations/information-commissioner-s-office.atom"),
    ("ico_html", "https://ico.org.uk/about-the-ico/media-centre/news-and-blogs/"),

    # Industry
    ("rea", "https://www.r-e-a.net/feed/"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# --- Per-source filters ------------------------------------------------------
FILTERS = {
    "ofgem": {
        "include": [
            "licence", "licensing", "standard conditions", "consultation",
            "guidance", "enforcement", "penalty", "heat network", "generator",
            "generation licence", "embedded generation", "connection",
            "incident", "caf", "nis2", "cyber",
        ],
        "exclude": [],
    },
    "desnz": {
        "include": [
            "contracts for difference", "cfd", "renewables obligation", "ro",
            "heat network", "chp", "funding", "grant", "consultation",
            "grid connection", "transmission", "npsc",
        ],
        "exclude": [],
    },
    "ea": {
        "include": [
            "permit", "permitting", "environmental permit", "emissions",
            "flood", "abstraction", "discharge", "spill", "compliance",
        ],
        "exclude": [],
    },
    # HSE: broaden so press releases get through
    "hse": {
        "include": [
            "electric", "electrical", "pressure system", "lifting", "turbine",
            "generator", "safety alert", "prosecution", "fine", "enforcement",
        ],
        "exclude": [],
    },
    "elexon": {
        "include": [
            "bsc", "settlement", "meter", "metering", "imbalance",
            "market-wide half-hourly", "mhhs", "modification",
            "change proposal", "re:P\\d{3}\\b",
        ],
        "exclude": [],
    },
    "dcode": {
        "include": [
            "distribution code", "engineering recommendation", "g59", "g98",
            "g99", "connection", "fault ride through", "embedded generation",
            "type test", "consultation", "modification", "proposal",
        ],
        "exclude": [],
    },
    "ena": {
        "include": [
            "engineering recommendation", "er p2", "er p28", "cyber",
            "resilience", "connection", "network code", "open networks",
        ],
        "exclude": [],
    },
    "neso": {
        "include": [
            "grid", "system operator", "operability", "connections", "queue",
            "winter outlook", "balancing", "constraint", "intertrip",
            "grid code", "cusc", "future energy scenarios", "network map",
        ],
        "exclude": [],
    },
    "ncsc": {
        "include": [
            "industrial control", "ics", "scada", "ot", "operational technology",
            "energy", "electric", "power", "cve-", "ransom", "malware",
            "vulnerability", "patch",
        ],
        "exclude": [],
    },
    "ico": {
        "include": ["breach", "security", "incident", "guidance", "enforcement", "fine", "penalty"],
        "exclude": ["job", "vacancy", "podcast", "webinar", "event"],
    },
    "rea": {
        "include": [
            "wind", "hydro", "chp", "biomass", "renewable", "planning",
            "grid connection", "business rates", "support scheme", "funding",
        ],
        "exclude": [],
    },
}

BYPASS = os.getenv("BYPASS_FILTERS", "0") == "1"

# --- Core utils --------------------------------------------------------------

def _clean_text(html: str) -> str:
    if not isinstance(html, str):
        return ""
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return re.sub(r"\s+", " ", soup.get_text(separator=" ")).strip()

def _fetch(url: str, tries: int = 3, backoff: float = 1.6) -> str:
    last_err = None
    for i in range(tries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=25)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            last_err = e
            if i < tries - 1:
                time.sleep(backoff ** i)
    raise last_err

def _extract_article(url: str) -> str:
    try:
        html = _fetch(url)
        return _clean_text(html)
    except Exception:
        return ""

def _parse_date(dstr):
    if not dstr:
        return None
    try:
        return dateparser.parse(dstr).isoformat()
    except Exception:
        return None

def _pick_summary(entry) -> str:
    if entry.get("summary"):
        return entry["summary"]
    if entry.get("description"):
        return entry["description"]
    if entry.get("content") and isinstance(entry["content"], list) and entry["content"]:
        for c in entry["content"]:
            val = c.get("value")
            if val:
                return val
    return ""

def _match(pattern: str, blob: str) -> bool:
    if pattern.startswith("re:") or pattern.startswith("re:".upper()):
        pat = pattern.split(":", 1)[1]
        try:
            return re.search(pat, blob, flags=re.IGNORECASE) is not None
        except re.error:
            return False
    return pattern.lower() in blob

def _passes_filters(source: str, title: str, body: str) -> bool:
    if BYPASS:
        return True
    cfg = FILTERS.get(source, {})
    inc = cfg.get("include") or []
    exc = cfg.get("exclude") or []
    blob = f"{title}\n{body}".lower()
    if inc and not any(_match(p, blob) for p in inc):
        return False
    if exc and any(_match(p, blob) for p in exc):
        return False
    return True

# --- HTML scrapers -----------------------------------------------------------

def _jsonld_news(soup):
    import json
    seen = set()
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        txt = (tag.string or tag.get_text() or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for obj in items:
            if not isinstance(obj, dict):
                continue
            t = (obj.get("@type") or "").lower()
            if t in ("newsarticle", "blogposting"):
                link = obj.get("url") or obj.get("mainEntityOfPage")
                title = (obj.get("headline") or obj.get("name") or "").strip()
                date_text = obj.get("datePublished") or obj.get("dateModified")
                if link and title and link not in seen:
                    seen.add(link)
                    yield {
                        "link": link,
                        "title": title,
                        "id": link,
                        "published": date_text,
                        "summary": obj.get("description") or "",
                    }

def _find_date_text(node):
    t = node.find("time", attrs={"datetime": True})
    if t and (t.get("datetime") or t.get_text(strip=True)):
        return t.get("datetime") or t.get_text(strip=True)
    t = node.find("time")
    if t and t.get_text(strip=True):
        return t.get_text(strip=True)
    dt = node.select_one(".date, .c-meta__date, .c-card__meta time, .c-card time")
    if dt:
        return dt.get("datetime") or dt.get_text(strip=True)
    return None

def _scrape_ico_news(list_url: str, max_pages: int = 2):
    results, url = [], list_url
    for _ in range(max_pages):
        try:
            html = _fetch(url)
        except Exception:
            break
        soup = BeautifulSoup(html, "html.parser")

        for item in _jsonld_news(soup):
            results.append(item)

        link_selectors = [
            'a.c-card__link[href*="/news-and-blogs/"]',
            'article a[href*="/news-and-blogs/"]',
            'a[href*="/about-the-ico/media-centre/news-and-blogs/"]',
            'a[href*="/news/"]',
            'a[href*="/blog/"]',
        ]
        seen = {r["link"] for r in results}
        for sel in link_selectors:
            for a in soup.select(sel):
                href = a.get("href")
                title = (a.get_text(strip=True) or "").strip()
                if not href or not title:
                    continue
                link = urljoin(url, href)
                if link in seen:
                    continue
                seen.add(link)
                container = a
                for _ in range(3):
                    if container.parent:
                        container = container.parent
                date_text = _find_date_text(container) or _find_date_text(a.parent or container)
                results.append({"link": link, "title": title, "id": link, "published": date_text, "summary": ""})

        next_link = soup.find("a", attrs={"rel": "next"}) or soup.find("a", string=lambda s: s and "Next" in s)
        if next_link and next_link.get("href"):
            url = urljoin(url, next_link["href"])
        else:
            break

    out, seen = [], set()
    for r in results:
        if r["link"] in seen:
            continue
        seen.add(r["link"])
        out.append(r)
    return out

def _looks_like_page(url: str) -> bool:
    """Allow DCode index/year pages as useful entries (not just PDFs)."""
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in (".html", "/")) or path.count("/") >= 3

def _scrape_dcode_list(url: str):
    html = _fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    sels = [
        "main a", "article a", ".entry-content a", "table a", "ul li a", "ol li a",
    ]
    seen = set()
    for sel in sels:
        for a in soup.select(sel):
            title = (a.get_text(strip=True) or "").strip()
            href = a.get("href")
            if not title or not href:
                continue
            link = urljoin(url, href)
            if link in seen:
                continue
            seen.add(link)
            # Keep index/year pages and individual items; drop pure anchors
            if not href.startswith("#") and _looks_like_page(link):
                yield {"link": link, "title": title, "id": link, "published": None, "summary": ""}

def _scrape_ena_news(url: str):
    html = _fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a[href*='/newsroom/'], a[href*='/all-news-and-updates/'], article a"):
        href = a.get("href")
        title = (a.get_text(strip=True) or "").strip()
        if not href or not title:
            continue
        link = urljoin(url, href)
        yield {"link": link, "title": title, "id": link, "published": None, "summary": ""}

def _scrape_neso_news(url: str):
    html = _fetch(url)
    soup = BeautifulSoup(html, "html.parser")
    # NESO media centre/press release cards + generic fallbacks
    for a in soup.select("a[href*='/news-and-events/'], a[href*='/press-release'], article a"):
        href = a.get("href")
        title = (a.get_text(strip=True) or "").strip()
        if not href or not title:
            continue
        link = urljoin(url, href)
        yield {"link": link, "title": title, "id": link, "published": None, "summary": ""}

# --- Main collector ----------------------------------------------------------

def collect_items():
    for source_name, feed_url in SOURCES:
        src = (source_name or "").strip().lower()

        # Route special HTML sources first
        if src == "ico_html":
            try:
                parsed_entries = list(_scrape_ico_news(feed_url))
                print(f"[{src}] (html) {len(parsed_entries)} entries fetched")
            except Exception as e:
                print(f"[{src}] HTML scrape error: {e}")
                continue
            base_src = "ico"

        elif src in ("dcode_mods_html", "dcode_consults_html"):
            try:
                parsed_entries = list(_scrape_dcode_list(feed_url))
                print(f"[{src}] (html) {len(parsed_entries)} entries fetched")
            except Exception as e:
                print(f"[{src}] HTML scrape error: {e}")
                continue
            base_src = "dcode"

        elif src == "ena_html":
            try:
                parsed_entries = list(_scrape_ena_news(feed_url))
                print(f"[{src}] (html) {len(parsed_entries)} entries fetched")
            except Exception as e:
                print(f"[{src}] HTML scrape error: {e}")
                continue
            base_src = "ena"

        elif src == "neso_html":
            try:
                parsed_entries = list(_scrape_neso_news(feed_url))
                print(f"[{src}] (html) {len(parsed_entries)} entries fetched")
            except Exception as e:
                print(f"[{src}] HTML scrape error: {e}")
                continue
            base_src = "neso"

        else:
            # Default: Atom/RSS
            try:
                xml = _fetch(feed_url)
                d = feedparser.parse(xml)
                parsed_entries = d.entries
                print(f"[{src}] {len(parsed_entries)} entries fetched")
            except Exception as e:
                print(f"[{src}] Feed error: {e}")
                continue
            base_src = src

        kept = skipped = 0
        for e in parsed_entries:
            link = e.get("link")
            title = (e.get("title") or "").strip()
            guid = e.get("id") or link or title
            published = (
                _parse_date(e.get("published"))
                or _parse_date(e.get("updated"))
                or _parse_date(e.get("issued"))
            )

            summary_html = e.get("summary") or e.get("description") or ""
            content = _clean_text(summary_html) if isinstance(summary_html, str) and summary_html else ""
            if not content and link:
                content = _extract_article(link)

            if not _passes_filters(base_src, title, content):
                skipped += 1
                continue

            kept += 1
            yield {
                "source": base_src,
                "link": link,
                "guid": guid,
                "title": title,
                "published_at": published,
                "content": content,
                "tags": base_src.upper(),
            }

        print(f"[{src}] kept {kept} · skipped {skipped}")

# -----------------------------------------------------------------------------


if __name__ == "__main__":
    import itertools, json
    items = list(itertools.islice(collect_items(), 12))
    print(json.dumps(items, indent=2)[:3000])
