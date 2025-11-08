import feedparser, requests, re, time
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as dateparser

SOURCES = [
    # Publications + news RSS (representative)
    "https://www.ofgem.gov.uk/rss/news-and-views.xml",
    "https://www.ofgem.gov.uk/rss/publications.xml",
]

def _clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # remove scripts and styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator=" ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _fetch(url: str) -> str:
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.text

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

def collect_items():
    for feed_url in SOURCES:
        d = feedparser.parse(feed_url)
        for e in d.entries:
            link = e.get("link")
            title = e.get("title", "").strip()
            guid = e.get("id") or link
            published = _parse_date(e.get("published") or e.get("updated"))
            content = ""
            # try content from feed; otherwise fetch page
            if e.get("summary"):
                content = _clean_text(e["summary"])
            if not content and link:
                content = _extract_article(link)
            yield {
                "source": "ofgem",
                "link": link,
                "guid": guid,
                "title": title,
                "published_at": published,
                "content": content,
            }
