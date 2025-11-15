# tools/ai_utils.py
import os, re, io, requests
from urllib.parse import urlparse

# --- Boilerplate cleaner for extracted text ---
_BOILERPLATE_PATTERNS = [
    r"\bskip to (main )?content\b",
    r"\b(main )?navigation\b",
    r"\b(show/?hide|toggle) menu\b",
    r"\b(sign in|register|log ?in|log ?out)\b",
    r"\b(search|search results|reset button in search)\b",
    r"\b(cookie(s)? (banner|settings|preferences)|accept all cookies)\b",
    r"\b(user account menu)\b",
    r"\bfooter\b",
    r"\bshare (this )?page\b",
    r"\brelated (content|links)\b",
    r"\bdata portal\b",
]
_BP_REGEX = re.compile("|".join(_BOILERPLATE_PATTERNS), re.IGNORECASE)

def clean_extracted_text(title: str, text: str, max_chars: int = 12000) -> str:
    if not text:
        return text
    raw = re.sub(r"[ \t]+", " ", text)
    raw = re.sub(r"\r\n?", "\n", raw)
    lines = [ln.strip() for ln in raw.split("\n")]

    kept = []
    ttl = (title or "").strip().lower()
    for ln in lines:
        if not ln:
            continue
        if _BP_REGEX.search(ln):
            continue
        if len(ln) <= 3:
            continue
        if len(ln) <= 18 and not ln.endswith((".", ":", "?", "!", "…")):
            continue
        if ttl and ln.lower() == ttl:
            continue
        kept.append(ln)

    cleaned = "\n".join(kept)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    if len(cleaned) < 200:
        cleaned = text.strip()
    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars]
    return cleaned

def is_pdf_link(url: str) -> bool:
    try:
        return urlparse(url).path.lower().endswith(".pdf")
    except Exception:
        return False

def fetch_pdf_bytes(url: str, timeout: int = 30) -> bytes:
    r = requests.get(url, timeout=timeout, allow_redirects=True, stream=True)
    r.raise_for_status()
    total = 0
    chunks = []
    for chunk in r.iter_content(65536):
        if not chunk:
            break
        total += len(chunk)
        if total > 15 * 1024 * 1024:
            break
        chunks.append(chunk)
    return b"".join(chunks)

def pdf_to_text(blob: bytes, max_pages: int = 8) -> str:
    try:
        from pypdf import PdfReader
    except Exception:
        return ""
    try:
        reader = PdfReader(io.BytesIO(blob))
        out = []
        for i, page in enumerate(reader.pages):
            if i >= max_pages:
                break
            try:
                out.append(page.extract_text() or "")
            except Exception:
                pass
        return "\n".join([x for x in out if x]).strip()
    except Exception:
        return ""

def openai_client():
    try:
        from openai import OpenAI
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            return None
        return OpenAI(api_key=key)
    except Exception:
        return None

def fallback_summary(text: str, limit_words: int = 100) -> str:
    words = (text or "").split()
    snippet = " ".join(words[:limit_words])
    return snippet + ("…" if len(words) > limit_words else "")

def generate_ai_summary(title: str, text: str, limit_words: int = 100) -> str:
    text = (text or "").strip()
    if not text:
        return "No content available to summarise."
    client = openai_client()
    if not client:
        return fallback_summary(text, limit_words)

    prompt = f"""Summarise the following item in up to {limit_words} words.
Plain UK English, no bullet points, no headings. Cover what it is, who it affects, and likely action/implication.

TITLE: {title}
TEXT:
{text[:6000]}
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise UK energy regulation analyst."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        out = (resp.choices[0].message.content or "").strip()
        words = out.split()
        if len(words) > limit_words:
            out = " ".join(words[:limit_words]) + "…"
        return out
    except Exception:
        return fallback_summary(text, limit_words)
