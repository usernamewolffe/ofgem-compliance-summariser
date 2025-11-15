# summariser/model.py
import os, re
from typing import List, Tuple, Optional

def _fallback_summary(text: str, title: str = "") -> str:
    words = re.findall(r"\w+[^\s]*", text or "")
    snippet = " ".join(words[:100])
    prefix = (title + " — ") if title else ""
    return prefix + snippet + ("…" if len(words) > 100 else "")

RULES = {
    "CAF/NIS": ["nis2", "network and information", "cyber assessment framework"],
    "Cyber": ["cyber", "malware", "vulnerability", "threat", "phishing"],
    "Incident": ["incident", "outage", "compromise"],
    "Guidance": ["guidance", "good practice"],
    "Enforcement": ["enforcement", "compliance case"],
}

def _heuristic_tags(text: str, title: str = "", source: Optional[str] = None) -> List[str]:
    blob = f"{title}\n{text}".lower()
    tags = set()
    for tag, needles in RULES.items():
        if any(n in blob for n in needles):
            tags.add(tag)
    if source:
        tags.add(source.upper())
    return sorted(tags)

def _openai_client():
    try:
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        return OpenAI(api_key=api_key)
    except Exception:
        return None

def summarise_and_tag(text: str, title: str = "", source: Optional[str] = None) -> Tuple[str, List[str]]:
    text = text or ""
    client = _openai_client()

    if not client or not text.strip():
        return _fallback_summary(text, title), _heuristic_tags(text, title, source)

    try:
        prompt = f"""
Summarise the item in up to 100 words in UK English.
Focus on what it is, who’s affected, and any required action.

TITLE: {title}
TEXT:
{text[:6000]}
"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Be precise, plain UK English."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        summary = (resp.choices[0].message.content or "").strip()
        tags = _heuristic_tags(text, title, source)
        return summary, tags
    except Exception:
        return _fallback_summary(text, title), _heuristic_tags(text, title, source)
