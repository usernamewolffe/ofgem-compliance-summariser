import os, re, textwrap
from typing import List, Tuple

def _fallback_summary(text: str, title: str = "") -> str:
    # Simple deterministic fallback: first ~60 words + title cue
    words = re.findall(r"\w+[^\s]*", text)
    snippet = " ".join(words[:60])
    return (title + " — " if title else "") + snippet + ("..." if len(words) > 60 else "")

def _heuristic_tags(text: str) -> List[str]:
    tags = []
    lower = text.lower()
    KEYWORDS = {
        "nis2": "NIS2",
        "cyber": "Cyber",
        "incident": "Incident",
        "consultation": "Consultation",
        "guidance": "Guidance",
        "enforcement": "Enforcement",
        "penalty": "Penalty",
        "generator": "Generators",
        "supplier": "Suppliers",
        "dno": "DNOs",
    }
    for k, v in KEYWORDS.items():
        if k in lower:
            tags.append(v)
    return sorted(set(tags))

def _openai_client():
    try:
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None
        return OpenAI(api_key=api_key)
    except Exception:
        return None

def summarise_and_tag(text: str, title: str = "") -> Tuple[str, list]:
    client = _openai_client()
    if not client or not text.strip():
        return _fallback_summary(text, title), _heuristic_tags(text)
    try:
        prompt = f"""
You are a UK energy regulation analyst.
Summarise the following Ofgem item in 3 bullets: TL;DR, Who is affected, Recommended action.
Then output a final line: Tags: comma separated short tags.

TITLE: {title}
TEXT:
{text[:6000]}
"""
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Be precise and concise. UK English."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        content = resp.choices[0].message.content.strip()
        # try to split out tags
        tags = []
        for line in content.splitlines()[::-1]:
            if line.lower().startswith("tags:"):
                tag_str = line.split(":", 1)[1]
                tags = [t.strip() for t in tag_str.split(",") if t.strip()]
                break
        if not tags:
            tags = _heuristic_tags(text)
        return content, tags
    except Exception:
        return _fallback_summary(text, title), _heuristic_tags(text)
