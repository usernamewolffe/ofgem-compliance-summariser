# api/ai_summary.py
import os
import re
import io
import json
from typing import Optional, List
from urllib.parse import urlparse

import requests
from openai import OpenAI

# ---- Fallback ----

def fallback_ai_summary(text: str, limit_words: int = 100) -> str:
    words = (text or "").split()
    snippet = " ".join(words[:limit_words])
    return snippet + ("…" if len(words) > limit_words else "")


def openai_client():
    try:
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            print("[AI] ⚠️ No OPENAI_API_KEY found in environment")
            return None
        print("[AI] ✅ OpenAI API key found, creating client")
        return OpenAI(api_key=key)
    except Exception as e:
        print(f"[AI] ❌ Failed to create OpenAI client: {e}")
        return None


# ---- Cleaning / boilerplate ----

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

    kept: List[str] = []
    ttl = (title or "").strip()
    ttl_low = ttl.lower()

    for ln in lines:
        if not ln:
            continue
        if _BP_REGEX.search(ln):
            continue
        if len(ln) <= 3:
            continue
        if len(ln) <= 18 and not ln.endswith((".", ":", "?", "!", "…")):
            continue
        if ttl and ln.lower() == ttl_low:
            continue
        kept.append(ln)

    cleaned = "\n".join(kept)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    if len(cleaned) < 200:
        cleaned = text.strip()
    if len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars]
    return cleaned


def is_boilerplate_summary(text: str) -> bool:
    if not text:
        return True
    bad_snippets = [
        "skip to main content",
        "user account menu",
        "reset button in search",
        "data portal",
        "sign in / register",
        "show/hide menu",
        "main navigation",
        "cookies",
    ]
    t = text.lower().strip()
    return any(snip in t for snip in bad_snippets)


# ---- Core generate function ----

def generate_ai_summary(
    title: str,
    text: str,
    limit_words: int = 100,
    guid: Optional[str] = None,
) -> str:
    print(f"[AI] 🔎 Generating summary guid={guid} title={title[:60]!r} len={len(text)}")
    text = (text or "").strip()
    if not text:
        print("[AI] ⚠️ No text provided to summarise.")
        return "No content available to summarise."

    client = openai_client()
    if not client:
        print("[AI] ⚠️ No OpenAI client available — using fallback snippet.")
        return fallback_ai_summary(text, limit_words)

    prompt = f"""Summarise the following item in up to {limit_words} words.
Plain UK English, no bullet points, no headings. Cover what it is, who it affects, and likely action/implication.

TITLE: {title}
TEXT:
{text[:6000]}
"""
    try:
        print("[AI] 🧠 Sending request to OpenAI API...")
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a precise UK energy regulation analyst.",
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        out = (resp.choices[0].message.content or "").strip()
        print("[AI] ✅ Received summary from OpenAI")
        words = out.split()
        if len(words) > limit_words:
            out = " ".join(words[:limit_words]) + "…"
        return out
    except Exception as e:
        print(f"[AI] ❌ OpenAI request failed: {e}")
        return fallback_ai_summary(text, limit_words)
