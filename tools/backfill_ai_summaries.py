#!/usr/bin/env python
"""
Backfill script to generate AI summaries for items without ai_summary.

Usage:
    python tools/backfill_ai_summaries.py
"""

import os
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "ofgem.db"


# ---------------------------------------------------------------------------
# OpenAI helpers (standalone, similar to server.py)
# ---------------------------------------------------------------------------

def _openai_client() -> Optional[OpenAI]:
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        print("[AI] ⚠️ No OPENAI_API_KEY found in environment")
        return None

    try:
        client = OpenAI(api_key=key)
        print("[AI] ✅ OpenAI client created")
        return client
    except Exception as e:
        print(f"[AI] ❌ Failed to create OpenAI client: {e}")
        return None


def _extract_text_from_response(resp) -> str:
    try:
        return resp.output[0].content[0].text.value.strip()
    except Exception as e:
        print(f"[AI] ❌ Failed to extract text from response: {e}")
        return ""


def _generate_ai_summary(
    client: OpenAI,
    *,
    title: str,
    url: str,
    body: str,
    max_tokens: int = 220,
) -> Optional[str]:
    if not body or not body.strip():
        print("[AI] ⚠️ No text provided to summarise.")
        return None

    system_prompt = (
        "You are an expert summariser for UK energy regulation and Ofgem-related "
        "content. Produce a short, plain-English summary (3–6 bullet points or a "
        "compact paragraph) that highlights the main regulatory/compliance points, "
        "key dates, affected parties, and any actions that regulated energy "
        "companies should consider."
    )

    user_prompt = (
        f"Title: {title}\n"
        f"Source: {url}\n\n"
        "Full text:\n"
        f"{body}\n\n"
        "Summarise this for a busy compliance officer at an energy company. "
        "Focus on regulatory impact, obligations, deadlines, and risks."
    )

    try:
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_output_tokens=max_tokens,
        )
        summary = _extract_text_from_response(resp)
        if summary:
            print("[AI] ✅ Summary generated")
            return summary
        else:
            print("[AI] ⚠️ Empty summary returned from API")
            return None
    except Exception as e:
        print(f"[AI] ❌ OpenAI request failed: {e}")
        return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def main():
    print("\n=== AI BACKFILL SCRIPT STARTED ===")
    print(f"[AI-BACKFILL] Using DB: {DB_PATH}")

    client = _openai_client()
    if not client:
        print("[AI-BACKFILL] ❌ No OpenAI client – aborting.")
        return

    conn = _get_conn()
    cur = conn.cursor()

    # Find items that need AI summaries
    cur.execute(
        """
        SELECT id, title, content, ai_summary, guid_or_link
        FROM items
        WHERE (ai_summary IS NULL OR TRIM(ai_summary) = '')
        ORDER BY id ASC
        """
    )
    rows = cur.fetchall()
    total = len(rows)
    print(f"[AI-BACKFILL] Found {total} items needing summaries")

    if not rows:
        print("[AI-BACKFILL] No items needing AI summaries.")
        return

    updated_count = 0

    for idx, row in enumerate(rows, start=1):
        row = dict(row)
        title = row.get("title") or ""
        url = row.get("guid_or_link") or ""
        body = row.get("content") or ""

        print(
            f"[AI] 🔎 ({idx}/{total}) Generating summary "
            f"id={row['id']} title='{title[:60]}' len={len(body)}"
        )

        summary = _generate_ai_summary(
            client,
            title=title,
            url=url,
            body=body,
        )

        if summary:
            cur.execute(
                "UPDATE items SET ai_summary = ? WHERE id = ?",
                (summary, row["id"]),
            )
            conn.commit()
            updated_count += 1
            print(f"[AI-BACKFILL] Updated id={row['id']}")
        else:
            print(f"[AI-BACKFILL] Skipped id={row['id']} (no summary)")

    print(f"[AI-BACKFILL] Done. Updated {updated_count} items.")


if __name__ == "__main__":
    main()
