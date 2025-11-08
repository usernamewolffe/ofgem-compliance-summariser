# summariser/model.py
import os, re
from typing import List, Tuple, Optional
from django.db import models

# ----------------------------- Django Model ---------------------------------

class Entry(models.Model):
    """A single regulatory update or announcement (e.g. Ofgem, DESNZ, etc.)."""

    source = models.CharField(max_length=50, db_index=True)
    title = models.CharField(max_length=500)
    link = models.URLField(unique=True)  # prevent duplicates by URL
    content = models.TextField(blank=True)
    summary = models.TextField(blank=True)
    tags = models.JSONField(default=list, blank=True)
    published_at = models.DateTimeField(db_index=True)

    class Meta:
        ordering = ["-published_at"]

    def __str__(self):
        return f"{self.source.upper()}: {self.title[:60]}"

    def tag_string(self) -> str:
        return ", ".join(self.tags or [])

    def save(self, *args, **kwargs):
        # Auto-populate summary/tags if omitted (handy for fixtures/backfills)
        if (not self.summary or not self.summary.strip()) or (not self.tags):
            s, t = summarise_and_tag(self.content or "", self.title or "", self.source or None)
            if not self.summary:
                self.summary = s
            if not self.tags:
                self.tags = t
        super().save(*args, **kwargs)

    @classmethod
    def create_or_update(
        cls,
        *,
        source: str,
        title: str,
        link: str,
        content: str,
        published_at,
    ) -> "Entry":
        """Create or update an entry; regenerates summary/tags from content."""
        summary, tags = summarise_and_tag(content, title, source)
        obj, _ = cls.objects.update_or_create(
            link=link,
            defaults={
                "source": source,
                "title": title,
                "content": content,
                "summary": summary,
                "tags": tags,
                "published_at": published_at,
            },
        )
        return obj

# ------------------------ Summary + Tagging Helpers -------------------------

def _fallback_summary(text: str, title: str = "") -> str:
    words = re.findall(r"\w+[^\s]*", text or "")
    snippet = " ".join(words[:60])
    prefix = (title + " — ") if title else ""
    return prefix + snippet + ("..." if len(words) > 60 else "")

RULES = {
    "CAF/NIS": [
        "nis2", " nis ", "network and information", "cyber assessment framework",
        " caf ", "incident reporting", "cyber resilience", "security directive",
        "essential services",
    ],
    "Cyber": [
        "cyber", "malware", "vulnerability", "threat", "phishing", "breach",
        "ransomware", "cve-", "patch tuesday"
    ],
    "Incident": ["incident", "outage", "compromise", "report within 72"],
    "Consultation": ["consultation", "call for evidence", "seeking views"],
    "Guidance": ["guidance", "how to comply", "updated guidance", "good practice"],
    "Enforcement": ["enforcement", "compliance case", "investigation"],
    "Penalty": ["penalty", "fine", "sanction"],
    "Generators": ["generator", "generation", "genco"],
    "Suppliers": ["supplier", "supply licence"],
    "DNOs": ["dno", "distribution network operator"],
    "NIS2": ["nis2"],
}

def _heuristic_tags(text: str, title: str = "", source: Optional[str] = None) -> List[str]:
    blob = f"{title}\n{text}".lower()
    tags = set()
    for tag, needles in RULES.items():
        if any(n in blob for n in needles):
            tags.add(tag)
    if source:
        tags.add(source.upper())  # e.g. OFGEM, DESNZ, NCSC
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
        summary = _fallback_summary(text, title)
        tags = _heuristic_tags(text, title, source)
        return summary, tags

    try:
        prompt = f"""
You are a UK energy regulation analyst.
Summarise the item in exactly 3 concise bullets:
• TL;DR
• Who is affected
• Recommended action
Then output a final line: "Tags: tag1, tag2, tag3".

TITLE: {title}
TEXT:
{text[:6000]}
""".strip()

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Be precise and concise. UK English."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        content = (resp.choices[0].message.content or "").strip()

        # Extract tags from the last "Tags:" line if present
        ll = [ln.strip() for ln in content.splitlines() if ln.strip()]
        gpt_tags: List[str] = []
        for line in reversed(ll):
            if line.lower().startswith("tags:"):
                tag_str = line.split(":", 1)[1]
                gpt_tags = [t.strip() for t in tag_str.split(",") if t.strip()]
                break

        merged = set(gpt_tags) | set(_heuristic_tags(text, title, source))
        return content, sorted(merged)

    except Exception:
        return _fallback_summary(text, title), _heuristic_tags(text, title, source)
