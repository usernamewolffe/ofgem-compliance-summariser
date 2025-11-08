# Ofgem Compliance Summariser (MVP)

A tiny prototype that:
- fetches Ofgem publications/news feeds,
- extracts article text,
- summarises it (OpenAI optional),
- stores results in SQLite,
- serves a simple JSON API (FastAPI).

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

export OPENAI_API_KEY=YOUR_KEY   # optional; without it we use a simple fallback
python main.py                    # scrape once and store to SQLite

# Run the API
uvicorn api.server:app --reload --port 8000
```

Open http://127.0.0.1:8000/items to see stored summaries.

## Design
- **scraper/ofgem.py** — fetch Ofgem RSS and extract article text.
- **summariser/model.py** — summarise & tag. Uses OpenAI if key present; else deterministic fallback.
- **storage/db.py** — very small SQLite helper.
- **api/server.py** — FastAPI read-only endpoints.
- **main.py** — orchestrates one scrape run.

## Notes
- This is an MVP: no auth, no retries to be fancy, and basic HTML text extraction.
- Add more sources by extending `SOURCES` in `scraper/ofgem.py`.
- For production, add a scheduler (e.g., cron or a GitHub Action) to run `python main.py` hourly.
