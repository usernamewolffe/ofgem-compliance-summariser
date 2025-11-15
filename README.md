# Ofgem Compliance Summariser

The Ofgem Compliance Summariser pulls compliance-relevant news from UK energy regulators, distils each article into an actionable summary, and serves the results through a small FastAPI application with a lightweight compliance workspace (organisations, sites, controls, and risk register tooling).

## Key features

- **Automated data collection** – RSS/Atom feeds and HTML pages from Ofgem, DESNZ, the Environment Agency, HSE, NCSC and other energy market bodies are downloaded, normalised, deduplicated, and topic tagged to reduce noise.
- **Summaries & tags** – Each article is summarised in up to 100 words using OpenAI (if an API key is available) with a deterministic fallback. Simple heuristics add topic labels and ensure we still get a useful summary when the AI service is unavailable.
- **Persistent storage** – Items, saved filters, framework controls, organisations, sites, controls, and risks are stored in SQLite with schema migrations handled programmatically. Upserts, indexes, and helper accessors live in `storage/db.py` for both API and tooling usage.
- **FastAPI UI + JSON API** – `api/server.py` exposes JSON feeds (`/api/items`, `/api/feed`, `/api/feed.csv`) and a small authenticated UI for browsing summaries, switching organisations, managing org members/sites, logging controls, and maintaining a risk register. Sessions use signed cookies with configurable inactivity timeouts.
- **Compliance workflows** – Web pages let you capture org/site metadata, assign owners to controls, capture risk status and treatments, and link articles to framework controls for traceability.
- **Email sharing & AI utilities** – Articles can be emailed via SendGrid, and helper scripts pre-compute AI summaries (including PDF text extraction) so the UI responds instantly.

## Repository layout

```
.
├── api/                 # FastAPI app, routers, templates, auth/session logic
├── scraper/             # Feed collectors and Ofgem publications crawler
├── summariser/          # Summarisation helpers and Jinja templates
├── storage/             # SQLite wrapper and schema migrations
├── tools/               # Utility scripts (email, AI helpers, pre-compute summaries)
├── scripts/             # Admin scripts (indexes, tag normalisation)
├── web/static/          # Tailwind-generated CSS assets
├── main.py              # CLI entry point for one-off scraping runs
├── requirements.txt     # Python dependencies
├── package.json         # Tailwind build dependencies
└── tailwind.config.js   # CSS build configuration
```

## Quick start

1. **Create and activate a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate            # Windows: .venv\Scripts\activate
   ```
2. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **(Optional) Install Node dependencies for Tailwind builds**
   ```bash
   npm install
   ```
4. **Create a `.env` file** with any environment variables you need (see [Environment variables](#environment-variables)).
5. **Run the scraper** to populate the SQLite database:
   ```bash
   python main.py --since 7   # only keep items from the last 7 days
   ```
6. **Start the API + UI**:
   ```bash
   uvicorn api.server:app --reload --port 8000
   ```
7. Open <http://127.0.0.1:8000/summaries> for the browser UI or <http://127.0.0.1:8000/api/items> for raw JSON.

## Scraper workflow

- `main.py` orchestrates the run: it collects RSS/HTML items, summarises them, tags them, upserts each record into SQLite, and then crawls Ofgem’s publications library via `scraper/ofgem_publications.py` to fill any gaps.
- Use `--since <days>` to skip older content when backfilling an existing database.
- The scraper automatically avoids duplicates (based on `guid`) and retries fallbacks if OpenAI fails.

## Running the web experience

- The FastAPI app serves both JSON endpoints and HTML templates located in `summariser/templates`. Static assets live under `web/static` and are served from `/static` by FastAPI.
- Authentication uses session cookies backed by SQLite tables. Sign up via `/account/register`, then log in to manage organisations, sites, controls, and risk entries.
- Use the organisation switcher (`/account/switch-org`) to view data for different tenants if your account belongs to multiple organisations.
- Articles can be linked to framework controls and exported through CSV endpoints for further analysis.

### Tailwind CSS assets

Tailwind is optional; the repository already includes a compiled stylesheet. To rebuild after editing templates:
```bash
npx tailwindcss -i web/static/css/input.css -o web/static/css/tailwind.css --watch
```
The Tailwind configuration scans the FastAPI Python files and Jinja templates for class names.

## Environment variables

| Variable | Description | Default |
| --- | --- | --- |
| `OPENAI_API_KEY` | Enables GPT-based summarisation and AI summary refresh endpoints. Without it the heuristic fallback is used. | _unset_ |
| `DB_PATH` | Override the SQLite database path used by both scraper and API. | `ofgem.db` |
| `BYPASS_FILTERS` | Set to `1` to disable per-source include/exclude filtering in the scraper. | `0` |
| `INACTIVITY_SECONDS` | Session inactivity timeout in seconds. | `10800` (3h) |
| `SESSION_COOKIE` | Name of the session cookie. | `ofgem_session` |
| `SESSION_MAX_AGE` | Maximum cookie age before expiry. | `10800` (3h) |
| `SESSIONS_SECRET` | Secret used to sign session cookies. | `dev-only-change-me` |
| `DEV_USER` | Default email used in development helper flows. | `andrewpeat@example.com` |
| `SENDGRID_API_KEY` | Enables emailing articles from the UI. | _unset_ |
| `EMAIL_FROM` | Sender displayed in outgoing article emails. | `Compliance Updates <noreply@compliance.franklinbutler.com>` |
| `PRECOMPUTE_DAYS_BACK` | How far back to look when precomputing AI summaries. | `365` |
| `PRECOMPUTE_LIMIT_WORDS` | Target word limit for generated summaries. | `100` |
| `PRECOMPUTE_ONLY_EMPTY` | When `1`, skip rows that already contain an AI summary. | `1` |

Create a `.env` file in the project root to make these available to both the scraper and FastAPI app (the API loads `.env` automatically via `python-dotenv`).

## Utilities & scripts

- `tools/precompute_summaries.py` – iterate over stored items, extract text (including PDFs), and cache AI summaries so the UI can respond instantly.
- `tools/email_utils.py` – lightweight helper to email articles through SendGrid.
- `scripts/ensure_indexes.py` – make sure SQLite indexes exist in older databases (safe to run repeatedly).
- `scripts/normalise_tags.py` – tidy tag metadata for stored items.

Run any of these scripts with `python path/to/script.py` once your virtual environment is active.

## Development tips

- The SQLite schema is automatically migrated when the API or scraper touches the database. SQL migration files (`migrations_*.sql`) are also included for audit purposes if you prefer manual migrations.
- When developing locally, clear `ofgem.db` to start fresh or point `DB_PATH` to an alternative file.
- Consider scheduling `python main.py` via cron (or GitHub Actions) and `tools/precompute_summaries.py` overnight so your database is always fresh and AI summaries stay cached.

## License

This repository is distributed under the ISC license (see `package.json`).
