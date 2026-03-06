# Source Truth — Kaiya Text2SQL Evaluation

This folder contains the **Source Truth** automation: a CLI that populates and evaluates natural-language queries against the Kaiya (Tellius) API using a Google Sheet as the source of truth.

## What Was Created

Converted from the Jupyter notebook (`Text_2_SQL_Evaluation_Ishdev_(Demo2).ipynb`) into a structured Python project with:

- **Configuration via `.env`** — No hardcoded secrets; all settings (Kaiya URL, auth, spreadsheet ID, OpenAI key, BV mappings, workers) come from environment variables.
- **Two execution modes** with optional follow-up-only phase:
  - **Data entry** — Fetches golden SQL from Kaiya for each query and writes it (and Trace ID, etc.) into the sheet.
  - **Query evaluation** — Compares Kaiya’s current response to the golden SQL using the sheet’s “Correct SQL” and LLM-assisted comparison; updates Status, Failure Category, and related columns.
- **Two-phase execution** — Parent queries run first; follow-up queries run in the same conversation as their parent (using parent’s `conversation_id` / Trace ID), avoiding races when running in parallel.

## Repository Layout

```
Source Truth/
├── .env.example          # Template — copy to .env and fill in your values
├── .gitignore             # Excludes .env, credentials.json, __pycache__
├── config.py              # Loads settings from .env
├── main.py                # CLI entry point (data_entry | query_evaluation, --followup, --retry)
├── requirements.txt      # Python dependencies
├── kaiya_client.py       # Kaiya API: call_api, get_kaiya_response, get_sql_query_response
├── evaluator.py          # LLM-assisted data comparison (fuzzy column mapping)
├── llm_summary.py        # LLM-based SQL comparison summary
├── sheet_utils.py        # Google Sheets: auth, open_sheet, update rows, parent-index, hyperlinks
├── data_entry.py         # Data-population flow (two-phase)
└── query_evaluation.py   # Query-evaluation flow (two-phase)
```

## Setup

1. **Copy the env template and add your values**
   ```bash
   cd "Source Truth"
   cp .env.example .env
   # Edit .env: BASE_URL, AUTH_TOKEN, USER_ID, SPREADSHEET_ID, SHEET_NAME,
   # OPENAI_API_KEY, GOOGLE_CREDENTIALS_PATH, and Business View mappings.
   ```

2. **Google Sheets access**
   - Use a **Google Cloud Service Account** JSON key (not OAuth “web” client).
   - Save the key file as `credentials.json` in `Source Truth/` (or set `GOOGLE_CREDENTIALS_PATH` in `.env`).
   - Share the target Google Sheet with the service account email (e.g. `...@project.iam.gserviceaccount.com`) with **Editor** access.

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

From the `Source Truth` directory:

```bash
# Data-population: get golden SQL from Kaiya (parents then follow-ups)
python main.py data_entry

# Data-population: follow-ups only (parents must already be done)
python main.py data_entry --followup

# Query evaluation: compare Kaiya vs golden SQL (parents then follow-ups)
python main.py query_evaluation

# Query evaluation: follow-ups only
python main.py query_evaluation --followup

# Optional: retry each query N times (e.g. for flaky APIs)
python main.py query_evaluation --retry 3
```

## Sheet Columns (Expected)

The sheet should have (among others):

- **Business View** — Name that maps to a BV ID via `.env` (e.g. `Gene_Sales=bv_...`).
- **Kaiya Query** — Natural-language query.
- **Follow-up Of** — Row number of parent (for follow-up rows); empty for parents.
- **Correct SQL** — Filled by `data_entry`; used by `query_evaluation` as golden SQL.
- **Trace ID**, **Status**, **Failure Category**, **Failure Description**, **Tellius Link**, etc. — Updated by the scripts.

## Reference

- Original logic: Jupyter notebook in the `Script` folder of the kaiya-api-testing-automation repo.
- Target API: Tellius/Kaiya (e.g. `/api/kaiya/conversation/process`, `/api/tql/query/execute`).
