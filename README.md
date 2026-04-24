# Audience Labs to Supabase Sync

This project includes:

- `intel.html`: browser UI for viewing and optionally syncing data
- `audience_labs_supabase_router.py`: Python sync script for Audience Labs to Supabase
- `app-config.example.js`: safe example browser config
- `.env.example`: safe example Python env file

## GitHub-safe setup

1. Copy `app-config.example.js` to `app-config.local.js` and fill in your real browser-safe values.
2. Copy `.env.example` to `.env` and fill in your real server-side values.
3. Do not commit `app-config.local.js` or `.env`. They are ignored by `.gitignore`.

## GitHub Actions

This repo includes a manual GitHub Actions workflow at `.github/workflows/audience-labs-sync.yml`.

Set these GitHub repository secrets before running it:

- `AUDIENCE_LABS_API_KEY`
- `SUPABASE_URL`
- `SUPABASE_KEY`

Optional repository variables:

- `TYPE_SUFFIX`
- `AUDIENCE_PAGE_SIZE`
- `AUDIENCE_REQUEST_TIMEOUT`
- `AUDIENCE_PAGE_DELAY`
- `AUDIENCE_RETRY_WAIT_SECONDS`
- `AUDIENCE_MAX_RETRY_WAIT_SECONDS`
- `AUDIENCE_MAX_RETRIES`
- `GEOCODE_ENABLED`
- `GEOCODE_SLEEP_SECONDS`
- `MIN_SKIPTRACE_MATCH_SCORE`

The workflow has no schedule. Run it manually from the GitHub Actions tab.

## Important security note

If you put API keys in `intel.html` or `app-config.local.js`, anyone who opens the page can see them.
For production-safe syncing, prefer running `audience_labs_supabase_router.py` with environment variables and use `intel.html` mainly for viewing/reporting.
