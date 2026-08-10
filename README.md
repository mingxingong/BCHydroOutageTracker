# BC Hydro Outage Tracker — Setup

## 1. Create the Supabase project
1. Go to supabase.com, create a free project.
2. In the SQL Editor, run `schema.sql` (enables PostGIS, creates tables, view, and the matching function).
3. Load your C&I target property list into the `properties` table (name, company, address, latitude, longitude). This can start as your existing Vancouver new-construction / prospect list, geocoded.

## 2. Set up the poller
1. `poll_outages.py` lives at the repo root; `poll.yml` lives at `.github/workflows/poll.yml` — both are already in place in this repo.
2. Add repo secrets: `SUPABASE_URL` and `SUPABASE_SERVICE_KEY` (Settings > API in your Supabase project — use the **secret** key (`sb_secret_...`), or the legacy **service_role** key if your project still uses those; not `publishable`/`anon`, since the poller writes data). GitHub: Settings > Secrets and variables > Actions > New repository secret.
3. Push. GitHub Actions will start polling BC Hydro's feed every 15 minutes and syncing outages + matches into Supabase automatically, free of charge.

## 3. One-time backfill (optional but recommended)
`backfill_outages.py` reconstructs ~6 years of history from the [outages/bchydro-outages](https://github.com/outages/bchydro-outages) git-scraping repo, so you don't have to wait weeks for the live poller to accumulate data. Run once, after the Supabase schema is in place and the secrets above are set:
```
git clone https://github.com/outages/bchydro-outages.git
cd bchydro-outages
pip install "supabase>=2.10" --break-system-packages
SUPABASE_URL=... SUPABASE_SERVICE_KEY=... python /path/to/backfill_outages.py
```

## 4. What you get
- `outage_events` — full history of every outage (location, cause, duration, customer count), building up automatically over time.
- `outage_property_matches` — which of your target properties sat inside which outage's affected area.
- `property_outage_stats` view — per property: outage count, % of outages under 4 hours, most recent outage. Query this directly for your "many short outages = good battery backup fit" scoring.

## 5. Next step: outreach trigger
Once matches have ~1-2 weeks of history, a small script (or a scheduled query) can find properties where `outreach_flagged_at` is still null and the match is 7-14 days old, then push a ClickUp task for cold outreach — same pattern as your existing Daily Hive workflow, just triggered by outage data instead of news scraping. Happy to build that piece next once the base pipeline is running and you've loaded a property list.

## Notes
- Outage areas are polygons, not addresses, so matches are "property fell inside the affected zone," which is a reasonable proxy but not 100% precise at boundary edges.
- The backfill source (`outages/bchydro-outages`) has been polling since Oct 2020, so history before that date isn't recoverable.
