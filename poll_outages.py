"""
Polls BC Hydro's public outage feed and syncs it into Supabase.

Run on a schedule (every 15-30 min) via GitHub Actions cron, matching
BC Hydro's own update cadence.

Env vars required:
    SUPABASE_URL
    SUPABASE_SERVICE_KEY   (the secret key - sb_secret_..., or legacy service_role -
                            not the publishable/anon key, since the poller writes data)

pip install requests "supabase>=2.10" --break-system-packages
"""

import os
import requests
from datetime import datetime, timezone
from supabase import create_client

BCHYDRO_FEED = "https://www.bchydro.com/power-outages/app/outages-map-data.json"

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"],
)


def fetch_current_outages():
    resp = requests.get(BCHYDRO_FEED, timeout=30)
    resp.raise_for_status()
    return resp.json()


def to_iso(epoch_ms):
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def polygon_to_wkt(coords):
    # BC Hydro sends a flat [lon, lat, lon, lat, ...] list
    if not coords or len(coords) < 6:
        return None
    pairs = [f"{coords[i]} {coords[i+1]}" for i in range(0, len(coords), 2)]
    if pairs[0] != pairs[-1]:
        pairs.append(pairs[0])  # polygons must close
    return f"SRID=4326;POLYGON(({', '.join(pairs)}))"


def sync_active_outages(current):
    now = datetime.now(timezone.utc).isoformat()
    current_ids = set()

    for o in current:
        current_ids.add(o["id"])
        row = {
            "id": o["id"],
            "gis_id": o.get("gisId"),
            "municipality": o.get("municipality"),
            "area": o.get("area"),
            "cause": o.get("cause"),
            "region_name": o.get("regionName"),
            "customers_out": o.get("numCustomersOut"),
            "date_off": to_iso(o.get("dateOff")),
            "date_on": to_iso(o.get("dateOn")),
            "status": "resolved" if o.get("dateOn") else "active",
            "latitude": o.get("latitude"),
            "longitude": o.get("longitude"),
            "polygon": polygon_to_wkt(o.get("polygon")),
            "last_seen_at": now,
        }
        if row["date_on"]:
            off = datetime.fromisoformat(row["date_off"])
            on = datetime.fromisoformat(row["date_on"])
            row["duration_min"] = int((on - off).total_seconds() / 60)

        supabase.table("outage_events").upsert(row).execute()

    return current_ids


def close_vanished_outages(current_ids):
    """Outages that disappeared from the feed without an explicit dateOn
    are treated as resolved as of last_seen_at (best available estimate)."""
    active = (
        supabase.table("outage_events")
        .select("id, date_off, last_seen_at")
        .eq("status", "active")
        .execute()
    )
    for row in active.data:
        if row["id"] not in current_ids:
            off = datetime.fromisoformat(row["date_off"])
            on = datetime.fromisoformat(row["last_seen_at"])
            duration = int((on - off).total_seconds() / 60)
            supabase.table("outage_events").update({
                "status": "resolved",
                "date_on": row["last_seen_at"],
                "duration_min": duration,
            }).eq("id", row["id"]).execute()


def match_properties_to_outages():
    """Point-in-polygon match: which properties sit inside which outage areas."""
    result = supabase.rpc("match_properties_in_outages").execute()
    return result.data


if __name__ == "__main__":
    current = fetch_current_outages()
    ids = sync_active_outages(current)
    close_vanished_outages(ids)
    matches = match_properties_to_outages()
    print(f"Synced {len(current)} active outages, {len(matches or [])} new property matches")
