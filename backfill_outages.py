"""
One-time backfill: reconstructs historical BC Hydro outage events from the
public git-scraping repo (github.com/outages/bchydro-outages), which has
been polling BC Hydro's outage feed since Oct 2020 and committing each
change. This gives us ~6 years of history in one pass instead of waiting
weeks for the live poller to accumulate it.

Usage:
    git clone https://github.com/outages/bchydro-outages.git
    cd bchydro-outages
    python backfill_outages.py

Env vars required (same as poll_outages.py):
    SUPABASE_URL
    SUPABASE_SERVICE_KEY   (the secret key - sb_secret_..., or legacy service_role)

pip install "supabase>=2.10" --break-system-packages
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from supabase import create_client

JSON_PATH = "bchydro-outages.json"
BATCH_SIZE = 500  # rows per Supabase upsert call

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"],
)


def to_iso(epoch_ms):
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def polygon_to_wkt(coords):
    if not coords or len(coords) < 6:
        return None
    pairs = [f"{coords[i]} {coords[i+1]}" for i in range(0, len(coords), 2)]
    if pairs[0] != pairs[-1]:
        pairs.append(pairs[0])
    return f"SRID=4326;POLYGON(({', '.join(pairs)}))"


def get_commit_list():
    """All commits that touched the outage JSON, oldest first, with timestamps."""
    out = subprocess.run(
        ["git", "log", "--format=%H %ct", "--diff-filter=AM", "--reverse", "--", JSON_PATH],
        capture_output=True, text=True, check=True,
    ).stdout
    commits = []
    for line in out.strip().splitlines():
        sha, ts = line.split()
        commits.append((sha, int(ts)))
    return commits


def stream_snapshots(commits):
    """Reads every historical version of the JSON file via a single
    `git cat-file --batch` process instead of one `git show` per commit."""
    refs = "\n".join(f"{sha}:{JSON_PATH}" for sha, _ in commits)
    proc = subprocess.Popen(
        ["git", "cat-file", "--batch"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    stdout, _ = proc.communicate(refs)
    lines = stdout.split("\n")
    i = 0
    idx = 0
    while i < len(lines) and idx < len(commits):
        header = lines[i]
        parts = header.split()
        if len(parts) < 3 or parts[1] == "missing":
            i += 1
            idx += 1
            continue
        size = int(parts[2])
        content = "\n".join(lines[i + 1:]).encode()[:size].decode(errors="replace")
        yield commits[idx][0], commits[idx][1], content
        consumed_lines = content.count("\n") + 1
        i += 1 + consumed_lines
        idx += 1


def backfill():
    commits = get_commit_list()
    print(f"Processing {len(commits)} historical snapshots...")

    seen = {}          # id -> latest active record dict
    to_write = []       # finished outage_event rows, batched
    written = 0

    def flush():
        nonlocal to_write, written
        if not to_write:
            return
        for i in range(0, len(to_write), BATCH_SIZE):
            supabase.table("outage_events").upsert(to_write[i:i + BATCH_SIZE]).execute()
        written += len(to_write)
        to_write = []

    for n, (sha, ts, content) in enumerate(stream_snapshots(commits)):
        try:
            records = json.loads(content)
        except json.JSONDecodeError:
            continue

        current_ids = set()
        for o in records:
            oid = o.get("id")
            if oid is None:
                continue
            current_ids.add(oid)

            if o.get("dateOn"):
                # Already resolved as of this snapshot - emit once
                if oid in seen or oid not in [r["id"] for r in to_write[-50:]]:
                    row = build_row(o, resolved=True)
                    to_write.append(row)
                seen.pop(oid, None)
            else:
                seen[oid] = o

        # Anything previously active but missing now = resolved between polls
        vanished = set(seen.keys()) - current_ids
        for oid in vanished:
            last = seen.pop(oid)
            row = build_row(last, resolved=True, fallback_date_on=ts)
            to_write.append(row)

        if len(to_write) >= BATCH_SIZE:
            flush()

        if n % 5000 == 0:
            print(f"  {n}/{len(commits)} snapshots processed, {written + len(to_write)} events so far")

    # Remaining still-active outages as of the last commit
    for oid, o in seen.items():
        to_write.append(build_row(o, resolved=False))
    flush()

    print(f"Done. {written} historical outage events written.")


def build_row(o, resolved, fallback_date_on=None):
    date_off = to_iso(o.get("dateOff"))
    date_on = to_iso(o.get("dateOn")) or (
        datetime.fromtimestamp(fallback_date_on, tz=timezone.utc).isoformat()
        if resolved and fallback_date_on else None
    )
    row = {
        "id": o["id"],
        "gis_id": o.get("gisId"),
        "municipality": o.get("municipality"),
        "area": o.get("area"),
        "cause": o.get("cause"),
        "region_name": o.get("regionName"),
        "customers_out": o.get("numCustomersOut"),
        "date_off": date_off,
        "date_on": date_on,
        "status": "resolved" if (resolved and date_on) else "active",
        "latitude": o.get("latitude"),
        "longitude": o.get("longitude"),
        "polygon": polygon_to_wkt(o.get("polygon")),
    }
    if date_on and date_off:
        off = datetime.fromisoformat(date_off)
        on = datetime.fromisoformat(date_on)
        row["duration_min"] = max(0, int((on - off).total_seconds() / 60))
    return row


if __name__ == "__main__":
    backfill()
