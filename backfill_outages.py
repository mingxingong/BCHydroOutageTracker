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
import threading
from datetime import datetime, timezone
from supabase import create_client

JSON_PATH = "bchydro-outages.json"
BATCH_SIZE = 300  # rows per Supabase upsert call

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
    `git cat-file --batch` process instead of one `git show` per commit.

    Streams stdout incrementally (header line, then exactly `size` bytes of
    content, then the batch format's trailing newline) instead of buffering
    the whole process output and re-joining the remaining lines on every
    iteration - the latter is O(n^2) and was OOM-killing the process on the
    full ~120k-commit history."""
    proc = subprocess.Popen(
        ["git", "cat-file", "--batch"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
    )

    def feed_stdin():
        for sha, _ in commits:
            proc.stdin.write(f"{sha}:{JSON_PATH}\n".encode())
        proc.stdin.close()

    writer = threading.Thread(target=feed_stdin, daemon=True)
    writer.start()

    stdout = proc.stdout
    for sha, ts in commits:
        header = stdout.readline()
        if not header:
            break
        parts = header.split()
        if len(parts) < 3 or parts[1] == b"missing":
            continue
        size = int(parts[2])
        content = stdout.read(size).decode(errors="replace")
        stdout.read(1)  # discard the batch format's trailing newline
        yield sha, ts, content

    writer.join()
    proc.stdout.close()
    proc.wait()


def backfill():
    commits = get_commit_list()
    print(f"Processing {len(commits)} historical snapshots...")

    seen = {}          # id -> latest active record dict
    to_write = {}      # id -> latest finished outage_event row, batched
    written = 0

    def flush():
        nonlocal to_write, written
        if not to_write:
            return
        rows = list(to_write.values())
        for i in range(0, len(rows), BATCH_SIZE):
            supabase.table("outage_events").upsert(rows[i:i + BATCH_SIZE]).execute()
        written += len(rows)
        to_write = {}

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
                # A dict keyed by id means a resolved event that reappears
                # in a later snapshot (common - resolved events often linger
                # a few more polls before dropping out) just overwrites its
                # own pending row instead of queuing a second one, so a
                # single upsert batch can never contain the same id twice.
                to_write[oid] = build_row(o, resolved=True)
                seen.pop(oid, None)
            else:
                seen[oid] = o

        # Anything previously active but missing now = resolved between polls
        vanished = set(seen.keys()) - current_ids
        for oid in vanished:
            last = seen.pop(oid)
            to_write[oid] = build_row(last, resolved=True, fallback_date_on=ts)

        if len(to_write) >= BATCH_SIZE:
            flush()

        if n % 5000 == 0:
            print(f"  {n}/{len(commits)} snapshots processed, {written + len(to_write)} events so far")

    # Remaining still-active outages as of the last commit
    for oid, o in seen.items():
        to_write[oid] = build_row(o, resolved=False)
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
