"""Check today's scheduled run vs manual backfill."""
from datetime import datetime, timezone

import boto3

logs = boto3.client("logs", region_name="eu-central-1")

def events(log_group, start, end, filter_pattern=""):
    kwargs = {
        "logGroupName": log_group,
        "startTime": int(start.timestamp() * 1000),
        "endTime": int(end.timestamp() * 1000),
        "limit": 100,
    }
    if filter_pattern:
        kwargs["filterPattern"] = filter_pattern
    return logs.filter_log_events(**kwargs).get("events", [])

now = datetime.now(timezone.utc)
today = now.replace(hour=0, minute=0, second=0, microsecond=0)

print("=== July 10 transformer invocations ===")
for e in events("/aws/lambda/dataforge-transformer", today, now):
    ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)
    msg = e["message"].strip().replace("\n", " ")[:160]
    print(f"{ts.isoformat()} | {msg}")

print("\n=== July 10 ingestors around 20:00 UTC ===")
for fn in ["dataforge-ingestor", "dataforge-ba-ingestor", "dataforge-company-ingestor", "dataforge-berlin-startups-ingestor"]:
    evs = events(f"/aws/lambda/{fn}", today.replace(hour=19, minute=55), now)
    print(f"\n{fn}:")
    for e in evs[-5:]:
        ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)
        msg = e["message"].strip().replace("\n", " ")[:120]
        print(f"  {ts.isoformat()} | {msg}")

print("\n=== July 9 20:30 transformer (last auto run before fix) ===")
start = datetime(2026, 7, 9, 20, 25, tzinfo=timezone.utc)
end = datetime(2026, 7, 9, 20, 40, tzinfo=timezone.utc)
for e in events("/aws/lambda/dataforge-transformer", start, end):
    ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)
    msg = e["message"].strip().replace("\n", " ")[:160]
    if "ERROR" in msg or "completed" in msg or "Aborting" in msg or "START" in msg:
        print(f"{ts.isoformat()} | {msg}")
