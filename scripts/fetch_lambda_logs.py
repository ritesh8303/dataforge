"""Fetch recent CloudWatch logs for transformer and gold-generator."""
import boto3
from datetime import datetime, timezone, timedelta

logs = boto3.client("logs", region_name="eu-central-1")

def fetch_logs(log_group, hours_back=96, limit=80):
    start = int((datetime.now(timezone.utc) - timedelta(hours=hours_back)).timestamp() * 1000)
    end = int(datetime.now(timezone.utc).timestamp() * 1000)
    print(f"\n{'='*60}\n{log_group}\n{'='*60}")
    try:
        resp = logs.filter_log_events(
            logGroupName=log_group,
            startTime=start,
            endTime=end,
            limit=limit,
        )
        events = resp.get("events", [])
        # Show last N events chronologically
        for e in events[-40:]:
            ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc)
            msg = e["message"].rstrip()
            print(f"{ts.strftime('%Y-%m-%d %H:%M:%S')} | {msg}")
        if not events:
            print("(no events)")
    except Exception as ex:
        print(f"Error: {ex}")

fetch_logs("/aws/lambda/dataforge-transformer", hours_back=120)
fetch_logs("/aws/lambda/dataforge-gold-generator", hours_back=120)
