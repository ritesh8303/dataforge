import os
import json
import pandas as pd
import awswrangler as wr


def lambda_handler(event, context):
    silver_path = os.environ.get('SILVER_PATH')
    gold_bucket = os.environ.get('GOLD_BUCKET')

    try:
        if not silver_path or not gold_bucket:
            raise ValueError("SILVER_PATH and GOLD_BUCKET environment variables must be set.")

        print("Reading Silver data...")
        df = wr.s3.read_parquet(path=silver_path, dataset=True)
        current = df[df['is_current'] == True].copy().reset_index(drop=True)
        print(f"Total active jobs: {len(current)}")

        # 1. All jobs
        cols = [c for c in ['job_id', 'title', 'company', 'location', 'source', 'scd_start_date', 'remote'] if c in current.columns]
        all_jobs = current[cols].copy()
        all_jobs['date_added'] = pd.to_datetime(all_jobs['scd_start_date']).dt.date.astype(str)
        all_jobs.drop(columns=['scd_start_date'], inplace=True)

        # 2. Jobs by source
        jobs_by_source = current.groupby('source').size().reset_index(name='job_count').sort_values('job_count', ascending=False)

        # 3. Top locations — take first part before comma to clean "Berlin, Berlin, Germany" → "Berlin"
        current['location_clean'] = current['location'].str.split(',').str[0].str.strip()
        top_locations = (
            current[current['location_clean'].notna() & (current['location_clean'] != '')]
            .groupby('location_clean').size()
            .reset_index(name='job_count')
            .sort_values('job_count', ascending=False)
            .head(20)
            .rename(columns={'location_clean': 'location'})
        )

        # 4. Remote vs onsite (Arbeitnow only — BA API has no remote field)
        if 'remote' in current.columns:
            remote_df = current[current['source'] == 'arbeitnow'].copy()
            remote_df['work_type'] = remote_df['remote'].apply(
                lambda x: 'Remote' if x is True or str(x) == 'True' else 'On-site'
            )
            remote_vs_onsite = remote_df.groupby('work_type').size().reset_index(name='job_count')
        else:
            remote_vs_onsite = pd.DataFrame({'work_type': [], 'job_count': []})

        # 5. Jobs trend over time
        df['date'] = pd.to_datetime(df['scd_start_date']).dt.date.astype(str)
        jobs_trend = df.groupby('date').size().reset_index(name='new_jobs').sort_values('date')

        # 6. Top companies
        top_companies = (
            current[current['company'].notna() & (current['company'] != '')]
            .groupby('company').size()
            .reset_index(name='job_count')
            .sort_values('job_count', ascending=False)
            .head(20)
        )

        # 7. Active vs expired
        df['status'] = df['is_current'].apply(lambda x: 'Active' if x else 'Expired')
        active_vs_expired = df.groupby('status').size().reset_index(name='job_count')

        # Write all to S3 gold bucket
        gold_base = f"s3://{gold_bucket}"
        wr.s3.to_csv(all_jobs,          path=f"{gold_base}/all_jobs.csv",           index=False)
        wr.s3.to_csv(jobs_by_source,    path=f"{gold_base}/jobs_by_source.csv",     index=False)
        wr.s3.to_csv(top_locations,     path=f"{gold_base}/top_locations.csv",      index=False)
        wr.s3.to_csv(remote_vs_onsite,  path=f"{gold_base}/remote_vs_onsite.csv",   index=False)
        wr.s3.to_csv(jobs_trend,        path=f"{gold_base}/jobs_trend.csv",         index=False)
        wr.s3.to_csv(top_companies,     path=f"{gold_base}/top_companies.csv",      index=False)
        wr.s3.to_csv(active_vs_expired, path=f"{gold_base}/active_vs_expired.csv",  index=False)

        msg = f"Gold layer refreshed. Active jobs: {len(current)}, Files written: 7"
        print(msg)
        return {"statusCode": 200, "body": json.dumps({"message": msg})}

    except Exception as e:
        error_msg = f"Gold generation failed: {str(e)}"
        print(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}
