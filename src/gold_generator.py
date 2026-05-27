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

        # 1. All active jobs
        cols = [c for c in [
            'job_id', 'title', 'company', 'location', 'zip_code', 'state',
            'source', 'scd_start_date', 'remote', 'url', 'job_types',
            'tags', 'description', 'start_date_raw', 'modified_at', 'ingested_at'
        ] if c in current.columns]
        all_jobs = current[cols].copy()
        all_jobs['date_added'] = pd.to_datetime(all_jobs['scd_start_date']).dt.date.astype(str)
        all_jobs.drop(columns=['scd_start_date'], inplace=True)
        all_jobs.rename(columns={'url': 'job_url', 'remote': 'is_remote'}, inplace=True)
        all_jobs['is_remote'] = all_jobs.get('is_remote', pd.Series(False, index=all_jobs.index)).apply(
            lambda x: True if str(x) == 'True' else False
        )

        # 1b. Expired jobs (is_current=False)
        expired_raw = df[df['is_current'] == False].copy()
        exp_cols = [c for c in [
            'job_id', 'title', 'company', 'location', 'zip_code', 'state',
            'source', 'scd_start_date', 'scd_end_date', 'remote', 'url', 'job_types',
            'tags', 'start_date_raw', 'modified_at', 'ingested_at'
        ] if c in expired_raw.columns]
        expired_jobs = expired_raw[exp_cols].copy()
        expired_jobs['date_added']   = pd.to_datetime(expired_jobs['scd_start_date']).dt.date.astype(str)
        expired_jobs['date_expired'] = pd.to_datetime(expired_jobs['scd_end_date']).dt.date.astype(str)
        expired_jobs.drop(columns=['scd_start_date', 'scd_end_date'], inplace=True)
        expired_jobs.rename(columns={'url': 'job_url', 'remote': 'is_remote'}, inplace=True)
        expired_jobs['is_remote'] = expired_jobs.get('is_remote', pd.Series(False, index=expired_jobs.index)).apply(
            lambda x: True if str(x) == 'True' else False
        )

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

        # 8. Top skills from tags (Arbeitnow) + title keywords (both sources)
        import re
        from collections import Counter

        SKILL_KEYWORDS = [
            # Data
            'Python', 'SQL', 'Spark', 'Kafka', 'Airflow', 'dbt', 'Pandas',
            'Hadoop', 'Hive', 'Flink', 'Databricks', 'Snowflake', 'BigQuery',
            # AI / ML
            'Machine Learning', 'Deep Learning', 'LLM', 'NLP', 'PyTorch',
            'TensorFlow', 'Scikit', 'MLflow', 'Hugging Face', 'OpenAI',
            'Generative AI', 'Computer Vision', 'RAG', 'LangChain',
            # Cloud
            'AWS', 'Azure', 'GCP', 'Kubernetes', 'Docker', 'Terraform',
            # BI / Analytics
            'Power BI', 'Tableau', 'Looker', 'Excel', 'Grafana',
            # Engineering
            'Java', 'Scala', 'Go', 'TypeScript', 'React', 'FastAPI',
        ]
        skill_pattern = re.compile(
            r'\b(' + '|'.join(re.escape(s) for s in SKILL_KEYWORDS) + r')\b',
            re.IGNORECASE
        )
        skill_counter = Counter()
        for _, row in current.iterrows():
            text = ' '.join(filter(None, [
                str(row.get('title', '')),
                str(row.get('tags', '')),
                str(row.get('description', ''))[:500],  # cap description length
            ]))
            for match in skill_pattern.finditer(text):
                skill_counter[match.group().title()] += 1

        top_skills = pd.DataFrame(
            skill_counter.most_common(20),
            columns=['skill', 'job_count']
        )
        gold_base = f"s3://{gold_bucket}"
        wr.s3.to_csv(all_jobs,          path=f"{gold_base}/all_jobs.csv",           index=False, quoting=1)  # QUOTE_ALL
        wr.s3.to_csv(expired_jobs,       path=f"{gold_base}/expired_jobs.csv",       index=False, quoting=1)  # QUOTE_ALL
        wr.s3.to_csv(jobs_by_source,    path=f"{gold_base}/jobs_by_source.csv",     index=False)
        wr.s3.to_csv(top_locations,     path=f"{gold_base}/top_locations.csv",      index=False)
        wr.s3.to_csv(remote_vs_onsite,  path=f"{gold_base}/remote_vs_onsite.csv",   index=False)
        wr.s3.to_csv(jobs_trend,        path=f"{gold_base}/jobs_trend.csv",         index=False)
        wr.s3.to_csv(top_companies,     path=f"{gold_base}/top_companies.csv",      index=False)
        wr.s3.to_csv(active_vs_expired, path=f"{gold_base}/active_vs_expired.csv",  index=False)
        wr.s3.to_csv(top_skills,         path=f"{gold_base}/top_skills.csv",          index=False)

        msg = f"Gold layer refreshed. Active jobs: {len(current)}, Files written: 9"
        print(msg)
        return {"statusCode": 200, "body": json.dumps({"message": msg})}

    except Exception as e:
        error_msg = f"Gold generation failed: {str(e)}"
        print(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}
