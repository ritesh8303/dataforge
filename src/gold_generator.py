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
            'source', 'ats', 'department', 'scd_start_date', 'remote', 'url',
            'job_types', 'tags', 'description', 'salary', 'published_at',
            'start_date_raw', 'modified_at', 'ingested_at'
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
            'source', 'ats', 'department', 'scd_start_date', 'scd_end_date',
            'remote', 'url', 'job_types', 'tags', 'salary', 'published_at',
            'start_date_raw', 'modified_at', 'ingested_at'
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

        # 4. Remote vs onsite (sources with an explicit remote signal)
        if 'remote' in current.columns:
            remote_df = current[current['source'].isin(['arbeitnow', 'direct'])].copy()
            remote_df['work_type'] = remote_df['remote'].apply(
                lambda x: 'Remote' if x is True or str(x) == 'True' else 'On-site'
            )
            remote_vs_onsite = remote_df.groupby('work_type').size().reset_index(name='job_count')
        else:
            remote_vs_onsite = pd.DataFrame({'work_type': [], 'job_count': []})

        # 5. Jobs trend — count only first appearance of each job_id (true new jobs)
        first_seen = df.sort_values('scd_start_date').drop_duplicates(subset='job_id', keep='first')
        first_seen['date'] = pd.to_datetime(first_seen['scd_start_date']).dt.date.astype(str)
        jobs_trend = first_seen.groupby('date').size().reset_index(name='new_jobs').sort_values('date')

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
        import html

        def strip_html(text):
            """Remove HTML tags and decode entities."""
            text = re.sub(r'<[^>]+>', ' ', str(text))
            return html.unescape(text)

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

        # Patterns for description-derived KPIs
        english_pattern  = re.compile(r'\b(the|and|for|with|you|our|your|we are|we\'re|join|team|role|experience|skills|requirements|responsibilities)\b', re.IGNORECASE)
        homeoffice_pattern = re.compile(r'\b(homeoffice|home.office|remote|hybrid|work from home|mobiles arbeiten)\b', re.IGNORECASE)
        benefits_pattern = re.compile(r'<h2[^>]*>\s*(benefits|benefits|vorteile|was wir bieten|was wir dir bieten|unser angebot)\s*</h2>', re.IGNORECASE)

        skill_counter    = Counter()
        english_count    = 0
        homeoffice_desc_count = 0
        benefits_count   = 0

        arbeitnow_jobs = current[current['source'] == 'arbeitnow']

        for _, row in current.iterrows():
            raw_desc = str(row.get('description', ''))
            plain_text = strip_html(raw_desc)
            combined = ' '.join(filter(None, [
                str(row.get('title', '')),
                str(row.get('tags', '')),
                plain_text[:500],
            ]))
            for match in skill_pattern.finditer(combined):
                skill_counter[match.group().title()] += 1

        for _, row in arbeitnow_jobs.iterrows():
            raw_desc = str(row.get('description', ''))
            plain_text = strip_html(raw_desc)
            if len(plain_text) > 100:
                en_matches = len(english_pattern.findall(plain_text[:1000]))
                total_words = len(plain_text[:1000].split())
                if total_words > 0 and (en_matches / total_words) > 0.04:
                    english_count += 1
            if homeoffice_pattern.search(raw_desc):
                homeoffice_desc_count += 1
            if benefits_pattern.search(raw_desc):
                benefits_count += 1

        top_skills = pd.DataFrame(
            skill_counter.most_common(20),
            columns=['skill', 'job_count']
        )

        description_insights = pd.DataFrame([{
            'english_jobs':        english_count,
            'homeoffice_mentioned': homeoffice_desc_count,
            'jobs_with_benefits':  benefits_count,
            'arbeitnow_total':     len(arbeitnow_jobs),
        }])
        gold_base = f"s3://{gold_bucket}"
        wr.s3.to_csv(all_jobs,          path=f"{gold_base}/all_jobs.csv",           index=False, quoting=1)  # QUOTE_ALL
        wr.s3.to_csv(expired_jobs,       path=f"{gold_base}/expired_jobs.csv",       index=False, quoting=1)  # QUOTE_ALL
        wr.s3.to_csv(jobs_by_source,    path=f"{gold_base}/jobs_by_source.csv",     index=False)
        wr.s3.to_csv(top_locations,     path=f"{gold_base}/top_locations.csv",      index=False)
        wr.s3.to_csv(remote_vs_onsite,  path=f"{gold_base}/remote_vs_onsite.csv",   index=False)
        wr.s3.to_csv(jobs_trend,        path=f"{gold_base}/jobs_trend.csv",         index=False)
        wr.s3.to_csv(top_companies,     path=f"{gold_base}/top_companies.csv",      index=False)
        wr.s3.to_csv(active_vs_expired, path=f"{gold_base}/active_vs_expired.csv",  index=False)
        wr.s3.to_csv(top_skills,          path=f"{gold_base}/top_skills.csv",           index=False)
        wr.s3.to_csv(description_insights, path=f"{gold_base}/description_insights.csv", index=False)

        msg = f"Gold layer refreshed. Active jobs: {len(current)}, Files written: 10"
        print(msg)
        return {"statusCode": 200, "body": json.dumps({"message": msg})}

    except Exception as e:
        error_msg = f"Gold generation failed: {str(e)}"
        print(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}
