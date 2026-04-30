import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime
import os
from pydantic import BaseModel, Field, ValidationError

# Initialize S3 client for deletion
s3 = boto3.client('s3')
sns = boto3.client('sns')

def notify_error(message):
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if topic_arn:
        sns.publish(
            TopicArn=topic_arn,
            Subject="DataForge Pipeline Alert",
            Message=message
        )

# Define Pydantic model for job posting
class JobPosting(BaseModel):
    job_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    company: str = Field(..., min_length=1)
    location: str = Field(..., min_length=1)
    source: str = Field(..., description="Source of the job posting, e.g., 'Arbeitnow', 'BA_Federal'")
    # Add other fields as they appear in your API response, making them Optional if not always present

def lambda_handler(event, context):
    silver_bucket = os.environ['SILVER_BUCKET']
    s3_path_silver = f"s3://{silver_bucket}/cleaned/jobs_history.parquet"
    sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
    
    # 1. Get the new file info from Bronze
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print(f"Processing new file: s3://{bucket}/{key}")
    
    # Safety: Ensure we only process JSON files
    if not key.endswith('.json'):
        print(f"Skipping non-json file: {key}")
        return {"status": "Skipped"}
    
    
    # Create processing metadata
    processing_date = datetime.now()

    # Load new data
    df_new = wr.s3.read_json(path=f"s3://{bucket}/{key}", lines=True)
    
    # --- DEDUPLICATION (Inside Batch) ---
    # If the API sends the same job twice in one file, take the last one
    df_new = df_new.drop_duplicates(subset=['job_id'], keep='last')
    
    # --- DATA QUALITY GATEKEEPER (with Pydantic) ---
    validated_records = []
    validation_errors = []
    for index, row in df_new.iterrows():
        try:
            # Convert row to dict, then validate with Pydantic
            validated_job = JobPosting(**row.to_dict())
            validated_records.append(validated_job.model_dump()) # Use model_dump() for Pydantic v2
        except ValidationError as e:
            validation_errors.append(f"Record {index} failed validation: {e.errors()}")
            print(f"Validation error for record {index}: {row.to_dict()} - Errors: {e.errors()}")

    if validation_errors:
        err_msg = f"Aborting: Schema validation errors found for {len(validation_errors)} records. First error: {validation_errors[0]}"
        notify_error(err_msg)
        return {"status": "Failed", "reason": "Schema Validation Error", "details": validation_errors}

    df_new = pd.DataFrame(validated_records) # Use only validated records
    df_new['valid_from'] = processing_date.date()
    df_new['valid_to'] = None
    df_new['is_current'] = True
    df_new['processing_year'] = processing_date.year
    df_new['processing_month'] = processing_date.month

    try:
        # 2. Load existing Silver history
        df_history = wr.s3.read_parquet(path=s3_path_silver)
        
        # 3. SCD Type 2 Logic: Mark old records as expired
        new_ids = df_new['job_id'].tolist()
        mask = (df_history['job_id'].isin(new_ids)) & (df_history['is_current'] == True)
        df_history.loc[mask, 'valid_to'] = processing_date.date()
        df_history.loc[mask, 'is_current'] = False

        # 4. Merge
        # Using sort=False maintains performance when schemas evolve
        df_final = pd.concat([df_history, df_new], ignore_index=True, sort=False).reset_index(drop=True)
        print("Success: History updated via Pandas.")
        
    except Exception as e:
        # This usually happens on the very first run of the pipeline
        print(f"Initial load or history read failed: {e}")
        df_final = df_new

    # 5. Write back to Silver as Parquet
    wr.s3.to_parquet(
        df=df_final,
        path=s3_path_silver,
        dataset=True,
        mode="overwrite",  # Overwrites the partition, not the whole bucket
        partition_cols=["processing_year", "processing_month"]
    )

    # 6. DELETE the file from Bronze (Cleanup)
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"Success: Deleted {key} from Bronze.")
    
    return {"status": "Silver Layer Updated (SCD2)"}