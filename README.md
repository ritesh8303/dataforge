# dataforge

A serverless Data Lakehouse built on AWS for processing German job market data.

## Architecture
- **Bronze Layer**: Raw JSON data ingested from Arbeitnow and BA APIs via AWS Lambda.
- **Silver Layer**: Cleaned and deduplicated Parquet files stored in S3, managed with SCD Type 2 logic to track historical changes.
- **Gold Layer**: Virtual consumption layer powered by DuckDB for fast SQL-based analytics and visualization.

## Tech Stack
- **Infrastructure**: Terraform (IaC)
- **Compute**: AWS Lambda (Python 3.11)
- **Storage**: AWS S3
- **Data Processing**: Pandas, AWS SDK for Pandas (awswrangler), Pydantic
- **Analytics**: DuckDB
- **Monitoring**: AWS SNS, CloudWatch, SQS (DLQ)

## Cost Efficiency
This project is designed to run entirely within the **AWS Free Tier**:
- Uses **SSM Parameter Store** instead of Secrets Manager.
- Uses **Lambda + DuckDB** instead of expensive AWS Glue or Athena.
- Uses **S3 Standard** within 5GB limits.

---
