# 🎬 IMDb Media Data ETL Pipeline

Batch processing pipeline analyzing IMDb movie quality data using AWS services.

## Architecture
```
IMDb TSV Files → S3 Raw → Python ETL → S3 Processed (Parquet)
→ AWS Glue Catalog → Amazon Athena → Grafana Dashboard + SNS Alerts
```

## AWS Services Used
- **Amazon S3** — 3-zone data lake (raw/processed/quarantine)
- **Python + boto3** — ETL + Data Quality checks
- **AWS Glue** — Data Catalog + Crawler
- **Amazon Athena** — Serverless SQL queries
- **Amazon SNS** — Pipeline notifications
- **Grafana Cloud** — Analytics dashboard

## Dataset
- Source: IMDb Non-Commercial Datasets
- Raw records: 12,321,221 titles
- Ratings: 1,640,974 records
- Movies after join + DQ: 339,781 records

## Pipeline Results
- ✅ Passed DQ: 339,781 records (100%)
- ❌ Quarantined: 2 bad records
- ⏱ Processing time: ~2 minutes

## Project Structure
scripts/
  upload_to_s3.py    → Upload IMDb files to S3
  glue_etl.py        → ETL + Data Quality checks
  setup_athena.py    → Register Athena tables

## How to Run
1. Install dependencies:
   pip install boto3 pandas pyarrow awscli

2. Configure AWS:
   aws configure

3. Run pipeline:
   python scripts/upload_to_s3.py
   python scripts/glue_etl.py
   python scripts/setup_athena.py

## Author
Vikas Pabba — Data Engineer
Texas A&M University-Kingsville | Websol.ai
```

4. Click **"Commit changes"** → **"Commit directly to master"** ✅

---

## 📍 What You've Accomplished So Far
```
✅ IMDb data → S3 (12M+ records)
✅ Python ETL → 339,781 clean movies
✅ Parquet → S3 processed zone
✅ Athena → SQL queries working
✅ SNS → Email notifications
✅ GitHub → Code pushed to master
