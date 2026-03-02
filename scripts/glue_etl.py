"""
IMDb Pipeline — Step 2: ETL + Data Quality
Reads IMDb TSV files from S3 raw zone
Applies transformations + data quality checks
Writes clean Parquet to S3 processed zone
Bad records go to S3 quarantine zone
Author: Vikas Pabba
"""

import sys
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import gzip
from datetime import datetime

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BUCKET         = "imdb-pipeline-vikas"
TODAY          = datetime.now().strftime("%Y/%m/%d")
S3_RAW         = f"raw/imdb/{TODAY}"
S3_PROCESSED   = "processed/movies"
S3_QUARANTINE  = "quarantine/movies"

BASICS_FILE    = "title.basics.tsv.gz"
RATINGS_FILE   = "title.ratings.tsv.gz"

# Columns to keep after join
KEEP_COLS = [
    "tconst", "titletype", "primarytitle",
    "startyear", "runtimeminutes", "genres",
    "averagerating", "numvotes"
]


def read_tsv_from_s3(s3_client, bucket, key):
    """Read a gzipped TSV file from S3 into a Pandas DataFrame"""
    logger.info(f"Reading s3://{bucket}/{key}")

    response = s3_client.get_object(Bucket=bucket, Key=key)
    compressed = response["Body"].read()

    with gzip.open(io.BytesIO(compressed), "rt", encoding="utf-8") as f:
        df = pd.read_csv(f, sep="\t", low_memory=False)

    # Lowercase all column names
    df.columns = df.columns.str.lower()
    logger.info(f"Loaded {len(df):,} rows | Columns: {list(df.columns)}")
    return df


def clean_imdb_nulls(df):
    """Replace IMDb's \\N placeholder with real NaN"""
    return df.replace("\\N", pd.NA)


def apply_data_quality(df):
    """
    Apply DQ rules — split into passed and failed records
    Rules:
      1. averagerating must be between 1.0 and 10.0
      2. numvotes must be > 0
      3. primarytitle must not be null
      4. tconst must not be null
    """
    logger.info("Applying data quality checks...")

    # Convert to numeric for comparison
    df["averagerating"] = pd.to_numeric(df["averagerating"], errors="coerce")
    df["numvotes"]      = pd.to_numeric(df["numvotes"],      errors="coerce")

    # DQ rules
    rule_rating   = df["averagerating"].between(1.0, 10.0)
    rule_votes    = df["numvotes"] > 0
    rule_title    = df["primarytitle"].notna()
    rule_tconst   = df["tconst"].notna()

    # All rules must pass
    all_passed = rule_rating & rule_votes & rule_title & rule_tconst

    passed_df = df[all_passed].copy()
    failed_df = df[~all_passed].copy()

    total     = len(df)
    passed    = len(passed_df)
    failed    = len(failed_df)
    dq_rate   = (passed / total * 100) if total > 0 else 0

    logger.info(f"✅ Passed DQ: {passed:,} records ({dq_rate:.1f}%)")
    logger.info(f"❌ Failed DQ: {failed:,} records quarantined")

    return passed_df, failed_df


def write_parquet_to_s3(s3_client, df, bucket, prefix):
    """Write DataFrame as Parquet to S3"""
    logger.info(f"Writing {len(df):,} records to s3://{bucket}/{prefix}/")

    table  = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    key = f"{prefix}/part-00000.snappy.parquet"
    s3_client.put_object(
        Bucket = bucket,
        Key    = key,
        Body   = buffer.getvalue()
    )
    logger.info(f"✅ Written to s3://{bucket}/{key}")


def write_json_to_s3(s3_client, df, bucket, prefix):
    """Write failed records as JSON to S3 quarantine"""
    if df.empty:
        logger.info("No failed records to quarantine")
        return

    logger.info(f"Writing {len(df):,} bad records to quarantine...")

    json_str = df.to_json(orient="records", lines=True)
    key      = f"{prefix}/failed_records.json"

    s3_client.put_object(
        Bucket = bucket,
        Key    = key,
        Body   = json_str.encode("utf-8")
    )
    logger.info(f"✅ Quarantine written to s3://{bucket}/{key}")


def send_sns_notification(passed, failed, dq_rate):
    """Send pipeline completion notification via SNS"""
    try:
        sns    = boto3.client("sns", region_name="us-east-1")
        topics = sns.list_topics()["Topics"]

        # Find our topic
        topic_arn = next(
            (t["TopicArn"] for t in topics if "imdb-pipeline-alerts" in t["TopicArn"]),
            None
        )

        if not topic_arn:
            logger.warning("SNS topic not found — skipping notification")
            return

        message = f"""
✅ IMDb ETL Pipeline — Completed Successfully

📊 Results:
   • Records passed DQ:     {passed:,}
   • Records quarantined:   {failed:,}
   • DQ pass rate:          {dq_rate:.1f}%
   • S3 processed path:     s3://{BUCKET}/{S3_PROCESSED}/
   • S3 quarantine path:    s3://{BUCKET}/{S3_QUARANTINE}/
   • Run date:              {TODAY}

{"⚠️  WARNING: DQ pass rate below 95%!" if dq_rate < 95 else "✅ DQ pass rate is healthy"}
        """

        sns.publish(
            TopicArn = topic_arn,
            Subject  = f"✅ IMDb Pipeline Succeeded — {TODAY}",
            Message  = message.strip()
        )
        logger.info("✅ SNS notification sent!")

    except Exception as e:
        logger.warning(f"SNS notification failed: {str(e)}")


def main():
    logger.info("═══ IMDb ETL Pipeline Started ═══")
    s3 = boto3.client("s3", region_name="us-east-1")

    try:
        # ── Step 1: Read raw files from S3 ──────────────────────────────────
        basics_df  = read_tsv_from_s3(s3, BUCKET, f"{S3_RAW}/{BASICS_FILE}")
        ratings_df = read_tsv_from_s3(s3, BUCKET, f"{S3_RAW}/{RATINGS_FILE}")

        # ── Step 2: Clean IMDb \N nulls ──────────────────────────────────────
        basics_df  = clean_imdb_nulls(basics_df)
        ratings_df = clean_imdb_nulls(ratings_df)

        # ── Step 3: Filter only movies (not TV shows etc) ────────────────────
        basics_df = basics_df[basics_df["titletype"] == "movie"]
        logger.info(f"Movies only: {len(basics_df):,} records")

        # ── Step 4: Join basics + ratings on tconst ──────────────────────────
        logger.info("Joining title.basics + title.ratings on tconst...")
        joined_df = basics_df.merge(ratings_df, on="tconst", how="inner")
        logger.info(f"After join: {len(joined_df):,} records")

        # ── Step 5: Select only needed columns ───────────────────────────────
        joined_df = joined_df[KEEP_COLS]

        # ── Step 6: Apply Data Quality checks ────────────────────────────────
        passed_df, failed_df = apply_data_quality(joined_df)

        dq_rate = len(passed_df) / len(joined_df) * 100

        # ── Step 7: Write clean records to S3 processed ──────────────────────
        write_parquet_to_s3(s3, passed_df, BUCKET, S3_PROCESSED)

        # ── Step 8: Write bad records to S3 quarantine ───────────────────────
        write_json_to_s3(s3, failed_df, BUCKET, S3_QUARANTINE)

        # ── Step 9: Send SNS notification ────────────────────────────────────
        send_sns_notification(len(passed_df), len(failed_df), dq_rate)

        logger.info("═══ IMDb ETL Pipeline Completed ✅ ═══")
        logger.info(f"📂 Clean data: s3://{BUCKET}/{S3_PROCESSED}/")
        logger.info(f"📂 Bad data:   s3://{BUCKET}/{S3_QUARANTINE}/")

    except Exception as e:
        logger.error(f"❌ Pipeline FAILED: {str(e)}")

        # Send failure notification
        try:
            sns = boto3.client("sns", region_name="us-east-1")
            topics = sns.list_topics()["Topics"]
            topic_arn = next(
                (t["TopicArn"] for t in topics if "imdb-pipeline-alerts" in t["TopicArn"]),
                None
            )
            if topic_arn:
                sns.publish(
                    TopicArn = topic_arn,
                    Subject  = f"❌ IMDb Pipeline FAILED — {TODAY}",
                    Message  = f"Pipeline failed with error:\n{str(e)}"
                )
        except:
            pass
        raise


if __name__ == "__main__":
    main()
