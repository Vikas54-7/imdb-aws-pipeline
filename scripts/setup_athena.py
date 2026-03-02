"""
IMDb Pipeline — Step 3: Setup Athena Tables
Creates Athena database and tables pointing to S3 processed Parquet
Author: Vikas Pabba
"""

import boto3
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
BUCKET            = "imdb-pipeline-vikas"
ATHENA_DB         = "imdb_pipeline"
ATHENA_RESULTS    = f"s3://{BUCKET}/athena-results/"
S3_PROCESSED_PATH = f"s3://{BUCKET}/processed/movies/"


def run_query(athena_client, query, database=None):
    """Run an Athena SQL query and wait for completion"""
    params = {
        "QueryString"            : query,
        "ResultConfiguration"    : {"OutputLocation": ATHENA_RESULTS},
    }
    if database:
        params["QueryExecutionContext"] = {"Database": database}

    response     = athena_client.start_query_execution(**params)
    execution_id = response["QueryExecutionId"]

    logger.info(f"Running query: {execution_id}")

    # Wait for completion
    while True:
        result = athena_client.get_query_execution(QueryExecutionId=execution_id)
        status = result["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            logger.info(f"✅ Query succeeded: {execution_id}")
            return execution_id
        elif status in ["FAILED", "CANCELLED"]:
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise Exception(f"Query failed: {reason}")

        time.sleep(2)


def setup_athena():
    athena = boto3.client("athena", region_name="us-east-1")

    # Step 1: Create database
    logger.info("Creating Athena database...")
    run_query(athena, f"CREATE DATABASE IF NOT EXISTS {ATHENA_DB}")

    # Step 2: Create movies table
    logger.info("Creating movies table...")
    run_query(athena, f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.movies (
            tconst          string,
            titletype       string,
            primarytitle    string,
            startyear       string,
            runtimeminutes  string,
            genres          string,
            averagerating   double,
            numvotes        bigint
        )
        STORED AS PARQUET
        LOCATION '{S3_PROCESSED_PATH}'
        TBLPROPERTIES ('parquet.compress'='SNAPPY')
    """, database=ATHENA_DB)

    logger.info("✅ Athena setup complete!")
    logger.info(f"   Database: {ATHENA_DB}")
    logger.info(f"   Table:    movies")
    logger.info(f"   Data:     {S3_PROCESSED_PATH}")
    logger.info("\n🔍 Test it in Athena console with:")
    logger.info(f"   SELECT * FROM {ATHENA_DB}.movies LIMIT 10;")


if __name__ == "__main__":
    setup_athena()
