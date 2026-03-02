"""
IMDb Pipeline — PySpark ETL Job for EMR
Author: Vikas Pabba
EMR Version: emr-7.2.0
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType
import boto3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

BUCKET         = "imdb-pipeline-vikas"
TODAY          = datetime.now().strftime("%Y/%m/%d")
S3_RAW         = f"s3://{BUCKET}/raw/imdb/{TODAY}"
S3_PROCESSED   = f"s3://{BUCKET}/processed/movies"
S3_QUARANTINE  = f"s3://{BUCKET}/quarantine/movies"
SNS_TOPIC_NAME = "imdb-pipeline-alerts"
REGION         = "us-east-1"

KEEP_COLS = [
    "tconst", "titletype", "primarytitle",
    "startyear", "runtimeminutes", "genres",
    "averagerating", "numvotes"
]


def create_spark_session():
    return (SparkSession.builder
        .appName("IMDb-ETL-Pipeline")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate())


def read_tsv_from_s3(spark, path):
    logger.info(f"Reading: {path}")
    df = (spark.read
        .option("sep", "\t")
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(path))
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    logger.info(f"Loaded {df.count():,} rows")
    return df


def clean_imdb_nulls(df):
    for column in df.columns:
        df = df.withColumn(
            column,
            when(col(column) == "\\N", None).otherwise(col(column))
        )
    return df


def apply_data_quality(df):
    logger.info("Applying data quality checks...")
    df = df.withColumn("averagerating", col("averagerating").cast(DoubleType()))
    df = df.withColumn("numvotes", col("numvotes").cast(IntegerType()))

    df = df.withColumn("dq_passed",
        (col("averagerating").between(1.0, 10.0)) &
        (col("numvotes") > 0) &
        (col("primarytitle").isNotNull()) &
        (col("tconst").isNotNull())
    )

    passed_df = df.filter(col("dq_passed") == True).drop("dq_passed")
    failed_df = df.filter(col("dq_passed") == False).drop("dq_passed")

    total  = df.count()
    passed = passed_df.count()
    failed = failed_df.count()
    rate   = (passed / total * 100) if total > 0 else 0

    logger.info(f"Passed DQ: {passed:,} records ({rate:.1f}%)")
    logger.info(f"Failed DQ: {failed:,} records quarantined")

    return passed_df, failed_df, rate


def send_sns_notification(passed, failed, dq_rate):
    try:
        sns    = boto3.client("sns", region_name=REGION)
        topics = sns.list_topics()["Topics"]
        topic_arn = next(
            (t["TopicArn"] for t in topics if SNS_TOPIC_NAME in t["TopicArn"]),
            None
        )
        if topic_arn:
            sns.publish(
                TopicArn = topic_arn,
                Subject  = f"IMDb EMR Pipeline Succeeded - {TODAY}",
                Message  = f"Passed: {passed:,} | Quarantined: {failed:,} | DQ Rate: {dq_rate:.1f}%"
            )
            logger.info("SNS notification sent!")
    except Exception as e:
        logger.warning(f"SNS failed: {str(e)}")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("=== IMDb EMR PySpark Pipeline Started ===")

    try:
        # Step 1: Read
        basics_df  = read_tsv_from_s3(spark, f"{S3_RAW}/title.basics.tsv.gz")
        ratings_df = read_tsv_from_s3(spark, f"{S3_RAW}/title.ratings.tsv.gz")

        # Step 2: Clean nulls
        basics_df  = clean_imdb_nulls(basics_df)
        ratings_df = clean_imdb_nulls(ratings_df)

        # Step 3: Filter movies only
        basics_df = basics_df.filter(col("titletype") == "movie")
        logger.info(f"Movies only: {basics_df.count():,} records")

        # Step 4: Join
        joined_df = basics_df.join(ratings_df, on="tconst", how="inner")
        logger.info(f"After join: {joined_df.count():,} records")

        # Step 5: Select columns
        joined_df = joined_df.select(KEEP_COLS)

        # Step 6: Data Quality
        passed_df, failed_df, dq_rate = apply_data_quality(joined_df)

        # Step 7: Write clean Parquet
        (passed_df.write
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(S3_PROCESSED))
        logger.info(f"Clean data written to {S3_PROCESSED}")

        # Step 8: Write quarantine JSON
        if failed_df.count() > 0:
            (failed_df.write.mode("overwrite").json(S3_QUARANTINE))
            logger.info(f"Quarantine written to {S3_QUARANTINE}")

        # Step 9: Trigger Glue Crawler
        try:
            glue = boto3.client("glue", region_name=REGION)
            glue.start_crawler(Name="imdb-processed-crawler")
            logger.info("Glue Crawler triggered!")
        except Exception as e:
            logger.warning(f"Crawler trigger failed: {str(e)}")

        # Step 10: SNS notification
        send_sns_notification(passed_df.count(), failed_df.count(), dq_rate)

        logger.info("=== IMDb EMR PySpark Pipeline Completed ===")

    except Exception as e:
        logger.error(f"Pipeline FAILED: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
