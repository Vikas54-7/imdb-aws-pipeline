"""
IMDb Pipeline — Step 1: Upload Raw Files to S3
Uploads IMDb TSV.GZ files to S3 raw zone
Author: Vikas Pabba
"""

import boto3
import os
from datetime import datetime

# ─── Config ───────────────────────────────────────────────────────────────────
BUCKET      = "imdb-pipeline-vikas"
TODAY       = datetime.now().strftime("%Y/%m/%d")
S3_RAW_PATH = f"raw/imdb/{TODAY}"
DATA_FOLDER = "data"

# IMDb files to upload
FILES = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz"
]

def upload_files():
    s3 = boto3.client("s3", region_name="us-east-1")

    for filename in FILES:
        local_path = os.path.join(DATA_FOLDER, filename)

        # Check file exists locally
        if not os.path.exists(local_path):
            print(f"❌ File not found: {local_path}")
            print(f"   Make sure {filename} is in the data/ folder")
            continue

        s3_key = f"{S3_RAW_PATH}/{filename}"
        file_size = os.path.getsize(local_path) / (1024 * 1024)

        print(f"⬆️  Uploading {filename} ({file_size:.1f} MB)...")

        s3.upload_file(
            Filename  = local_path,
            Bucket    = BUCKET,
            Key       = s3_key,
            ExtraArgs = {"ContentType": "application/gzip"}
        )

        print(f"✅ Uploaded to s3://{BUCKET}/{s3_key}")

    print("\n🎉 All files uploaded successfully!")
    print(f"📂 S3 path: s3://{BUCKET}/{S3_RAW_PATH}/")

if __name__ == "__main__":
    upload_files()
