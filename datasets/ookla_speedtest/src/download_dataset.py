import boto3
import os
from pathlib import Path
import logging
from botocore import UNSIGNED
from botocore.client import BaseClient
from botocore.config import Config
from src.helpers import GEOPARQUET_DIR, BUCKET_NAME, REGION_NAME, QUARTERS

# Configure logging (less noise from botocore)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(
    logging.WARNING
)  # suppressing botocore checksum spam

# Making the directory
# os.makedirs(GEOPARQUET_DIR, exist_ok=True)


# Creating the s3 client with anonymous token
def create_s3_client() -> BaseClient:
    return boto3.client(
        "s3", region_name=REGION_NAME, config=Config(signature_version=UNSIGNED)
    )


# Creating the download file function and creating the loggers
def prepare_download(
    s3_client: BaseClient,
    s3_key: str,
    output_path: str = GEOPARQUET_DIR,
    target_bucket_name=BUCKET_NAME,
) -> None:
    filename = os.path.basename(s3_key)
    # checking if directory exists
    os.makedirs(output_path, exist_ok=True)
    local_file_path = os.path.join(output_path, filename)
    if os.path.exists(local_file_path):
        logger.info(f"File {local_file_path} already exists. Skipping download.")
        return
    logger.info(f"Downloading s3://{target_bucket_name}/{s3_key} to {local_file_path}")
    try:
        s3_client.download_file(target_bucket_name, s3_key, local_file_path)
        logger.info(f"Successfully downloaded {local_file_path}")
    except Exception as e:
        logger.error(f"Error downloading s3://{target_bucket_name}/{s3_key}: {e}")


# Doing the actual downloading; calling the S3 client, and putting the S3 filenames together
def download_files(year: int, quarters: dict = QUARTERS) -> None:
    """
    Downloads the performance data files from the target Ookla S3 bucket for 1 year to a local directory
    Disclaimer: there is similarity in the naming convention download_file and download_files, which cannot be changed
    because of a requirement by botocore. Please note the difference.
    """
    s3_client = create_s3_client()
    downloaded_files = []
    formats = ["parquet"]  # "shapefiles" is an option to download too
    service_types = ["mobile", "fixed"]
    for quarter, month in quarters.items():
        for format_type in formats:
            for service_type in service_types:
                filename = f"{year}-{month}-01_performance_{service_type}_tiles.parquet"
                s3_key = f"{format_type}/performance/type={service_type}/year={year}/quarter={quarter}/{filename}"
                prepare_download(s3_client, s3_key, GEOPARQUET_DIR, BUCKET_NAME)
                local_file_path = Path(GEOPARQUET_DIR) / filename
                downloaded_files.append(local_file_path)
    return downloaded_files
