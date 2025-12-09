
# single use script, will be updated to a modular function

from google.cloud import storage
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_PARQUET_DIR = PROJECT_ROOT / "notebooks"

PROJECT_ID = "nyc-dw-project"
BUCKET_NAME = "nyc-dw-project"

# path to parquet files
# LOCAL_PARQUET_DIR = Path("Repo/data-warehouse-project/")

def upload_file(client: storage.Client, bucket_name: str, local_path: Path, gcs_path: str):
    if not local_path.is_file():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path))
    print(f"Uploaded {local_path} -> gs://{bucket_name}/{gcs_path}")
    
def main():
    client = storage.Client(project=PROJECT_ID)
    
    print(f"PROJECT_ROOT      = {PROJECT_ROOT}")
    print(f"LOCAL_PARQUET_DIR = {LOCAL_PARQUET_DIR}")
    print("Files in LOCAL_PARQUET_DIR:")
    for p in sorted(LOCAL_PARQUET_DIR.glob("*.parquet")):
        print("  -", p.name)

    files_to_upload = [
        "311_service_requests_data.parquet",
        "rodent_inspection_data.parquet",
        "dim_time.parquet",
        "dim_agency.parquet",
        "dim_status.parquet",
        "dim_channel_type.parquet",
        "dim_location.parquet",
        "dim_complaint_type.parquet",
        "fact_311_service_requests.parquet",
        "dim_rodent_location.parquet",
        "dim_rodent_time.parquet",
        "dim_rodent_inspection.parquet",
        "dim_rodent_result.parquet",
        "fact_rodent_inspection.parquet"
    ]

    for filename in files_to_upload:
        local_file = LOCAL_PARQUET_DIR / filename
        gcs_key = f"warehouse/{filename}"   # folder prefix in the bucket
        upload_file(client, BUCKET_NAME, local_file, gcs_key)

if __name__ == "__main__":
    main()