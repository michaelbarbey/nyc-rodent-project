from pathlib import Path

# global project directory

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

# file names
RAW_311_PATH = RAW_DIR / "311_service_requests_data.parquet"
RAW_RODENT_PATH = RAW_DIR / "rodent_inspection_data.parquet"

# dimension and fact table paths

DIM_AGENCY_PATH = WAREHOUSE_DIR / "dim_agency.parquet"
DIM_CHANNEL_PATH = WAREHOUSE_DIR / "dim_channel.parquet"
DIM_COMPLAINT_PATH = WAREHOUSE_DIR / "dim_complaint_type.parquet"
DIM_LOCATION_PATH = WAREHOUSE_DIR / "dim_location.parquet"
DIM_RODENT_INSPECTION_PATH = WAREHOUSE_DIR / "dim_rodent_inspection.parquet"
DIM_RODENT_RESULT_PATH = WAREHOUSE_DIR / "dim_rodent_result.parquet"
DIM_RODENT_TIME_PATH = WAREHOUSE_DIR / "dim_rodent_time.parquet"
DIM_STATUS_PATH = WAREHOUSE_DIR / "dim_status.parquet"
DIM_TIME_PATH = WAREHOUSE_DIR / "dim_time.parquet"

FACT_311_PATH = WAREHOUSE_DIR / "fact_311_service_requests.parquet"
FACT_RODENT_PATH = WAREHOUSE_DIR / "fact_rodent_inspection.parquet"