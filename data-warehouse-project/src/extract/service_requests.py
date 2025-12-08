from __future__ import annotations
import polars as pl

from extract.common import download_to_parquet
from logging_utils import get_logger

logger = get_logger(__name__)

# 311-specific config

BASE_URL_311 = "https://data.cityofnewyork.us/resource/erm2-nwe9.csv"
BLOCK_SIZE_311 = 100_000
MAX_WORKERS_311 = 4

COLUMNS_311 = [
    "unique_key", 
    "created_date", 
    "closed_date", 
    "agency", 
    "agency_name",
    "complaint_type", 
    "descriptor", 
    "location_type", 
    "incident_zip",
    "city", 
    "status", 
    "resolution_action_updated_date", 
    "borough", 
    "open_data_channel_type" # channel_type --> open_data_channel_type
]

# schema override forces incident_zip as string
SCHEMA_OVERRIDES_311 = {
    "incident_zip": pl.Utf8
}

DEFAULT_311_OUTPUT = "311_service_requests_data.parquet"

# extract_311_data specifies the parameters needed for download_parquet function
def extract_311_data(
    output_parquet: str = DEFAULT_311_OUTPUT,
) -> None:
    
    logger.info(
        f"EXTRACTING: 311 SERVICE REQUESTS DATA"
    )
    
    download_to_parquet(
        base_url=BASE_URL_311,
        columns=COLUMNS_311,
        output_parquet=output_parquet,
        block_size=BLOCK_SIZE_311,
    )
    
    logger.info(
        f"COMPLETED: 311 SERVICE REQUESTS DATA"
    )