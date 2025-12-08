from __future__ import annotations
import polars as pl

from extract.common import download_to_parquet
from logging_utils import get_logger

logger = get_logger(__name__)

# rodent inspection data url
BASE_URL_RODENT = 'https://data.cityofnewyork.us/resource/p937-wjvj.csv'
BLOCK_SIZE_RODENT = 100_000
MAX_WORKERS_RODENT = 4

COLUMNS_RODENT = [
    "job_ticket_or_work_order_id", # this is a foreign key from the 311 data
    "job_id", 
    "job_progress", 
    "inspection_date", 
    "result",
    "borough", 
    "inspection_type", 
    "zip_code", 
    "nta" # added attributes for analysis 
]

SCHEMA_OVERRIDES_RODENT = {
    "zip_code": pl.utf8,    # forces zipcode datatype to strings
}

DEFAULT_RODENT_OUTPUT = "rodent_inspection_data.parquet"

def extract_rodent_data(
    output_parquet: str = DEFAULT_RODENT_OUTPUT,
) -> None:
    
    # extracts data in chunks or blocks
    logger.info(
        f"EXTRACTING: RODENT INSPECTION DATA"
    )
    
    download_to_parquet(
        base_url=BASE_URL_RODENT,
        columns=COLUMNS_RODENT,
        output_parquet=output_parquet,
        block_size=BLOCK_SIZE_RODENT,
        max_workers=MAX_WORKERS_RODENT,
        schema_overrides=SCHEMA_OVERRIDES_RODENT,
    )
    
    logger.info(
        f"COMPLETED: RODENT INSPECTION DATA"
    )
    