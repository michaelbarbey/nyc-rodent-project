from __future__ import annotations
import polars as pl
from logging_utils import get_logger
logger = get_logger(__name__)

def build_dim_rodent_inspection(rodent_stg: pl.DataFrame) -> pl.DataFrame:
    
    dim = (
        rodent_stg.select([
            "job_ticket_or_work_order_id",
            "job_id",
            "inspection_type", # added, not included in documentation
            "job_progress"
            ]).unique().with_row_index("inspection_dim_id", offset=1)
    )
    logger.info(
        f"dim_rodent_inspection: complete"
        f"rows: {dim.height}"
    )
    return dim