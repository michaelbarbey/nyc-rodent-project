from __future__ import annotations
import polars as pl

from logging_utils import get_logger

logger = get_logger(__name__)

def build_dim_rodent_location(rodent_stg: pl.DataFrame) -> pl.DataFrame:
    
    # creating location | no 'city' column, renaming nta to neighborhood
    dim = (
        rodent_stg.select([
            #    pl.lit("NY").alias("state"),
            #    pl.lit("USA").alias("country"),
            pl.col("nta").alias("neighborhood"),
            pl.col("borough"),
            pl.col("zip_code"),
    ]).unique().with_row_index("rodent_location_dim_id", offset=1) # surrogate key
    )
    
    logger.info(
        f"dim_rodent_location: complete"
        f"rows: {dim.height}"
    )
    return dim