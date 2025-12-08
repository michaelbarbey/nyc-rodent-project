from __future__ import annotations
import polars as pl

from logging_utils import get_logger

logger = get_logger(__name__)

def build_stg_rodent(raw_rodent: pl.DataFrame) -> pl.DataFrame:
    logger.info(
        f"BUILDING: stg_rodent_inspection"
    )
    
    df = raw_rodent
    
    # ensures column datatype is datetime 
    if df.schema.get("inspection_date") == pl.Utf8:
        df = df.with_columns(
            pl.col("inspection_date").str.to_datetime(strict=False).alias("inspection_date")
        )
        
    # ensures column datatype is string
    if df.schema.get("zipcode") != pl.Utf8:
        df = df.with_columns(
            pl.col("zip_code").cast(pl.Utf8) # the cast method is a fancy way to change the datatype to string. part of the polars library and applies it to the entire column.
        )
    '''
    "job_ticket_or_work_order_id", "job_id", "job_progress", "inspection_date", "result",
    "borough", "inspection_type", "zip_code", "nta" # added attributes for analysis
    '''
    df = df.with_columns(
        pl.col("job_ticket_or_work_order_id").cast(pl.Utf8),
        pl.col("job_id").cast(pl.Utf8),
        pl.col("job_progress").cast(pl.Utf8),
        pl.col("result").cast(pl.Utf8),
        pl.col("borough").cast(pl.Utf8),
        pl.col("inspection_type").cast(pl.Utf8),
        pl.col("nta").cast(pl.Utf8),
    )
    
    logger.info(
        f"stg_rodent_inspection shape: {df.height} rows, {df.width} columns"
    )
    return df