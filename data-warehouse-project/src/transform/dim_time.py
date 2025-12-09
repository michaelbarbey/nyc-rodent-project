from __future__ import annotations
import polars as pl

from logging_utils import get_logger

logger = get_logger(__name__)

# time dimension [ created_date ] | [inspection_date]
def build_dim_time_from_date(
    df: pl.DataFrame,
    datetime_col: str,
) -> pl.DataFrame:
    
    df_non_null = df.filter(pl.col(datetime_col).is_not_null())
    dim_time = (
        df_non_null.select([
            pl.col("created_date").dt.date().alias("date"),
            pl.col("created_date").dt.year().alias("year"),
            pl.col("created_date").dt.month().alias("month"),
            pl.col("created_date").dt.strftime("%Y-%m").alias("YYYY-MM"),
            pl.col("created_date").dt.strftime("%b").alias("month_name"), # abbreviated month name
            pl.col("created_date").dt.day().alias("day"),
            pl.col("created_date").dt.week().alias("week"),
            pl.col("created_date").dt.quarter().alias("quarter"),
        ]).unique().with_row_index("time_dim_id", offset=1)
    )
    
    return dim_time