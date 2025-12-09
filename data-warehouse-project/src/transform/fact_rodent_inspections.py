from __future__ import annotations
import polars as pl

from logging_utils import get_logger

logger = get_logger(__name__)

def build_fact_rodent_inspection(
    stg_rodent: pl.DataFrame,
    dim_time: pl.DataFrame,
    dim_rodent_location:pl.DataFrame,
    dim_rodent_inspection: pl.DataFrame,
    dim_rodent_result: pl.DataFrame
) -> pl.DataFrame:
    
    # creating rodent inspection fact table

    fact_rodent = (
        stg_rodent
        # location join
        .join(
            dim_rodent_location,
            left_on=["borough", "zip_code", "nta"],
            right_on=["borough", "zip_code", "neighborhood"],
            how="left"
        )
        
        # time join
        .join(
            dim_time.select(["time_dim_id", "date"]),
            left_on=pl.col("inspection_date").dt.date(),
            right_on=pl.col("date"),
            how="left"
        )
        
        # inspection join
        .join(
            dim_rodent_inspection,
            on=["job_ticket_or_work_order_id", 
                "job_id", "inspection_type", 
                "job_progress"
                ],
            how="left"
        )
        
        # result join
        .join(
            dim_rodent_result,
            left_on="result",
            right_on="inspection_result",
            how="left"
        )
        .select([
            pl.col("time_dim_id"),
            pl.col("rodent_location_dim_id"),
            pl.col("inspection_dim_id"),
            
            pl.col("job_ticket_or_work_order_id"),
            pl.col("job_id"),
            pl.col("job_progress"),
            
            pl.col("inspection_date"),
            pl.col("inspection_type"),
            pl.col("results"),
            
            pl.col("borough"),
            pl.col("zip_code"),
            pl.col("neighborhood"),
        ])
    )

    logger.info(
        f"fact_rodent_inspection: complete"
        f"rows: {fact_rodent.height}"
    )
    return fact_rodent
