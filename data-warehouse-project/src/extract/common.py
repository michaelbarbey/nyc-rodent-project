from __future__ import annotations
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Optional, Tuple # helps annotate function parameters and makes them static

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from logging_utils import get_logger

logger = get_logger(__name__)
ArrowTableWithOffset = Tuple[int, "pa.Table"] 

def download_block(
    offset: int,
    *,
    base_url: str,
    columns: list[str],
    block_size: int,
    schema_overrides: Optional[Dict[str, pl.DataType]] = None,) -> Optional[ArrowTableWithOffset]:
    
    # Socrata API in SQL -- pending to test SODA 3
    # downloads a block from Socrata using sql queries
    
    soql = (
        f"SELECT {', '.join(columns)} "
        f"LIMIT {block_size} OFFSET {offset}"  
    )
    
    encoded_query = urllib.parse.quote(soql, safe='')
    url = f"{base_url}?$query={encoded_query}"
    
    try: 
        df_block = pl.read_csv(
            url, 
            columns=columns,
            schema_overrides=schema_overrides or {},
            )
        
        if df_block.height == 0:
            logger.info(
                f"offset: {offset}"
                f"status: no rows returned"
            )
            return None
        table = df_block.to_arrow() # File type transformation to arrow
        return (offset, table)
    except Exception as e:
        logger.error(
            f"offset: {offset}"
            f"status: error downloading"
            f"error: {e}"
        )
        return None

# utilized the same execution logic from the jupyter notebook into a reusable function
def download_to_parquet(
    # defining specific parameter datatypes
    *,
    base_url: str,
    columns: list[str],
    output_parquet: str | Path,
    block_size: int = 100_000,
    max_workers: int = 4,
    schema_overrides: Optional[Dict[str, pl.DataType]] = None,
) -> None:
    
    # the function downloads the entire socrata dataset in blocks and creates a parquet file.
    
    output_parquet = Path(output_parquet)
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    
    offset = 0
    first_block = True
    writer: Optional[pq.ParquetWriter] = None # file name variable
    
    # monitoring download workflow, rather than console prints
    logger.info(
        f"download url: {base_url} "
        f"{output_parquet} "
        f"block size: {block_size}"
        f"max workers: {max_workers}"
    )
    
    while True:
        
        offsets = [offset + i*block_size for i in range(max_workers)]
        all_done = False
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(
                download_block, 
                o, 
                base_url=base_url, 
                columns=columns, 
                block_size=block_size, 
                schema_overrides=schema_overrides,
                ): o for o in offsets
                       }
            
            for future in as_completed(futures):
                result = future.result()
                if result is None: # the loop will skip empty futures
                    all_done = True
                    continue
                
                block_offset, table = result
                
                # Initialize ParquetWriter on first block
                if first_block:
                    writer = pq.ParquetWriter(
                        output_parquet, 
                        table.schema, 
                        compression='snappy'
                        )
                    first_block = False
                
                writer.write_table(table)
                logger.info(
                    f"block offset: {block_offset}"
                    f"status: downloaded"
                    )
                
        if all_done:
            break
                
        offset += max_workers * block_size
        
    # closing file
    if writer:
        writer.close()
        logger.info(
            f"data download: completed"
            f"parquet file created"
            )
    else:
        logger.warning(
            f"data download: incomplete"
            f"parquet file not created"
            )