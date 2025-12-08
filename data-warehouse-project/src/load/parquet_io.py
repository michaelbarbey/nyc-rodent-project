import polars as pl
from pathlib import Path

from logging_utils import get_logger

logger = get_logger(__name__)

# confirms file path, creates directory if path doesnt exist
def _parent_path(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    
def write_parquet(df: pl.DataFrame, path: Path) -> None:
    _parent_path(path)
    logger.info(
        f"WRITING TO PATH: {path}"
    )
    df.write_parquet(path)