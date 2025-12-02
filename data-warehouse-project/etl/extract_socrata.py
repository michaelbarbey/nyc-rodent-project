import polars as pl             # faster and more efficient than pandas
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed

import urllib.parse
# base_url = 'https://data.cityofnewyork.us/api/v3/views/p937-wjvj/query.csv'
block_size = 100_000  # Number of rows per block
#output_parquet = 'rodent_inspection_data.parquet'
output_parquet = '311_service_requests_data.parquet'

base_url = 'https://data.cityofnewyork.us/resource/erm2-nwe9.csv'
max_workers = 4 # Number of threads for parallel processing

columns = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "location_type", "incident_zip",
    "city", "status", "resolution_action_updated_date", "borough",
    "latitude", "longitude"
]

def download_block (offset):
    # Socrata API in SQL -- may test json format later
    soql = (
        f"SELECT {', '.join(columns)} "
        f"LIMIT {block_size} OFFSET {offset}"  
    )
    
    encoded_query = urllib.parse.quote(soql, safe='')
    url = f"{base_url}?$query={encoded_query}"
    
    try: 
        df_block = pl.read_csv(url, 
                               columns=columns, 
                               schema_overrides={"incident_zip": pl.Utf8}
                               )
        if df_block.height == 0:
            return None
        table = df_block.to_arrow() # File type transformation to arrow
        return (offset, table)
    except Exception as e:
        print(f"Error downloading block at offset {offset}: {e}")
        return None
    
# download loop and offset counter

offset = 0
first_block = True
writer = None

while True:
    # Use ThreadPoolExecutor to download multiple blocks in parallel
    offsets = [offset + i*block_size for i in range(max_workers)]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_block, o): o for o in offsets}
        
        all_done = False
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                all_done = True
                continue
            
            block_offset, table = result
            
            # Initialize ParquetWriter on first block
            if first_block:
                writer = pq.ParquetWriter('/models/311_service_requests_data.parquet', table.schema, compression='snappy')
                first_block = False
            
            writer.write_table(table)
            print(f"Downloaded and wrote block at offset {block_offset}")
    if all_done:
        break
    offset += max_workers * block_size
    
# Closing file writer
if writer:
    writer.close()

print("All data downloaded and saved to Parquet.")