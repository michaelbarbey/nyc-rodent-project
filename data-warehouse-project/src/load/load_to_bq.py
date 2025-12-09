from google.cloud import bigquery

PROJECT_ID = "nyc-dw-project"
DATASET_ID = "nyc_data_warehouse_project"
BUCKET_NAME = "nyc-dw-project"

def load_parquet_to_bq(
    client: bigquery.Client,
    table_id: str,
    gcs_uri: str,
    write_disposition: str = "WRITE_TRUNCATE",
):
  
    # loading parquet file from GCS into a BigQuery table.

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        autodetect=True,
    )

    print(f"{gcs_uri}: LOADING  | TABLE ID: {table_id}")
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    result = load_job.result()  
    table = client.get_table(table_id)
    print(f"{table.num_rows} LOADED INTO TABLE ID: {table_id}")
    return result

def main():
    client = bigquery.Client(project=PROJECT_ID)

    # 311 tables
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_location",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_location.parquet",
    )

    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_time",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_time.parquet",
    )

    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.fact_311_service_requests",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/fact_311_service_requests.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.311_service_requests_data",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/311_service_requests_data.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_agency",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_agency.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_status",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_status.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_channel_type",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_channel_type.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_complaint_type",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_complaint_type.parquet",
    )

    # rodent tables
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.rodent_inspection_data",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/rodent_inspection_data.parquet",
    )

    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_rodent_location",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_rodent_location.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_rodent_time",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_rodent_time.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_rodent_inspection",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_rodent_inspection.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.dim_rodent_result",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/dim_rodent_result.parquet",
    )
    
    load_parquet_to_bq(
        client,
        table_id=f"{PROJECT_ID}.{DATASET_ID}.fact_rodent_inspection",
        gcs_uri=f"gs://{BUCKET_NAME}/warehouse/fact_rodent_inspection.parquet",
    )

if __name__ == "__main__":
    main()