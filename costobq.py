from google.cloud import bigquery

def load_data_from_gcs(bucket_name, source_file_name, dataset_id, table_id):
    # Construct a BigQuery client object
    client = bigquery.Client()

    # Set the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Set the job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        autodetect=True,      # Automatically infer schema
    )

    # Construct the URI for the source file in GCS
    uri = f'gs://{bucket_name}/{source_file_name}'

    # Load data from GCS to BigQuery
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )

    print(f'Starting job {load_job.job_id}')

    load_job.result()  # Waits for the job to complete.

    print('Job finished.')
    destination_table = client.get_table(table_ref)
    print(f'Loaded {destination_table.num_rows} rows.')

# Example usage
load_data_from_gcs('your-bucket-name', 'your-file-name.csv', 'your-dataset-id', 'your-table-id')
