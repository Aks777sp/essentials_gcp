import os
from google.cloud import bigquery

def gcs_to_bigquery(data, context):
    client = bigquery.Client()

    dataset_id = 'your-dataset-id'
    table_id = 'your-table-id'
    bucket_name = data['bucket']
    source_file_name = data['name']

    uri = f'gs://{bucket_name}/{source_file_name}'

    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )

    load_job.result()

    print(f'File {source_file_name} loaded into {dataset_id}:{table_id}.')
