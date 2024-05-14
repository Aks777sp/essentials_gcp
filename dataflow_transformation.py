import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

def transform_data(element):
    # Perform transformation on the element
    # Example: Convert all strings to uppercase
    return {k: v.upper() if isinstance(v, str) else v for k, v in element.items()}

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'your-project-id'
    google_cloud_options.job_name = 'your-job-name'
    google_cloud_options.staging_location = 'gs://your-bucket-name/staging'
    google_cloud_options.temp_location = 'gs://your-bucket-name/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    p = beam.Pipeline(options=options)

    (p
     | 'Read from GCS' >> beam.io.ReadFromText('gs://your-bucket-name/your-file-name.csv', skip_header_lines=1)
     | 'Parse CSV' >> beam.Map(lambda line: dict(zip(['field1', 'field2', 'field3'], line.split(','))))
     | 'Transform Data' >> beam.Map(transform_data)
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            'your-project-id:your-dataset-id.your-table-id',
            schema='field1:STRING, field2:STRING, field3:STRING',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
     )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
