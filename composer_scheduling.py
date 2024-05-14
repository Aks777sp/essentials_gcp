from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'start_date': days_ago(1),
}

dag = DAG(
    'bigquery_sample_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

run_bigquery_query = BigQueryExecuteQueryOperator(
    task_id='run_bigquery_query',
    sql="""
    SELECT * FROM `your-project-id.your-dataset-id.your-table-id`
    """,
    use_legacy_sql=False,
    dag=dag,
)

run_bigquery_query
