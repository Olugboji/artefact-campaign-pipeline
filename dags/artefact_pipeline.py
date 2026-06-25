from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

default_args = {'retries': 2, 'retry_delay': timedelta(minutes=2)}

with DAG(
    'artefact_transform_dev',
    schedule_interval=None,
    start_date=datetime(2026, 6, 19),
    default_args=default_args,
    catchup=False,
    tags=['artefact', 'dev'],
) as dag:
    run_glue = GlueJobOperator(
        task_id='run_glue_transform',
        job_name='artefact-transform-dev',
        aws_conn_id='aws_default',
        region_name='us-east-1',
    )