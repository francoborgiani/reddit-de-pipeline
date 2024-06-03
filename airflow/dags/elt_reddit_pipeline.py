from os import remove
from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

"""
DAG to extract Reddit data, load into AWS S3, and copy to AWS Reddit
"""

output_name = datetime.now().strftime("%Y%m%d")

schedule_interval = "@daily"
start_date = days_ago(1)

default_args = {"owner": "airflow", "depends_on_past": False, "retries": 1}

with DAG(
    dag_id="elt_reddit_pipeline",
    description="Reddit ELT",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["RedditETL"]
) as dag:
    extract_reddit_data = BashOperator(
        task_id="extract_reddit_data",
        bash_command=f"python /opt/airflow/extraction/extract_reddit_etl.py {output_name}",
        dag=dag
    )

    extract_reddit_data.doc_md = "Extract Reddit data and store as CSV"

    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"python /opt/airflow/extraction/upload_to_s3.py"
    )
