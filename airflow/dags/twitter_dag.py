from datetime import datetime
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from os.path import join


with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=join(
            "/home/felipe/projetos/estudos/datapipeline/datalake",
            "twitter_aluraonline",
            "extract_date={{ ds }}",
            "AluraOnline_{{ ds_nodash }}.json"
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id='transform_twitter_aluraonline',
        application=(
            '/home/felipe/projetos/estudos/datapipeline/spark/transformation.py'
        ),
        name='twitter_transformation',
        application_args=[
            "--src",
            "/home/felipe/projetos/estudos/datapipeline/datalake/bronze/twitter_aluraonline/extract_date=2022-08-25",

            "--dest",
            "/home/felipe/projetos/estudos/datapipeline/"
            "datalake/silver/twitter_aluraonline",

            "--process-date",
            "{{ ds }}"
        ]

    )
