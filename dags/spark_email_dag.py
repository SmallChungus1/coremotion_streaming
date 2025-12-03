from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from analytics.charts import SnowflakeCharts, email_to_user
from dotenv import load_dotenv
import os

def run_analytics(ds, **_):
    load_dotenv(os.getenv("ANALYTICS_ENV")) #path to the .env file in analytics folder
    charts = SnowflakeCharts()
    recipients = charts.generate_daily_weekly_charts(cur_date=ds)
    email_to_user(user_email_list=recipients)

default_args = {"owner": "coremotion", "retries": 1, "retry_delay": timedelta(seconds=20)}

with DAG(
    "spark_ingest_and_email",
    default_args=default_args,
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    spark_ingest = SparkSubmitOperator(
        task_id="kafka_to_snowflake",
        application="/opt/airflow/dags/spark_pipeline/kafka_to_snowflake.py",
        name="kafka2snowflake",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,net.snowflake:spark-snowflake_2.13:3.0.0",
        env_vars={"ANALYTICS_ENV": "/opt/airflow/dags/analytics/.env"},
        conn_id="spark_default",
    )

    send_email = PythonOperator(
        task_id="generate_charts_and_email",
        python_callable=run_analytics,
        provide_context=True,
        env={"ANALYTICS_ENV": "/opt/airflow/dags/analytics/.env"},
    )

    spark_ingest >> send_email
