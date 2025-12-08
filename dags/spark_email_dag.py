from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv
import os

import sys
#add the analytics folder path for airflow
sys.path.append("/Users/hansonli/Desktop/coremotion_streaming")
from analytics.charts import SnowflakeCharts, email_to_user
from spark_pipeline.kafka_to_snowflake import deduplicate_snowflake_table
from datetime import date
import pendulum

cst_tz = pendulum.timezone("America/Chicago")

PROJECT_ROOT = "/Users/hansonli/Desktop/coremotion_streaming"

SPARK_APP = f"{PROJECT_ROOT}/spark_pipeline/kafka_to_snowflake.py"
ANALYTICS_ENV = f"{PROJECT_ROOT}/analytics/.env"

def run_analytics(cur_date, analytics_env_path):
    '''helper function to call the analytics functions + user email send'''
    load_dotenv(analytics_env_path) #path to the .env file in analytics folder
    charts = SnowflakeCharts()
    recipients = charts.generate_daily_weekly_charts(cur_date=cur_date)
    email_to_user(user_email_list=recipients)

default_args = {"owner": "coremotion", "retries": 1, "retry_delay": timedelta(seconds=20)}

with DAG(
    "spark_ingest_and_email",
    default_args=default_args,
    schedule="0 11 21 * * *", #run at 11am and 11pm "0 11 21 * * *"
    start_date=datetime(2025, 11, 1, tzinfo=cst_tz), #set tz to cst
    catchup=False,
) as dag:
    spark_ingest = SparkSubmitOperator(
        task_id="kafka_to_snowflake_spark",
        application=SPARK_APP,
        name="kafka2snowflake",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,net.snowflake:spark-snowflake_2.13:3.0.0",
        env_vars={
            "ANALYTICS_ENV": ANALYTICS_ENV
        },
        conn_id="spark_local", #Important: need to go to airflow dashboard -> admin -> connections -> add new connections ->  
    )

    dedupe_table = PythonOperator(
        task_id="dedupe_snowflake_table",
        python_callable=deduplicate_snowflake_table
    )

    send_email = PythonOperator(
        task_id="gen_charts_and_send_emails",
        python_callable=run_analytics,
        op_kwargs={
            "cur_date": date.today().strftime("%Y-%m-%d"),
            "analytics_env_path": ANALYTICS_ENV
        },
    )

    spark_ingest >> dedupe_table >> send_email
