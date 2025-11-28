import matplotlib.pyplot as plt
import numpy as np
import snowflake.connector
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timezone
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, mode, to_date, from_utc_timestamp, split

class SnowflakeCharts:
    def __init__(self):
        
        self.spark = SparkSession.builder.appName("stream_analytics").config("spark.driver.memory", "4g").getOrCreate()
        try: 
            with open(os.getenv("rsakey_path"), "r") as f:
                private_key_str = f.read()
            private_key_body = re.sub("-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n", "", private_key_str)
            
            sf_conn = snowflake.connector.connect(
                account=os.getenv("sfURL").split('.')[0],
                user=os.getenv("sfUser"),
                database=os.getenv("sfDatabase"),
                schema=os.getenv("sfSchema"),
                warehouse=os.getenv("sfWarehouse"),
                private_key=private_key_body
            )

            self.sf_cursor = sf_conn.cursor()
        except Exception as e:
            raise RuntimeError(f"Snowflake connection failed in get_streamkeys_from_snowflake: {e} Exiting")

    def graph_charts(self, spark_df, unique_keys_list, output_dir="./charts"):
        plot_cols=["WINDOW_END", "AVG_SPEED", "MAX_SPEED", "ACCELERATION", "USER_MODE"]

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for session_key in unique_keys_list:
            print(f"Processing session: {session_key}")

            #plotting by time, so we order by WINDOW_END which is our x-axis
            session_spark_df = spark_df.filter(
                col("STREAM_KEY") == lit(session_key)
            ).select(plot_cols).orderBy("WINDOW_END")

            cur_user_mode = session_spark_df.select(mode('USER_MODE')).collect()[0][0]
            if session_spark_df.count() == 0:
                print(f"Warning: No data found for key {session_key}.")
                continue
                
            #convert back ton pandas for plotting
            session_pandas_df = session_spark_df.toPandas()
            
            # Ensure WINDOW_END is a datetime index for plotting
            if not pd.api.types.is_datetime64_any_dtype(session_pandas_df['WINDOW_END']):
                session_pandas_df['WINDOW_END'] = pd.to_datetime(session_pandas_df['WINDOW_END'])
            session_pandas_df = session_pandas_df.set_index('WINDOW_END')
            
            plt.figure(figsize=(15, 8))
            plt.plot(session_pandas_df.index, session_pandas_df['AVG_SPEED'], 
                    label='Avg Speed', linewidth=2)
            plt.plot(session_pandas_df.index, session_pandas_df['MAX_SPEED'], 
                    label='Max Speed', linestyle='--')
            plt.plot(session_pandas_df.index, session_pandas_df['ACCELERATION'], 
                    label='Acceleration', linestyle='-.')
            
            plt.title(f"{"activity" if cur_user_mode=="on_foot" else "driving"} Metrics for Session: {session_key}", fontsize=14)
            plt.xlabel("Time", fontsize=12)
            plt.ylabel("Metric Value", fontsize=12)
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()

            #chart saves
            filename = session_key.replace('@', '_at_').replace(':', '-').replace('.', '_')
            output_path = os.path.join(output_dir, f"{filename}_metrics.png")
            plt.savefig(output_path)
            plt.close()

            print(f"\nSuccessfully generated charts in the '{output_dir}' directory.")


    def generate_daily_weekly_charts(self, cur_date: str):
        ''''
        for daily we graph speed-related metrics per stream session
        for weekly we graph number of driving/walking sessions, avg of avg speeds, avg of max speeds, total distance traveled per day'''
        if isinstance(cur_date, str):
            try:
                # Attempt to parse the string into a datetime object
                # This assumes a YYYY-MM-DD or similar standard format
                end_date_dt = pd.to_datetime(cur_date).date()
            except ValueError:
                print(f"Error: Invalid date format received: {cur_date}")
                return
        else:
            print(f"Error: cur_date must be a string or datetime object.")
            return
        
        #format to sql format, need proper conversion from UTC in stream_key to match time of user's local time
        sql_end_date_str = end_date_dt.strftime('%Y-%m-%d')
        
        activity_query_weekly = f"""
        SELECT
        CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE AS session_date, 
        SPLIT_PART(t1.STREAM_KEY, '_', 1) AS user_email,
        
        COUNT(DISTINCT CASE WHEN t1.USER_MODE = 'driving' THEN t1.STREAM_KEY END) AS driving_sessions_weekly,
        COUNT(DISTINCT CASE WHEN t1.USER_MODE = 'on_foot' THEN t1.STREAM_KEY END) AS on_foot_sessions_weekly,
        
        SUM(t1.AVG_SPEED * 10) / 1000 AS total_distance_km, 
        
        AVG(CASE WHEN t1.USER_MODE = 'driving' THEN t1.AVG_SPEED ELSE NULL END) AS avg_speed_driving,
        
        AVG(CASE WHEN t1.USER_MODE = 'on_foot' THEN t1.AVG_SPEED ELSE NULL END) AS avg_speed_on_foot
        
        FROM
            {os.getenv("tableName")} t1
        WHERE
            CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE
            BETWEEN DATEADD(day, -7, '{sql_end_date_str}'::DATE) AND '{sql_end_date_str}'::DATE
            
        GROUP BY
            1, 2 
        ORDER BY
            session_date DESC;
        """

        #daily graphs
        # self.sf_cursor.execute(activity_query_daily)        
        # daily_res_df = self.spark.createDataFrame(self.sf_cursor.fetch_pandas_all()) #load as spark df
        # daily_unique_keys = [r[0] for r in daily_res_df.select("STREAM_KEY").distinct().collect()]
        # self.graph_daily_charts(spark_df=daily_res_df, unique_keys_list=daily_unique_keys)
        
        #weekly graphs
        self.sf_cursor.execute(activity_query_weekly)
        weekly_res_df = self.sf_cursor.fetch_pandas_all()
        print(weekly_res_df.head(10))




if __name__ == "__main__":
    load_dotenv()
    snoflakeChartObj = SnowflakeCharts()
    snoflakeChartObj.generate_daily_weekly_charts(cur_date="2025-11-29")