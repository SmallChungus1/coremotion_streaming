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
            
            self.sf_conn = snowflake.connector.connect(
                account=os.getenv("sfURL").split('.')[0],
                user=os.getenv("sfUser"),
                database=os.getenv("sfDatabase"),
                schema=os.getenv("sfSchema"),
                warehouse=os.getenv("sfWarehouse"),
                private_key=private_key_body
            )

            self.sf_cursor = self.sf_conn.cursor()
        except Exception as e:
            raise RuntimeError(f"Snowflake connection failed in get_streamkeys_from_snowflake: {e} Exiting")

    def graph_charts(self, time_mode, session_pandas_df, session_key: str, output_dir="./charts"):
        # func ow generalized to take the data it needs (a Pandas df) no longer needs to deal with Spark or unique keys list iteration.
        
        x_axis_col = "WINDOW_END" if time_mode == "daily" else "SESSION_DATE"
        plot_cols = ["WINDOW_END", "AVG_SPEED", "MAX_SPEED", "ACCELERATION"]
        filename = session_key.replace('@', '_at_').replace(':', '-').replace('.', '_')

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        if session_pandas_df.empty:
            print(f"Warning: No data found for key {session_key}.")
            return
            
        cur_user_mode = session_pandas_df['USER_MODE'].iloc[0]
            
        session_pandas_df[x_axis_col] = pd.to_datetime(session_pandas_df[x_axis_col])
        session_pandas_df = session_pandas_df.set_index(x_axis_col)
        
        #dual-axis graph daily metrics that invovles
        if time_mode=="daily":
            fig, ax1 = plt.subplots(figsize=(15, 8))
            
            # speed metrics on left axis
            ax1.plot(session_pandas_df.index, session_pandas_df['AVG_SPEED'], 
                    label='Avg Speed (m/s)', linewidth=2, color='darkcyan')
            ax1.plot(session_pandas_df.index, session_pandas_df['MAX_SPEED'], 
                    label='Max Speed (m/s)', linestyle='--', color='lightblue')
            ax1.plot(session_pandas_df.index, session_pandas_df['ACCELERATION'], 
                    label='Acceleration (m/s^2)', linestyle='-.', color='darkorange')
            
            ax1.set_xlabel("Time", fontsize=12)
            ax1.set_ylabel("Speed & Acceleration (m/s, m/s2)", fontsize=12, color='black')
            ax1.grid(True, linestyle='--', alpha=0.6)
            
            # distance traveled on right axis
            ax2 = ax1.twinx() #creates the second axis sharing the same X-axis
                        
            ax2.plot(session_pandas_df.index, session_pandas_df['CUMULATIVE_DISTANCE_KM'], 
                    label='Distance Traveled (km)', linewidth=2.5, color='red')
            
            ax2.set_ylabel("Cumulative Distance (km)", fontsize=12, color='red')
            ax2.tick_params(axis='y', labelcolor='red')
                        
            lines, labels = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines + lines2, labels + labels2, loc='upper left')
            
            plt.title(f"{"activity" if cur_user_mode=="on_foot" else "driving"} Metrics vs. Distance Traveled: {session_key}", fontsize=14)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()

            # chart saves
            filename = session_key.replace('@', '_at_').replace(':', '-').replace('.', '_')
            output_path = os.path.join(output_dir, f"{filename}_{time_mode}_metrics.png")
            plt.savefig(output_path)
            plt.close()

        else:
            #speed charts
            plt.figure(figsize=(15, 8))
            plt.bar(x='AVG_SPEED_DRIVING', height=session_pandas_df['AVG_SPEED_DRIVING'],
                    label="Avg Speed Driving")
            plt.bar(x='AVG_SPEED_ON_FOOT',height=session_pandas_df['AVG_SPEED_ON_FOOT'], 
                    label='Avg Speed On Foot')
            plt.title(f"Movement Analysis for week of {session_key.split("_")[-1]}", fontsize=14)
            plt.xlabel("Days", fontsize=12)
            plt.ylabel("Speed (m/s)", fontsize=12)
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            #chart saves
            output_path = os.path.join(output_dir, f"{filename}_{time_mode}_metrics.png")
            plt.savefig(output_path)
            plt.close()
            
            #drive/walk freq chart
            plt.figure(figsize=(15,8))
            plt.bar(x='DRIVING_SESSIONS_WEEKLY', height=session_pandas_df['DRIVING_SESSIONS_WEEKLY'], label="# driving sessions")
            plt.bar(x='ON_FOOT_SESSIONS_WEEKLY', height=session_pandas_df['ON_FOOT_SESSIONS_WEEKLY'], label="# walking sessions")
            plt.title(f"Overview for week of {session_key.split("_")[-1]}", fontsize=14)
            plt.xlabel("Days", fontsize=12)
            plt.ylabel("# of sessions", fontsize=12)
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            #chart saves
            output_path = os.path.join(output_dir, f"{filename}_{time_mode}_overview.png")
            plt.savefig(output_path)
            plt.close()

            #distance travelled chart
            plt.figure(figsize=(15,8))
            plt.plot(session_pandas_df.index, session_pandas_df['TOTAL_DISTANCE_KM'], label="km traveled")
            plt.title(f"Distance traveled for week of {session_key.split("_")[-1]}", fontsize=14)
            plt.xlabel("Days", fontsize=12)
            plt.ylabel("Distance (km)", fontsize=12)
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            #chart saves
            output_path = os.path.join(output_dir, f"{filename}_{time_mode}_distances.png")
            plt.savefig(output_path)
            plt.close()

        print(f"\nSuccessfully generated chart for {session_key}.")


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
        
        # format to sql format, need proper conversion from UTC in stream_key to match time of user's local time
        sql_end_date_str = end_date_dt.strftime('%Y-%m-%d')
        
        ### Weekly graph generation ###
        
        #get user emails, and aggregate on that per week
        weekly_unique_emails_query = f"""
        WITH FilteredData AS (
            SELECT
                -- Select the email part and the complex local date for filtering
                SPLIT_PART(t.STREAM_KEY, '_', 1) AS USER_EMAIL,
                CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE AS SESSION_DATE_LOCAL
            FROM 
                {os.getenv("tableName")} t
        )
        SELECT 
            DISTINCT USER_EMAIL
        FROM 
            FilteredData
        WHERE 
            -- Now filter on the calculated local date against the input date
            SESSION_DATE_LOCAL = '{sql_end_date_str}'::DATE;
        """

        self.sf_cursor.execute(weekly_unique_emails_query)
        user_email_list = [r[0] for r in self.sf_cursor.fetchall()]
        print(user_email_list)
        for cur_user_email in user_email_list:
            #sql query for each user
            activity_query_weekly = f"""
            SELECT
            CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE AS session_date, 
            SPLIT_PART(t1.STREAM_KEY, '_', 1) AS USER_EMAIL,
            
            COUNT(DISTINCT CASE WHEN t1.USER_MODE = 'driving' THEN t1.STREAM_KEY END) AS driving_sessions_weekly,
            COUNT(DISTINCT CASE WHEN t1.USER_MODE = 'on_foot' THEN t1.STREAM_KEY END) AS on_foot_sessions_weekly,
            
            SUM(t1.AVG_SPEED * 10) / 1000 AS total_distance_km, 
            
            AVG(CASE WHEN t1.USER_MODE = 'driving' THEN t1.AVG_SPEED ELSE NULL END) AS avg_speed_driving,
            
            AVG(CASE WHEN t1.USER_MODE = 'on_foot' THEN t1.AVG_SPEED ELSE NULL END) AS avg_speed_on_foot,
            
            MODE(t1.USER_MODE) AS USER_MODE
            
            FROM
                {os.getenv("tableName")} t1
            WHERE
                USER_EMAIL = '{cur_user_email}'
                AND
                CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE
                BETWEEN DATEADD(day, -7, '{sql_end_date_str}'::DATE) AND '{sql_end_date_str}'::DATE
            GROUP BY
                1, 2 
            ORDER BY
                session_date DESC;
            """
            
            print("--- WEEKLY AGGREGATION RESULT ---")
            self.sf_cursor.execute(activity_query_weekly)
            weekly_res_df = self.sf_cursor.fetch_pandas_all()
            print(weekly_res_df.head(10))

            self.graph_charts("weekly", weekly_res_df, f"{ cur_user_email}_{cur_date}")

        ### Daily graph generation ###
        
        # 1. Query Snowflake to get ALL unique STREAM_KEYs for the specific day
        #    - This result is a small list, safe to collect.
        daily_session_keys_query = f"""
        SELECT 
            DISTINCT STREAM_KEY 
        FROM {os.getenv("tableName")}
        WHERE 
            CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE = '{sql_end_date_str}'::DATE;
        """
        self.sf_cursor.execute(daily_session_keys_query)
        
        daily_unique_keys_raw = self.sf_cursor.fetchall() 
        # Convert to simple list of strings for iteration
        daily_unique_keys = [r[0] for r in daily_unique_keys_raw] 
        
        print(f"\n--- DAILY GRAPHING: Found {len(daily_unique_keys)} sessions for {sql_end_date_str} ---")
        
        # 2. Iterate through the small list of keys and run a targeted query for each one
        for session_key in daily_unique_keys:
            if isinstance(session_key, list):
                session_key = session_key[0] #sometimes the key is wrapped in a list
            # Query to fetch ALL 10-second window data for THIS specific session_key ONLY
            daily_data_query = f"""
            SELECT 
                WINDOW_END, AVG_SPEED, MAX_SPEED, ACCELERATION, USER_MODE,
                SUM(AVG_SPEED * 10) OVER (ORDER BY WINDOW_END) / 1000 AS cumulative_distance_km
            FROM {os.getenv("tableName")} as t
            WHERE 
                STREAM_KEY = '{session_key}'
            ORDER BY
                WINDOW_END;
            """
            
            # Execute and fetch the small chunk of data (one session) directly into a Pandas DataFrame
            # This avoids creating any large Spark DataFrames locally.
            session_df = pd.read_sql(daily_data_query, self.sf_conn)
            self.graph_charts("daily", session_df, session_key)

        print("Finished generating daily charts.")

if __name__ == "__main__":
    load_dotenv()
    snoflakeChartObj = SnowflakeCharts()
    snoflakeChartObj.generate_daily_weekly_charts(cur_date="2025-11-26")