import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import snowflake.connector
import os
from dotenv import load_dotenv
import re
from datetime import datetime, timezone
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, mode, to_date, from_utc_timestamp, split
import seaborn as sns
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import smtplib
from pathlib import Path

matplotlib.use('Agg') #use agg backend to prevent rendering UI

BASE_DIR = Path(__file__).resolve().parent #need to get base dir for the sf_rsa_key file, since airflow can't find the relative path

def email_to_user(img_dir="charts", user_email_list=[], hard_braking_count=[], sharp_turn_count=[], rapid_accl_count=[], total_drive_count=[]):
        '''
        Emails created charts to each unique email address using html templating
        '''
        img_dir = BASE_DIR / img_dir #full path for airflow
        print(f"img_dir: {img_dir}")

        for user_email, cur_hard_braking_count, cur_sharp_turn_count, cur_rapid_accl_count, cur_total_drive_count in zip(user_email_list, hard_braking_count, sharp_turn_count, rapid_accl_count, total_drive_count):
            msg = MIMEMultipart("related")
            msg["Subject"] = f"Movement Analytics Report {datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}"
            msg["From"] = os.getenv("emailSender")
            

            html = """
            <html>
            <body>
                <h2>Your movement analytics</h2>
                <img src="cid:chart_img">
            </body>
            </html>
            """

            daily_html_parts  = ["<h2>Your Daily Report for All Sessions</h2>"]
            weekly_html_parts = ["<h2>Your Weekly Overview</h2>"]
            weekly_html_parts.append(f'<h4>Weekly driving anamoly detection report: Out of {cur_total_drive_count} total drives, you had: # {cur_hard_braking_count} of hard turns | # {cur_sharp_turn_count} of sharp turns | #  {cur_rapid_accl_count} of rapid accelerations </h4>')
            

            alt = MIMEMultipart("alternative")
            msg.attach(alt)
            msg["To"] = user_email
        
            for filename in os.listdir(img_dir):
                
                #aggregate content by user emails
                if not filename.endswith(".png"):
                    continue
                
                #to match user email must use same processing as we did when saving the files
                if user_email.replace('@', '_at_').replace(':', '-').replace('.', '_') not in filename:
                    continue

                filepath = os.path.join(img_dir, filename)
                print(filepath)
                cid = filename.replace(".", "_")  # unique content-id

                if "_daily_" in filename:
                    daily_html_parts.append(
                        f'<p><img src="cid:{cid}" style="max-width:600px;"></p>'
                    )
                else:
                    weekly_html_parts.append(
                        f'<p><img src="cid:{cid}" style="max-width:600px;"></p>'
                    )

                #mime embed image into html
                with open(filepath, "rb") as f:
                    img = MIMEImage(f.read(), _subtype="png")

                img.add_header("Content-ID", f"<{cid}>")

                # if SEND_AS_ATTACHMENT:
                #     # inline HTML + attachment version
                #     img.add_header("Content-Disposition", "attachment", filename=filename)

                msg.attach(img)

            #build final html after getting the parts, for each user
            daily_html_parts.append("<br>")
            weekly_html_parts.append("</body></html>")
            final_html = "<html><body>" + "\n".join(daily_html_parts + weekly_html_parts) + "</html>"

            alt.attach(MIMEText(final_html, "html"))

            #smtp gmail auth with google's App Password thing since we cant mfa
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(os.getenv("emailSender"), os.getenv("gmailAppPass"))
                server.send_message(msg)

            print(f"Email sent to {user_email}")

        #clean up charts dir after sending all mails
        for filename in os.listdir(img_dir):
            os.remove(os.path.join(img_dir, filename))

class SnowflakeCharts:
    def __init__(self):
        self.user_email_list = [] #set in generate_daily_weekly_charts
        self.spark = SparkSession.builder.appName("stream_analytics").config("spark.driver.memory", "4g").getOrCreate()
        try: 
            full_rsakey_path = BASE_DIR / os.getenv("rsakey_path") #full path, needed for airflow to find sf pem file

            with open(full_rsakey_path, "r") as f:
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

    def graph_charts(self, time_mode, session_pandas_df, session_key: str, weekly_attitude_df=None, output_dir="charts"):
        # func ow generalized to take the data it needs (a Pandas df) no longer needs to deal with Spark or unique keys list iteration.
        
        output_dir = BASE_DIR / output_dir #full path for airflow
        print(f"output_dir: {output_dir}")

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

        if weekly_attitude_df is not None:
            weekly_attitude_df[x_axis_col] = pd.to_datetime(weekly_attitude_df[x_axis_col])
            weekly_attitude_df = weekly_attitude_df.set_index(x_axis_col)
        
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
                    label='Distance Traveled (m)', linewidth=2.5, color='red')
            
            ax2.set_ylabel("Cumulative Distance (m)", fontsize=12, color='red')
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


            #distance travelled charts
            plt.figure(figsize=(15,8))
            plt.plot(session_pandas_df.index, session_pandas_df['TOTAL_DISTANCE_KM'], label="meters traveled")
            plt.title(f"Distance traveled for week of {session_key.split("_")[-1]}", fontsize=14)
            plt.xlabel("Days", fontsize=12)
            plt.ylabel("Distance (m)", fontsize=12)
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.6)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            #chart saves
            output_path = os.path.join(output_dir, f"{filename}_{time_mode}_distances.png")
            plt.savefig(output_path)
            plt.close()

            #violin for yaw/pitch/roll distributions
            if weekly_attitude_df is not None:

                plt.figure(figsize=(10, 5))
                sns.violinplot(
                    data=weekly_attitude_df,
                    x="USER_MODE",
                    y="YAW_VARIANCE",
                    inner=None,          
                    cut=0,               
                    bw_method=0.2,              
                    alpha=0.3,           
                    linewidth=1,
                    color="salmon"     
                )

                plt.title("Yaw Variance (Turning Behavior) by Mode")
                plt.grid(True, linestyle="--", alpha=0.4)
                output_path = os.path.join(output_dir, f"{filename}_{time_mode}_yawvardist.png")
                plt.savefig(output_path)
                plt.close()

                plt.figure(figsize=(10, 5))
                sns.violinplot(
                    data=weekly_attitude_df,
                    x="USER_MODE",
                    y="ROLL_VARIANCE",
                    inner=None,          
                    cut=0,               
                    bw_method=0.2,              
                    alpha=0.3,           
                    linewidth=1,
                    color="salmon"     
                )

                plt.title("Roll Variance by Mode")
                plt.grid(True, linestyle="--", alpha=0.4)
                output_path = os.path.join(output_dir, f"{filename}_{time_mode}_rollvardist.png")
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
        self.user_email_list = [r[0] for r in self.sf_cursor.fetchall()]

        print(self.user_email_list)
        for cur_user_email in self.user_email_list:
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
            
            activity_attitude_weekly_query = f"""
            SELECT
                SPLIT_PART(t1.STREAM_KEY, '_', 1) AS user_email,
                CONVERT_TIMEZONE(
                    'America/Chicago', 
                    SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ
                )::DATE AS session_date,

                t1.user_mode,
                t1.yaw_variance,
                t1.pitch_variance,
                t1.roll_variance

            FROM
                {os.getenv("tableName")} t1
                
            WHERE
                SPLIT_PART(t1.STREAM_KEY, '_', 1) = '{cur_user_email}'
                AND
                CONVERT_TIMEZONE(
                    'America/Chicago', 
                    SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ
                )::DATE 
                    BETWEEN DATEADD(day, -7, '{sql_end_date_str}'::DATE)
                    AND '{sql_end_date_str}'::DATE
                
                AND yaw_variance IS NOT NULL
                AND pitch_variance IS NOT NULL
                AND roll_variance IS NOT NULL

            ORDER BY
                session_date DESC;
            """
            print("--- WEEKLY AGGREGATION RESULT ---")
            #get aggregated data first
            self.sf_cursor.execute(activity_query_weekly)
            weekly_res_df = self.sf_cursor.fetch_pandas_all()
            print(weekly_res_df.head(10))
            #then get variance data for distribution plots
            self.sf_cursor.execute(activity_attitude_weekly_query)
            weekly_attitude_df = self.sf_cursor.fetch_pandas_all()

            self.graph_charts("weekly", weekly_res_df, f"{ cur_user_email}_{cur_date}", weekly_attitude_df)

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

        hard_breaking_count, sharp_turn_count, rapid_accl_count, total_drive_count = self.driving_anomaly_detect(sql_end_date_str, self.user_email_list)
        return self.user_email_list, hard_breaking_count, sharp_turn_count, rapid_accl_count, total_drive_count

    #to be used inside generate_daily_weekly_charts
    def driving_anomaly_detect(self, sql_end_date_str, user_email_list):
        hard_braking_count, sharp_turn_count, rapid_accl_count, total_drive_count = [], [], [], []
        
        for cur_user_email in user_email_list:

            drive_count_qry = f"""
            SELECT COUNT(DISTINCT STREAM_KEY),
            FROM {os.getenv("tableName")} t1
            WHERE
                USER_MODE = 'driving'
                AND 
                STREAM_KEY LIKE '{cur_user_email}%'
                AND
                CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE
                BETWEEN DATEADD(day, -7, '{sql_end_date_str}'::DATE) AND '{sql_end_date_str}'::DATE;
            """
            hard_braking_qry = f"""
            SELECT COUNT(*),
            FROM {os.getenv("tableName")} t1
            WHERE
                ACCELERATION > 0.6 AND AVG_SPEED > 5 
                AND 
                STREAM_KEY LIKE '{cur_user_email}%'
                AND
                CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE
                BETWEEN DATEADD(day, -7, '{sql_end_date_str}'::DATE) AND '{sql_end_date_str}'::DATE;
            """

            sharp_turn_qry = f"""
            SELECT COUNT(*),
            FROM {os.getenv("tableName")} t1
            WHERE
                YAW_VARIANCE > 5.0 AND AVG_SPEED > 3
                AND
                STREAM_KEY LIKE '{cur_user_email}%'
                AND
                CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE
                BETWEEN DATEADD(day, -7, '{sql_end_date_str}'::DATE) AND '{sql_end_date_str}'::DATE;
            """

            rapid_accl_qry = f"""
            SELECT COUNT(*),
            FROM {os.getenv("tableName")} t1
            WHERE
                YAW_VARIANCE > 0.6 AND AVG_SPEED < 3
                AND
                STREAM_KEY LIKE '{cur_user_email}%'
                AND
                CONVERT_TIMEZONE('America/Chicago', SPLIT_PART(t1.STREAM_KEY, '_', 2)::TIMESTAMP_TZ)::DATE
                BETWEEN DATEADD(day, -7, '{sql_end_date_str}'::DATE) AND '{sql_end_date_str}'::DATE;
            """

            total_drive_count.append(self.sf_cursor.execute(drive_count_qry).fetchone()[0])
            hard_braking_count.append(self.sf_cursor.execute(hard_braking_qry).fetchone()[0])
            sharp_turn_count.append(self.sf_cursor.execute(sharp_turn_qry).fetchone()[0])
            rapid_accl_count.append(self.sf_cursor.execute(rapid_accl_qry).fetchone()[0])

        return hard_braking_count, sharp_turn_count, rapid_accl_count, total_drive_count

    def driving_risk_inference(self, sql_end_date_str, user_email_list):
        pass
        

if __name__ == "__main__":
    load_dotenv()
    snoflakeChartObj = SnowflakeCharts()
    user_email_list, hard_braking_count, sharp_turn_count, rapid_accl_count, total_drive_count = snoflakeChartObj.generate_daily_weekly_charts(cur_date="2025-12-08")
    print(user_email_list)
    email_to_user(user_email_list=user_email_list, 
                  hard_braking_count=hard_braking_count, 
                  sharp_turn_count=sharp_turn_count, 
                  rapid_accl_count=rapid_accl_count,
                  total_drive_count=total_drive_count)
