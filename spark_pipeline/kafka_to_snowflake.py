from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, from_json, window, avg, stddev, min, max, count, last, last_value, mode
from dotenv import load_dotenv
import snowflake.connector
import os
import json
import re
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent #need to get base dir for the sf_rsa_key file, since airflow can't find the relative path

def load_json_schema(path):
    with open(path, "r") as f:
        schema_json = json.load(f)
    return StructType.fromJson(schema_json)

def get_streamkeys_from_snowflake(sf_private_key):
    try: 
        sf_conn = snowflake.connector.connect(
            account=os.getenv("sfURL").split('.')[0],
            user=os.getenv("sfUser"),
            database=os.getenv("sfDatabase"),
            schema=os.getenv("sfSchema"),
            warehouse=os.getenv("sfWarehouse"),
            private_key=sf_private_key
        )
    except Exception as e:
        print(f"Snowflake connection failed in get_streamkeys_from_snowflake: {e}. Exiting")
        return

    cur = sf_conn.cursor()

    try:
        cur.execute("SELECT DISTINCT STREAM_KEY FROM COYOTE_DB.IOS_STREAM_SCHEMA.IOS_STREAM ORDER BY STREAM_KEY;")
        all_rows = cur.fetchall()
        print(f"unique streamkeys: {all_rows}")
        return all_rows
    except Exception as e:
        print(f"error executing query in get_streamkeys_from_snowflake returning None: {e}")
        return None
    finally:
        cur.close()
        sf_conn.close()
    
#snowflake spark connector only supports spark 3.5.x not pyspark 4
#to run pipeline use the spark-submit command below with pacakges specificed for kafak conneciton:
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,net.snowflake:spark-snowflake_2.13:3.0.0 ./spark_pipeline/kafka_to_snowflake.py

def load_data_eventhub(spark_app_name, kafka_topic, schema_path="./spark_pipeline/stream_schema.json"):
    full_rsakey_path = BASE_DIR / os.getenv("rsakey_path") #full path, needed for airflow to find sf pem file
    with open(full_rsakey_path, "r") as f:
        private_key_str = f.read()
    private_key_body = re.sub("-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n", "", private_key_str)
    
    existing_stream_keys = get_streamkeys_from_snowflake(sf_private_key=private_key_body)
    existing_stream_keys = [tup[0] for tup in existing_stream_keys]
    # # print(f"SF Private Key: {private_key_body}")

    snowflake_options = {
    "sfURL": os.getenv("sfURL"),
    "sfUser": os.getenv("sfUser"),
    "sfDatabase": os.getenv("sfDatabase"),
    "sfSchema": os.getenv("sfSchema"),
    "sfWarehouse": os.getenv("sfWarehouse"),
    "pem_private_key": private_key_body
    }
    spark = SparkSession.builder.appName(spark_app_name).getOrCreate()

    #start timer
    start = time.perf_counter()

    stream_schema = load_json_schema(schema_path)

    #eventhub conneciton variables
    event_hub_bootstrap_server = os.getenv("event-hub-bootstrap-server")
    event_hub_conn_str = os.getenv("event-hub-primary-conn-str")
    

    #spark read config for eventhub
    df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", event_hub_bootstrap_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";') \
    .load()

    df_extracted = (
        df_raw
        .select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), stream_schema).alias("json"), #value field contains the whole json string
            col("timestamp").alias("kafka_timestamp"),
            col("offset")
        )
        .select("json.*", "kafka_key", "kafka_timestamp", "offset") #select all field from the json field which contains our streamed data
    )

    #de-duplicate dataframe by removing rows with existing stream_key values
    df_extracted = df_extracted.filter(~col("stream_key").isin(existing_stream_keys))
    df_extracted.show()
    # avg(speed)
    # max(speed)
    # variance(speed)
    # count of speed spikes
    # acceleration (Δspeed / Δtime)
    #last long/lat of window

    # yaw variance → steering
    # pitch variance → acceleration/braking
    # roll variance → phone tilt (distraction level)

    agg_df = (
        df_extracted
        .withColumn("timestamp_ts", col("cur_time").cast("timestamp"))
        .groupBy(window("timestamp_ts", "10 seconds"), "stream_key")
        .agg(
            avg("speed").alias("avg_speed"),
            max("speed").alias("max_speed"),
            min("speed").alias("min_speed"),
            stddev("speed").alias("speed_variance"),
            stddev("yaw").alias("yaw_variance"),
            stddev("pitch").alias("pitch_variance"),
            stddev("roll").alias("roll_variance"),
            last("lon").alias("last_longitude"),
            last("lat").alias("last_latitude"),
            count("*").alias("sample_count")
        )
        .withColumn("acceleration", ((col("max_speed") - col("min_speed")) / 10.0))
    ).join(df_extracted.select(
        col("stream_key"),
        col("user_mode")
    ),
    on="stream_key",
    how="inner")

    final_df = (
        agg_df.select(
        "stream_key",
        "user_mode",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_speed",
        "max_speed",
        "min_speed",
        "acceleration",
        "speed_variance",
        "yaw_variance",
        "pitch_variance",
        "roll_variance",
        "last_longitude",
        "last_latitude",
        "sample_count"
    )
    )
    final_df.show(truncate=False)

    final_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", os.getenv("tableName")) \
    .mode("append") \
    .save()

    #end timer
    end = time.perf_counter()
    spark.stop() 
    print(f"Pipeline time: {end - start:.2f}s")




def load_data_kafka(spark_app_name, kafka_bootstrap_server, kafka_topic, schema_path="./stream_schema.json"):
    
    with open(os.getenv("rsakey_path"), "r") as f:
        private_key_str = f.read()
    private_key_body = re.sub("-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n", "", private_key_str)
    
    existing_stream_keys = get_streamkeys_from_snowflake(sf_private_key=private_key_body)
    existing_stream_keys = [tup[0] for tup in existing_stream_keys]
    # print(f"SF Private Key: {private_key_body}")

    snowflake_options = {
    "sfURL": os.getenv("sfURL"),
    "sfUser": os.getenv("sfUser"),
    "sfDatabase": os.getenv("sfDatabase"),
    "sfSchema": os.getenv("sfSchema"),
    "sfWarehouse": os.getenv("sfWarehouse"),
    "pem_private_key": private_key_body
    }


    spark = SparkSession.builder.appName(spark_app_name).getOrCreate()

    stream_schema = load_json_schema(schema_path)

    df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

    df_extracted = (
        df_raw
        .select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), stream_schema).alias("json"), #value field contains the whole json string
            col("timestamp").alias("kafka_timestamp"),
            col("offset")
        )
        .select("json.*", "kafka_key", "kafka_timestamp", "offset") #select all field from the json field which contains our streamed data
    )

    #de-duplicate dataframe by removing rows with existing stream_key values
    df_extracted = df_extracted.filter(~col("stream_key").isin(existing_stream_keys))
    df_extracted.show()
    # avg(speed)
    # max(speed)
    # variance(speed)
    # count of speed spikes
    # acceleration (Δspeed / Δtime)
    #last long/lat of window


    # yaw variance → steering
    # pitch variance → acceleration/braking
    # roll variance → phone tilt (distraction level)

    agg_df = (
        df_extracted
        .withColumn("timestamp_ts", col("cur_time").cast("timestamp"))
        .groupBy(window("timestamp_ts", "10 seconds"), "stream_key")
        .agg(
            avg("speed").alias("avg_speed"),
            max("speed").alias("max_speed"),
            min("speed").alias("min_speed"),
            stddev("speed").alias("speed_variance"),
            stddev("yaw").alias("yaw_variance"),
            stddev("pitch").alias("pitch_variance"),
            stddev("roll").alias("roll_variance"),
            last("lon").alias("last_longitude"),
            last("lat").alias("last_latitude"),
            count("*").alias("sample_count")
        )
        .withColumn("acceleration", ((col("max_speed") - col("min_speed")) / 10.0))
    ).join(df_extracted.select(
        col("stream_key"),
        col("user_mode")
    ).dropDuplicates(["stream_key"]),
    on="stream_key",
    how="inner")

    final_df = (
        agg_df.select(
        "stream_key",
        "user_mode",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_speed",
        "max_speed",
        "min_speed",
        "acceleration",
        "speed_variance",
        "yaw_variance",
        "pitch_variance",
        "roll_variance",
        "last_longitude",
        "last_latitude",
        "sample_count"
    )
    )

    final_df.show(truncate=False)

    final_df.write \
    .format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", os.getenv("tableName")) \
    .mode("append") \
    .save()

    spark.stop()    

def main():
    load_dotenv()
    spark_app_name = "kafka2snowflake"

    ###local kafka###
    # kafka_bootstrap_server = "{ip_add}:9092"
    # # kafka_bootstrap_server = "{ip_add}:9092"
    # kafka_topic = "ios_local_stream"

    # load_data_kafka(spark_app_name=spark_app_name, 
    #           kafka_bootstrap_server=kafka_bootstrap_server, 
    #           kafka_topic=kafka_topic)


    ###eventhub based kafak###
    load_data_eventhub(spark_app_name=spark_app_name, 
              kafka_topic=os.getenv("event-hub-topic"), 
              schema_path=os.path.abspath("./spark_pipeline/stream_schema.json"))
    
if __name__ == "__main__":
    main()
