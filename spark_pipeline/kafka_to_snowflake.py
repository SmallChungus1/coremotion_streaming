import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, from_json, window, avg, stddev, min, max, count
from dotenv import load_dotenv
import os
import json
import re


def load_json_schema(path):
    with open(path, "r") as f:
        schema_json = json.load(f)
    return StructType.fromJson(schema_json)

#snowflake spark connector only supports spark 3.5.x not pyspark 4
#to run pipeline use the spark-submit command below with pacakges specificed for kafak conneciton:
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,net.snowflake:spark-snowflake_2.13:3.0.0 kafka_to_snowflake.py


def load_data(spark_app_name, kafka_bootstrap_server, kafka_topic, schema_path="./stream_schema.json"):
    
    with open(os.getenv("rsakey_path"), "r") as f:
        private_key_str = f.read()
    private_key_body = re.sub("-----BEGIN PRIVATE KEY-----|-----END PRIVATE KEY-----|\n", "", private_key_str)
    
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

    df_extracted.show()
    # avg(speed)
    # max(speed)
    # variance(speed)
    # count of speed spikes
    # acceleration (Δspeed / Δtime)

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
    # kafka_bootstrap_server = "192.168.1.227:9092"
    kafka_bootstrap_server = "10.232.138.60:9092"
    kafka_topic = "ios_local_stream"

    load_data(spark_app_name=spark_app_name, 
              kafka_bootstrap_server=kafka_bootstrap_server, 
              kafka_topic=kafka_topic)
    
if __name__ == "__main__":
    main()
