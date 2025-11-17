import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, from_json

import json

def load_json_schema(path):
    with open(path, "r") as f:
        schema_json = json.load(f)
    return StructType.fromJson(schema_json)

#to run pipeline use the spark-submit command below with pacakges specificed for kafak conneciton:
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 kafka_to_snowflake.py

def load_data(spark_app_name, kafka_bootstrap_server, kafka_topic, schema_path="./stream_schema.json"):
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
        .drop("stream_key") #stream_key and kafka_key columns are the same

    )

    agg_df = (
        
    )
    df_extracted.show(truncate=False)
    spark.stop()

def main():
    spark_app_name = "kafka2snowflake"
    kafka_bootstrap_server = "192.168.1.227:9092"
    kafka_topic = "ios_local_stream"

    load_data(spark_app_name=spark_app_name, 
              kafka_bootstrap_server=kafka_bootstrap_server, 
              kafka_topic=kafka_topic)
    
if __name__ == "__main__":
    main()
