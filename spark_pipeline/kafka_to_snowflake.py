import pyspark
from pyspark.sql import SparkSession

#to run pipeline use the spark-submit command below with pacakges specificed for kafak conneciton:
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 kafka_to_snowflake.py

def load_data(spark_app_name, kafka_bootstrap_server, kafka_topic):
    spark = SparkSession.builder.appName(spark_app_name).getOrCreate()

    df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

    df.show()
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
