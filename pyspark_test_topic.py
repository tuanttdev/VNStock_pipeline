from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import time

JARS = ",".join([
    "/home/tuantt/airflow/jars/kafka-clients-3.5.1.jar",
    "/home/tuantt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "/home/tuantt/airflow/jars/commons-pool2-2.12.0.jar",
    "/home/tuantt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "/home/tuantt/airflow/jars/spark-streaming-kafka-0-10_2.12-3.5.1.jar",
    "/home/tuantt/airflow/jars/kafka_2.13-3.5.1.jar",
    "/home/tuantt/airflow/jars/spark-streaming_2.12-3.5.1.jar"
])


# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaBatchConsumer") \
    .config("spark.jars", JARS) \
    .getOrCreate()

# Cấu hình Kafka để tiêu thụ tin nhắn từ topic 'test-topic'
kafka_streaming_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9093") \
    .option("subscribe", "test-topic") \
    .load()
kafka_streaming_df.printSchema()

# Chuyển giá trị 'value' từ Kafka (kiểu binary) thành kiểu STRING
message_batch_df = kafka_streaming_df.selectExpr("CAST(value AS STRING)")

# Hiển thị dữ liệu (batch processing)
message_batch_df.show(truncate=False)

# # Chuyển dữ liệu thành output
# query = message_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
#
#
# # Chạy Spark Streaming
# query.awaitTermination()