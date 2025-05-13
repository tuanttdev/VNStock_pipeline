import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType , FloatType , DoubleType
from pyspark.sql.streaming import StreamingQueryListener
from utilities.utilities import wait_for_exit_spark_stream
import subprocess
from urllib.parse import urlparse

# JARS = ",".join([
#     "/home/tuantt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
#     "/home/tuantt/airflow/jars/kafka-clients-3.5.1.jar",
#     "/home/tuantt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
#     "/home/tuantt/airflow/jars/commons-pool2-2.12.0.jar"
# ])

JARS_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
KAFKA_TOPIC_NAME = "stock.public.matching_trans"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

DEFAULT_HOST_HDFS = 'hdfs://localhost:9000'
HDFS_DATA_FOLDER = f'{DEFAULT_HOST_HDFS}/stockDataLake/matching_trans_v2/'
HDFS_CHECKPOINT = f'{DEFAULT_HOST_HDFS}/stockDataLake/matching_trans_checkpoint/'


class KafkaOffsetPrinter(StreamingQueryListener):
    def onQueryProgress(self, event):
        progress = event.progress
        print("=== Batch Info ===")
        print("Batch ID:", progress["batchId"])
        print("Input Rows:", progress["numInputRows"])
        print("Kafka Offsets:")
        print(progress["sources"])

# 1. Tạo Spark Session
spark = SparkSession.builder \
    .appName("KafkaDebeziumToHDFS") \
    .config("spark.jars.packages", JARS_PACKAGES) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# print(spark.sparkContext._jsc.sc().listJars())

# 2. Đọc dữ liệu từ Kafka topic
# chỗ này do spark sẽ đọc từ đầu tới cuối topic nên bị tràn heap(default = 1g) , cần giới hạn lại 1 lần đọc 500 mess thôi
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock.public.matching_trans") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 100000) \
    .load()

# 3. Parse value từ Kafka (Debezium trả về JSON trong field `value`)
df_string = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# 4. Parse JSON
schema = StructType() \
    .add("after", StructType()
        .add("symbol", StringType())
        .add("time", StringType())
        .add("price", FloatType())
        .add("match_type", StringType())
        .add("id", StringType())
        .add("volume", FloatType())
    ) \
    .add("op", StringType())

print(schema)

df_parsed = df_string \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.after.*", "data.op") \
    .withColumn("insert_time" , current_timestamp())

df_parsed.printSchema()

# #
# # df_parsed.writeStream.outputMode("append").format("console").start().awaitTermination()
#
# # 5. Ghi vào HDFS (ở định dạng Parquet)
# # checkpoint được sử dụng để spark stream có thể xử lý frault khi hệ thống crash thì nó sẽ biết cần bắt đầu lại từ đâu để tránh trùng lặp dữ liệu



# Background thread: log status every X seconds

query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", HDFS_DATA_FOLDER) \
    .option("checkpointLocation", HDFS_CHECKPOINT) \
    .outputMode("append") \
    .start()




url = spark.sparkContext.uiWebUrl
parsed = urlparse(url)
port = parsed.port

print(f'Open Spark Session web UI on http://localhost:{port}')
subprocess.run(["explorer.exe", f"http://localhost:{port}"])


def log_status(interval_sec=60):
    while query.isActive:
        print(f"[INFO] Streaming is running... (query id: {query.id})")
        time.sleep(interval_sec)

# Run keyboard listener in background
threading.Thread(target=wait_for_exit_spark_stream(spark_stream=query), daemon=True).start()
threading.Thread(target=log_status, daemon=True).start()

# spark.streams.addListener(KafkaOffsetPrinter())

query.awaitTermination()
