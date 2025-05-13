import threading
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from utilities.utilities import wait_for_exit_spark_stream
from pyspark.sql.functions import from_json, col, current_timestamp , lit , to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DoubleType, TimestampType
import subprocess
from urllib.parse import urlparse

JARS_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


DEFAULT_HOST_HDFS = 'hdfs://localhost:9000'
HDFS_DATA_FOLDER = f'{DEFAULT_HOST_HDFS}/stockDataLake/matching_trans_v2/'
HDFS_CHECKPOINT = f'{DEFAULT_HOST_HDFS}/stockDataLake/clickHouse_matching_trans_checkpoint/'

CLICKHOUSE_JDBC_URL = 'jdbc:clickhouse://localhost:8123/stockWareHouse'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'tuantt'
CLICKHOUSE_TABLE = 'matching_trans'
CLICKHOUSE_DRIVER = 'com.clickhouse.jdbc.ClickHouseDriver'

class KafkaOffsetPrinter(StreamingQueryListener):
    def onQueryProgress(self, event):
        progress = event.progress
        print("=== Batch Info ===")
        print("Batch ID:", progress["batchId"])
        print("Input Rows:", progress["numInputRows"])
        print("Kafka Offsets:")
        print(progress["sources"])

def write_to_clickhouse(batch_df, batch_id):
    print(f"🔥 Batch {batch_id} received {batch_df.count()} records")
    batch_df.show()
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", CLICKHOUSE_JDBC_URL) \
            .option("user", CLICKHOUSE_USER) \
            .option("password", CLICKHOUSE_PASSWORD) \
            .option("dbtable", CLICKHOUSE_TABLE) \
            .option("driver", CLICKHOUSE_DRIVER) \
            .option("isolationLevel", "NONE") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"❌ Error in batch {batch_id}: {e}")


# 1. Tạo Spark Session
spark = SparkSession.builder \
    .appName("HDFS to ClickHouse Streaming") \
    .config("spark.jars.packages", "com.clickhouse:clickhouse-jdbc:0.8.5") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# print(spark.sparkContext._jsc.sc().listJars())
# schema
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("time", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("volume", FloatType(), True),
    StructField("id", StringType(), True)
])

# Read streaming data from HDFS
df = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load(HDFS_DATA_FOLDER)

df.printSchema()
df = df.withColumn("last_modified", current_timestamp())
df = df.withColumn("time" , to_timestamp(col("time")))
# df = df.withColumn("test", lit(11) )

# query = df.writeStream \
#     .format("console") \
#     .outputMode("append" ) \
#     .start()
#
# # Start the streaming query
query = df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", HDFS_CHECKPOINT) \
    .start()


url = spark.sparkContext.uiWebUrl
parsed = urlparse(url)
port = parsed.port

print(f'Open Spark Session web UI on http://localhost:{port}')
subprocess.run(["explorer.exe", f"http://localhost:{port}"])


# Run keyboard listener in background
threading.Thread(target=wait_for_exit_spark_stream(spark_stream=query), daemon=True).start()


query.awaitTermination()
