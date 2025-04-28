from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from pyspark.streaming import StreamingContext

DEFAULT_HOST_HDFS = 'hdfs://localhost:9000'
JARS = ",".join([
    "/home/tuantt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "/home/tuantt/airflow/jars/kafka-clients-3.5.1.jar",
    "/home/tuantt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "/home/tuantt/airflow/jars/commons-pool2-2.12.0.jar"
])

print(JARS)
JARS_spark_sql_kafka =  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"

# 1. Tạo Spark Session
spark = SparkSession.builder \
    .appName("KafkaDebeziumToHDFS") \
    .config("spark.jars", JARS) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(spark.sparkContext._jsc.sc().listJars())

# 2. Đọc dữ liệu từ Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock.public.company") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Parse value từ Kafka (Debezium trả về JSON trong field `value`)
df_string = df_raw.selectExpr("CAST(value AS STRING) as json_str")

# 4. Parse JSON
schema = StructType().add("payload", StructType()
    .add("after", StructType()
        .add("symbol", StringType())
        .add("organ_name", StringType())
        # add thêm các trường khác tùy bảng
    ).add("op", StringType())
)

df_parsed = df_string \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.payload.after.*", "data.payload.op")

df_parsed.printSchema()
#
# df_parsed.writeStream.outputMode("append").format("console").start().awaitTermination()

# 5. Ghi vào HDFS (ở định dạng Parquet)
df_parsed.writeStream \
    .format("parquet") \
    .option("path", f"{DEFAULT_HOST_HDFS}/data/stock_company/") \
    .option("checkpointLocation", f"{DEFAULT_HOST_HDFS}/data/stock_company_checkpoint/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()


# query = df_parsed \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()
#
#
# # Đợi cho đến khi dòng streaming kết thúc
# query.awaitTermination()