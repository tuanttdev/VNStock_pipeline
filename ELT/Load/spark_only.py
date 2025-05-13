from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType


JARS = ",".join([
    "/home/tuantt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar",
    "/home/tuantt/airflow/jars/kafka-clients-3.5.1.jar",
    "/home/tuantt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "/home/tuantt/airflow/jars/commons-pool2-2.12.0.jar"
])
JARS_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

spark = (SparkSession.builder.appName("Check Spark Mode")
            .config("spark.jars.packages", JARS_packages)
            .getOrCreate())

# Kiểm tra Spark master
print(spark.sparkContext.master)

if not spark.sparkContext._jsc.sc().isStopped():
    print("SparkSession is running.")
else:
    print("SparkSession is stopped.")

# 2. Đọc dữ liệu từ Kafka topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock.public.matching_trans") \
    .option("startingOffsets", "earliest") \
    .load()
print(df_raw)


# 3. Parse value từ Kafka (Debezium trả về JSON trong field `value`)
df_string = df_raw.selectExpr("CAST(value AS STRING) as json_str")
print(df_string)

# 4. Parse JSON
schema = StructType().add("payload", StructType()
    .add("after", StructType()
        .add("symbol", StringType())
        .add("price", StringType())
        # add thêm các trường khác tùy bảng
    ).add("op", StringType())
)

print(schema)


df_parsed = df_string \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.payload.after.*", "data.payload.op")

print(df_parsed)

query = df_parsed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()



if not spark.sparkContext._jsc.sc().isStopped():
    print("SparkSession is running.")
else:
    print("SparkSession is stopped.")

query.awaitTermination()
