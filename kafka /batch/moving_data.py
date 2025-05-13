
from pyspark.sql import SparkSession, dataframe , column
from pyspark.sql.functions import from_json, col , lit, row_number
from pyspark.sql.types import StructType, StringType, IntegerType , FloatType , DoubleType
from pyspark.sql.window import Window

JARS_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

DEFAULT_HOST_HDFS = 'hdfs://localhost:9000'
NEW_HDFS_DATA_FOLDER = f'{DEFAULT_HOST_HDFS}/stockDataLake/matching_trans_v2/'
OLD_HDFS_DATA_FOLDER = f'{DEFAULT_HOST_HDFS}/stockDataLake/matching_trans/'



# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("AddInsertTimeColumn") \
    .getOrCreate()

# Đọc dữ liệu từ folder cũ
df_old = spark.read.parquet(OLD_HDFS_DATA_FOLDER)

# 2. Loại bỏ các bản ghi bị xóa (op = 'd')
df_filtered = df_old.filter(col("op") != 'd')

# 3. Chỉ giữ bản ghi mới nhất theo `id` (giả sử cột `id` có thể trùng)
window_spec = Window.partitionBy("id").orderBy(col("time").desc())  # hoặc "insert_time" nếu phù hợp

df_deduped = df_filtered.withColumn("row_num", row_number().over(window_spec)) \
                        .filter(col("row_num") == 1) \
                        .drop("row_num")

# Thêm cột insert_time (thời điểm ghi dữ liệu)
df_new = df_deduped.withColumn("insert_time", lit(None).cast("timestamp"))

# view data
# df_deduped.createOrReplaceTempView("your_table_view")
#
# result = spark.sql("""
#     SELECT *
#     FROM your_table_view
#     order by id desc
# """)
# result.show()
#
# result2 = spark.sql("""
#     SELECT *
#     FROM your_table_view
#     where op = 'd'
#
#     order by time desc
# """)
# result2.show()
# Ghi dữ liệu mới vào folder mới (ghi đè nếu có)

df_new.write \
    .mode("overwrite") \
    .parquet(NEW_HDFS_DATA_FOLDER)

print("✅ Ghi dữ liệu thành công vào matching_trans_v2")