from pyspark.sql import SparkSession
from utilities.utilities import get_windows_host_ip



DEFAULT_HOST_HDFS = 'hdfs://localhost:9000'
# Khởi tạo SparkSession
# spark = SparkSession.builder \
#     .appName("Read Text from HDFS") \
#     .getOrCreate()
# # print(spark.)
# # Đọc file text từ HDFS
# text_df = spark.read.text(f"{DEFAULT_HOST_HDFS}/test.txt")
# text_df.show(truncate=False)

# 1. Tạo Spark session
spark = SparkSession.builder \
    .appName("PostgreSQL to HDFS") \
    .config("spark.jars", "/home/tuantt/airflow/jars/postgresql-42.2.27.jar") \
    .getOrCreate()

# print(spark)


# 2. Đọc dữ liệu từ PostgreSQL

POSTGRES_HOST = get_windows_host_ip()

df = spark.read.format("jdbc").options(
    url=f"jdbc:postgresql://{POSTGRES_HOST}:5432/BackendStock",
    driver="org.postgresql.Driver",
    dbtable="(SELECT * FROM company WHERE exchange = 'HSX') AS temp",
    user="postgres",
    password="tuantt"
).load()


# 3. Ghi ra HDFS dưới dạng Parquet
df.write.mode("append").parquet(f"{DEFAULT_HOST_HDFS}/data/your_table_parquet")
# Nếu muốn ghi ra CSV:
df.write.mode("append").csv(f"{DEFAULT_HOST_HDFS}/data/your_table_csv", header=True)

# Đọc file Parquet từ HDFS
# your_table_parquet = spark.read.parquet(f"{DEFAULT_HOST_HDFS}/data/your_table_parquet")
# your_table_csv = spark.read.option("header" , True).csv(f"{DEFAULT_HOST_HDFS}/data/your_table_csv")
#
# # create view
#
# your_table_parquet.createOrReplaceTempView("your_table_view")
#
# your_table_csv.createOrReplaceTempView("your_table_csv_view")
#
# result = spark.sql("""
#     SELECT symbol, exchange
#     FROM your_table_view
#     WHERE group_symbol IS NULL
# """)
# result.show()
#
#
# result = spark.sql("""
#     SELECT symbol, exchange
#     FROM your_table_csv_view
#     WHERE group_symbol IS NOT NULL
# """)
# result.show()

# Hiển thị 5 dòng đầu tiên
# your_table_parquet.show()
# your_table_csv.show()
#
spark.stop()