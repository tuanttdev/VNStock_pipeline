from pyspark.sql import SparkSession
from utilities.utilities import get_windows_host_ip


# 1. Tạo Spark session
spark = SparkSession.builder \
    .appName("PostgreSQL to HDFS") \
    .config("spark.jars", "/home/tuantt/airflow/jars/postgresql-42.2.27.jar") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

print(spark)


# 2. Đọc dữ liệu từ PostgreSQL

POSTGRES_HOST = get_windows_host_ip()
DEFAULT_HOST_HDFS = 'hdfs://localhost:9000'


# Đọc file Parquet từ HDFS
# your_table_parquet = spark.read.parquet(f"{DEFAULT_HOST_HDFS}/stockDataLake/matching_trans").where("time >= '2025-05-08' and time < '2025-05-09' and symbol = 'SAB' ").orderBy("time" , ascending=False)
your_table_parquet = spark.read.parquet(f"{DEFAULT_HOST_HDFS}/stockDataLake/matching_trans_v2") \
    # .where("time >= '2025-05-13' AND time < '2025-05-14' and insert_time is not null") \
    # .orderBy("time", ascending=False)


# your_table_csv = spark.read.option("header" , True).csv(f"{DEFAULT_HOST_HDFS}/data/your_table_csv")

# create view

your_table_parquet.createOrReplaceTempView("your_table_view")

# your_table_csv.createOrReplaceTempView("your_table_csv_view")

# your_table_parquet.show(20, truncate=False)
#
result3 = spark.sql("""
    SELECT *
    FROM your_table_view
    order by time desc

""")
result3.show(truncate=False)
#
#
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

# spark.stop()