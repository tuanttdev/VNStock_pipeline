echo "========== Stopping Airflow =========="
pkill -f "airflow"

echo "========== Stopping Kafka UI =========="
sudo lsof -i :8081
<pid>
kill <pid>

echo "========== Stopping Kafka =========="
$HOME/kafka/bin/kafka-server-stop.sh


echo "========== Stopping Zookeeper =========="
$HOME/kafka/bin/zookeeper-server-stop.sh

echo "========== Stopping HDFS (NameNode & DataNode) =========="
stop-dfs.sh

echo "========== Stopping Spark =========="
pkill -f "org.apache.spark.deploy"
pkill -f "org.apache.spark.sql"
pkill -f "spark-submit"


echo "========== Done. =========="