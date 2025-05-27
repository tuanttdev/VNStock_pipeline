#!/bin/bash
# File này được build trên nền là wsl2 + ubuntu
# Lệnh explorer.exe là của window mở windows explorer chứ không phải lệnh gốc của unbuntu
# Đường dẫn virtualenv
echo "Activate venv ..."
AIRFLOW_VENV="$HOME/stockpipeline3.10/.venv"
source "$AIRFLOW_VENV/bin/activate"
export AIRFLOW_CONFIG=$HOME/VNStock_pipeline/airflow.cfg

echo "Starting Airflow webserver..."
airflow webserver --port 8080 > ./log_services/airflow-webserver.log 2>&1 &

echo "Starting Airflow scheduler..."
airflow scheduler > ./log_services/airflow-scheduler.log 2>&1 &

while ! nc -z localhost 8080; do
  echo "Waiting for Airflow Webserver..."
  sleep 2
done

# Mở Airflow Web UI trên trình duyệt
echo "Opening Airflow Web UI..."
explorer.exe http://localhost:8080

deactivate

echo "$HOME/kafka/bin/zookeeper-server-start.sh"

# Start HDFS
echo "Starting HDFS..."
start-dfs.sh

while ! nc -z localhost 9870; do
  echo "Waiting for Hadoop..."
  sleep 2
done

# Mở Airflow Web UI trên trình duyệt
echo "Opening Hadoop Web UI..."
 explorer.exe http://localhost:9870


# Start ZooKeeper in background
echo "Starting ZooKeeper..."
  $HOME/kafka/bin/zookeeper-server-start.sh $HOME/kafka/config/zookeeper.properties > ./log_services/zookeeper.log 2>&1 &

# Wait a few seconds for ZooKeeper to initialize

while ! nc -z localhost 2181; do
  echo "Waiting for Zookeeper..."
  sleep 2
done


# Start Kafka in background
echo "Starting Kafka..."
$HOME/kafka/bin/kafka-server-start.sh $HOME/kafka/config/server.properties > ./log_services/kafka.log 2>&1 &

# Wait a few seconds for Kafka to initialize
while ! nc -z localhost 9092; do
  echo "Waiting for Kafka..."
  sleep 2
done

# Start kafka UI
SERVER_PORT=8081 \
KAFKA_CLUSTERS_0_NAME=local \
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS=localhost:9092 \
java -jar $HOME/kafka-ui/kafka-ui-api-v0.7.1.jar > ./log_services/kafka-ui.log 2>&1 &

while ! nc -z localhost 8081; do
  echo "Waiting for Kafka Web UI..."
  sleep 2
done

echo "Opening Kafka Web UI..."
 explorer.exe http://localhost:8081

# start kafka connector for debezium capture data change in postgre

$HOME/kafka/bin/connect-distributed.sh $HOME/kafka/config/connect-distributed-custom.properties > ./log_services/kafka-connector.log 2>&1 &
while ! nc -z localhost 8083; do
  echo "Waiting for Kafka Web UI..."
  sleep 2
done

echo "Opening Kafka Connector..."
 explorer.exe http://localhost:8083


echo "All services started."
echo "Logs: zookeeper.log, kafka.log"

# Optional: keep script running to tail logs (optional)
# tail -f zookeeper.log kafka.log