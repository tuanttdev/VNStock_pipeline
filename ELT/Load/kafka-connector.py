import requests
import json
import subprocess
from utilities.utilities import get_windows_host_ip

host_ip = get_windows_host_ip()

connector_config = {
    "name": "stock-postgres-connector3",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": host_ip,
        "database.port": "5432",
        "database.user": "debezium",
        "database.password": "tuantt",
        "database.dbname": "BackendStock",
        "database.server.name": "PostgreSQL 13",
        "table.include.list": "public.matching_trans,public.ohlcvt_history,public.price_board",
        "slot.name": "debezium_slot",
        "publication.name": "debezium_pub",
        "topic.prefix": "stock",
        "database.history.kafka.bootstrap.servers": "localhost:9092",
        "database.history.kafka.topic": "schema-changes.stock"
    }
}

response = requests.post(
    "http://localhost:8083/connectors",
    headers={"Content-Type": "application/json"},
    data=json.dumps(connector_config)
)

if response.status_code == 201:
    print("✅ Connector created successfully!")
elif response.status_code == 409:
    print("⚠️ Connector already exists.")
else:
    print(f"❌ Error creating connector: {response.status_code}\n{response.text}")
