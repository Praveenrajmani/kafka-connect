#!/usr/bin/env python3
"""
Kafka Connect Iceberg Sink Initialization Script

This script:
1. Creates the Kafka control topic (required for Iceberg commit coordination)
2. Creates the warehouse and namespace in MinIO AIStor Tables using PyIceberg
3. Deploys the Iceberg Sink connector

Uses PyIceberg REST catalog for AIStor Tables API.
"""

import hashlib
import json
import os
import sys
import time

import boto3
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# =============================================================================
# Configuration from environment
# =============================================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")
WAREHOUSE_NAME = os.getenv("WAREHOUSE_NAME", "kafkawarehouse")
NAMESPACE_NAME = os.getenv("NAMESPACE_NAME", "streaming")
TABLE_NAME = os.getenv("TABLE_NAME", "events")


def print_step(step: int, msg: str):
    """Print a formatted step message."""
    print(f"\n{'='*60}")
    print(f"  Step {step}: {msg}")
    print(f"{'='*60}\n")


def print_success(msg: str):
    """Print a success message."""
    print(f"✓ {msg}")


def print_error(msg: str):
    """Print an error message."""
    print(f"✗ {msg}")


def print_info(msg: str):
    """Print an info message."""
    print(f"ℹ {msg}")


def sigv4_sign(method: str, url: str, body: str, headers: dict) -> dict:
    """Sign a request using AWS SigV4."""
    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    headers["x-amz-content-sha256"] = payload_hash
    headers["Host"] = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")

    request = AWSRequest(
        method=method,
        url=url,
        data=body,
        headers=headers,
    )
    SigV4Auth(session.get_credentials(), "s3tables", "us-east-1").add_auth(request)
    return dict(request.headers)


# =============================================================================
# Step 1: Create Kafka Control Topic
# =============================================================================
def create_control_topic():
    """Create the Iceberg control topic with exactly 1 partition."""
    print_step(1, "Creating Kafka Control Topic")

    control_topic = "control-iceberg"

    # Wait for Kafka to be ready
    print_info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    max_retries = 30
    admin_client = None

    for i in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="iceberg-init",
            )
            break
        except Exception as e:
            if i < max_retries - 1:
                print_info(f"Waiting for Kafka... ({i+1}/{max_retries})")
                time.sleep(2)
            else:
                print_error(f"Failed to connect to Kafka: {e}")
                return False

    # Create control topic
    try:
        topic = NewTopic(
            name=control_topic,
            num_partitions=1,  # MUST be exactly 1 for coordinator election
            replication_factor=1,
        )
        admin_client.create_topics([topic])
        print_success(f"Created control topic: {control_topic} (1 partition)")
    except TopicAlreadyExistsError:
        print_info(f"Control topic already exists: {control_topic}")
    except Exception as e:
        print_error(f"Failed to create control topic: {e}")
        return False
    finally:
        if admin_client:
            admin_client.close()

    # Also create the data topic
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="iceberg-init",
        )
        data_topic = NewTopic(
            name="events",
            num_partitions=3,
            replication_factor=1,
        )
        admin_client.create_topics([data_topic])
        print_success("Created data topic: events (3 partitions)")
    except TopicAlreadyExistsError:
        print_info("Data topic already exists: events")
    except Exception as e:
        print_error(f"Failed to create data topic: {e}")
    finally:
        if admin_client:
            admin_client.close()

    return True


# =============================================================================
# Step 2: Create Warehouse and Namespace using REST API with SigV4
# =============================================================================
def setup_aistor_tables():
    """Create warehouse and namespace in MinIO AIStor Tables using REST API."""
    print_step(2, "Setting Up MinIO AIStor Tables (REST API with SigV4)")

    # Wait for MinIO to be ready
    print_info(f"Connecting to MinIO at {MINIO_ENDPOINT}...")
    max_retries = 30

    for i in range(max_retries):
        try:
            resp = requests.get(f"{MINIO_ENDPOINT}/minio/health/live", timeout=5)
            if resp.status_code == 200:
                print_success("MinIO is ready")
                break
        except Exception:
            pass

        if i < max_retries - 1:
            print_info(f"Waiting for MinIO... ({i+1}/{max_retries})")
            time.sleep(2)
        else:
            print_error("MinIO is not responding")
            return False

    # Create warehouse using REST API with SigV4 signing
    print_info(f"Creating warehouse: {WAREHOUSE_NAME}")
    warehouse_url = f"{MINIO_ENDPOINT}/_iceberg/v1/warehouses"
    payload = json.dumps({"name": WAREHOUSE_NAME})
    headers_to_sign = {
        "content-type": "application/json",
        "content-length": str(len(payload)),
    }

    try:
        signed_headers = sigv4_sign("POST", warehouse_url, payload, headers_to_sign)
        resp = requests.post(warehouse_url, data=payload, headers=signed_headers, timeout=30)

        if resp.status_code == 200:
            print_success(f"Created warehouse: {WAREHOUSE_NAME}")
        elif resp.status_code == 409:
            print_info(f"Warehouse already exists: {WAREHOUSE_NAME}")
        else:
            print_error(f"Failed to create warehouse: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        print_error(f"Error creating warehouse: {e}")
        return False

    # Create namespace using REST API
    print_info(f"Creating namespace: {NAMESPACE_NAME}")
    namespace_url = f"{MINIO_ENDPOINT}/_iceberg/v1/{WAREHOUSE_NAME}/namespaces"
    payload = json.dumps({"namespace": [NAMESPACE_NAME]})
    headers_to_sign = {
        "content-type": "application/json",
        "content-length": str(len(payload)),
    }

    try:
        signed_headers = sigv4_sign("POST", namespace_url, payload, headers_to_sign)
        resp = requests.post(namespace_url, data=payload, headers=signed_headers, timeout=30)

        if resp.status_code == 200:
            print_success(f"Created namespace: {NAMESPACE_NAME}")
        elif resp.status_code == 409:
            print_info(f"Namespace already exists: {NAMESPACE_NAME}")
        else:
            print_info(f"Namespace creation response: {resp.status_code} - {resp.text}")
    except Exception as e:
        print_info(f"Note: {e}")

    return True


# =============================================================================
# Step 3: Deploy Iceberg Sink Connector
# =============================================================================
def deploy_connector():
    """Deploy the Iceberg Sink connector to Kafka Connect."""
    print_step(3, "Deploying Iceberg Sink Connector")

    connector_name = "iceberg-sink"

    # Wait for Kafka Connect to be ready
    print_info(f"Connecting to Kafka Connect at {KAFKA_CONNECT_URL}...")
    max_retries = 60

    for i in range(max_retries):
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
            if resp.status_code == 200:
                print_success("Kafka Connect is ready")
                break
        except Exception:
            pass

        if i < max_retries - 1:
            print_info(f"Waiting for Kafka Connect... ({i+1}/{max_retries})")
            time.sleep(2)
        else:
            print_error("Kafka Connect is not responding")
            return False

    # Check if connector already exists
    resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
    if resp.status_code == 200:
        print_info(f"Connector already exists: {connector_name}")
        print_info("Deleting existing connector for fresh deployment...")
        requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
        time.sleep(2)

    # Connector configuration
    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
            "tasks.max": "2",
            "topics": "events",
            # Table settings
            "iceberg.tables": f"{NAMESPACE_NAME}.{TABLE_NAME}",
            "iceberg.tables.auto-create-enabled": "true",
            "iceberg.tables.evolve-schema-enabled": "true",
            # Catalog settings (REST catalog pointing to MinIO AIStor)
            "iceberg.catalog.type": "rest",
            "iceberg.catalog.uri": "http://minio:9000/_iceberg",
            "iceberg.catalog.warehouse": WAREHOUSE_NAME,
            # SigV4 authentication
            "iceberg.catalog.rest.sigv4-enabled": "true",
            "iceberg.catalog.rest.signing-name": "s3tables",
            "iceberg.catalog.rest.signing-region": "us-east-1",
            # S3 settings for data access
            "iceberg.catalog.s3.access-key-id": ACCESS_KEY,
            "iceberg.catalog.s3.secret-access-key": SECRET_KEY,
            "iceberg.catalog.s3.endpoint": "http://minio:9000",
            "iceberg.catalog.s3.path-style-access": "true",
            # Control topic for commit coordination
            "iceberg.control.topic": "control-iceberg",
            "iceberg.control.commit.interval-ms": "10000",
            "iceberg.control.commit.timeout-ms": "60000",
            # Converters
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
        },
    }

    # Deploy connector
    print_info(f"Deploying connector: {connector_name}")
    resp = requests.post(
        f"{KAFKA_CONNECT_URL}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config),
        timeout=30,
    )

    if resp.status_code == 201:
        print_success(f"Deployed connector: {connector_name}")
    else:
        print_error(f"Failed to deploy connector: {resp.status_code} - {resp.text}")
        return False

    # Wait for connector to be running
    print_info("Waiting for connector to start...")
    time.sleep(5)

    resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status")
    if resp.status_code == 200:
        status = resp.json()
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        task_states = [t.get("state", "UNKNOWN") for t in status.get("tasks", [])]

        print_info(f"Connector state: {connector_state}")
        print_info(f"Task states: {task_states}")

        if connector_state == "RUNNING":
            print_success("Connector is running!")
        else:
            print_error(f"Connector is in unexpected state: {connector_state}")

    return True


# =============================================================================
# Step 4: Print Summary and Test Commands
# =============================================================================
def print_summary():
    """Print summary and helpful commands."""
    print_step(4, "Setup Complete!")

    print("""
┌─────────────────────────────────────────────────────────────────────────┐
│                    KAFKA CONNECT ICEBERG SINK READY                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Services:                                                               │
│    • Kafka:         localhost:9092                                       │
│    • Kafka Connect: http://localhost:8083                                │
│    • MinIO Console: http://localhost:9001 (minioadmin/minioadmin)        │
│    • MinIO API:     http://localhost:9000                                │
│                                                                          │
│  Configuration:                                                          │
│    • Warehouse:     kafkawarehouse                                       │
│    • Namespace:     streaming                                            │
│    • Table:         events                                               │
│    • Data Topic:    events                                               │
│    • Control Topic: control-iceberg                                      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

Test Commands:
─────────────────────────────────────────────────────────────────────────

1. Produce test messages:

   echo '{"event_id": "evt-001", "event_type": "click", "user_id": 123, "timestamp": "2024-12-24T10:00:00Z"}' | \\
     docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic events

2. Check connector status:

   curl -s http://localhost:8083/connectors/iceberg-sink/status | jq

3. List Kafka topics:

   docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

4. View MinIO Console:

   Open http://localhost:9001 in your browser
   Navigate to: kafkawarehouse/streaming/events/

5. Query table with PyIceberg (requires AWS creds in env):

   ./kafka-connect-iceberg-sink.sh query

""")


# =============================================================================
# Main
# =============================================================================
def main():
    print("\n" + "=" * 60)
    print("  Kafka Connect Iceberg Sink Initialization")
    print("=" * 60)

    # Run setup steps
    if not create_control_topic():
        print_error("Failed at Step 1: Create control topic")
        sys.exit(1)

    if not setup_aistor_tables():
        print_error("Failed at Step 2: Setup AIStor Tables")
        sys.exit(1)

    if not deploy_connector():
        print_error("Failed at Step 3: Deploy connector")
        sys.exit(1)

    print_summary()
    print_success("All setup steps completed successfully!")


if __name__ == "__main__":
    main()
