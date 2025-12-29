#!/usr/bin/env python3
"""
Kafka Connect Iceberg Sink Initialization Script - Dual Cluster Setup

This script:
1. Creates the Kafka control topic (required for Iceberg commit coordination)
2. Creates a test bucket on the source MinIO cluster (to generate logs)
3. Creates the warehouse and namespace in Destination MinIO AIStor Tables
4. Deploys the Iceberg Sink connector pointing to destination cluster

Architecture:
  Source MinIO (4-node) -> Kafka -> Kafka Connect -> Iceberg -> Destination MinIO (4-node)
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
from botocore.client import Config
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# =============================================================================
# Configuration from environment
# =============================================================================
# Source MinIO cluster (generates logs)
MINIO_SOURCE_ENDPOINT = os.getenv("MINIO_SOURCE_ENDPOINT", "http://nginx-source:9000")
SOURCE_ACCESS_KEY = os.getenv("MINIO_SOURCE_ACCESS_KEY", "minioadmin")
SOURCE_SECRET_KEY = os.getenv("MINIO_SOURCE_SECRET_KEY", "minioadmin")

# Destination MinIO cluster (Iceberg tables)
MINIO_DEST_ENDPOINT = os.getenv("MINIO_DEST_ENDPOINT", "http://nginx-dest:9000")
DEST_ACCESS_KEY = os.getenv("MINIO_DEST_ACCESS_KEY", "minioadmin")
DEST_SECRET_KEY = os.getenv("MINIO_DEST_SECRET_KEY", "minioadmin")

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083")

# Iceberg configuration
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


def sigv4_sign(method: str, url: str, body: str, headers: dict, endpoint: str, access_key: str, secret_key: str) -> dict:
    """Sign a request using AWS SigV4."""
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()

    headers["x-amz-content-sha256"] = payload_hash
    headers["Host"] = endpoint.replace("http://", "").replace("https://", "")

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
# Step 2: Setup Source MinIO (create test bucket to generate logs)
# =============================================================================
def setup_source_minio():
    """Create a test bucket on source MinIO to generate API logs."""
    print_step(2, "Setting Up Source MinIO Cluster (Log Generator)")

    # Wait for source MinIO to be ready
    print_info(f"Connecting to Source MinIO at {MINIO_SOURCE_ENDPOINT}...")
    max_retries = 30

    for i in range(max_retries):
        try:
            resp = requests.get(f"{MINIO_SOURCE_ENDPOINT}/minio/health/live", timeout=5)
            if resp.status_code == 200:
                print_success("Source MinIO is ready")
                break
        except Exception:
            pass

        if i < max_retries - 1:
            print_info(f"Waiting for Source MinIO... ({i+1}/{max_retries})")
            time.sleep(2)
        else:
            print_error("Source MinIO is not responding")
            return False

    # Create S3 client for source cluster
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_SOURCE_ENDPOINT,
        aws_access_key_id=SOURCE_ACCESS_KEY,
        aws_secret_access_key=SOURCE_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Create a test bucket
    test_bucket = "test-logs-bucket"
    try:
        s3_client.create_bucket(Bucket=test_bucket)
        print_success(f"Created test bucket: {test_bucket}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print_info(f"Test bucket already exists: {test_bucket}")
    except Exception as e:
        print_info(f"Bucket creation note: {e}")

    # Upload a test object to generate some logs
    try:
        s3_client.put_object(
            Bucket=test_bucket,
            Key="test-object.txt",
            Body=b"This is a test object to generate MinIO API logs",
        )
        print_success("Uploaded test object to generate API logs")
    except Exception as e:
        print_info(f"Test object upload note: {e}")

    return True


# =============================================================================
# Step 3: Create Warehouse and Namespace on Destination MinIO
# =============================================================================
def setup_dest_aistor_tables():
    """Create warehouse and namespace in Destination MinIO AIStor Tables."""
    print_step(3, "Setting Up Destination MinIO AIStor Tables (Iceberg)")

    # Wait for destination MinIO to be ready
    print_info(f"Connecting to Destination MinIO at {MINIO_DEST_ENDPOINT}...")
    max_retries = 30

    for i in range(max_retries):
        try:
            resp = requests.get(f"{MINIO_DEST_ENDPOINT}/minio/health/live", timeout=5)
            if resp.status_code == 200:
                print_success("Destination MinIO is ready")
                break
        except Exception:
            pass

        if i < max_retries - 1:
            print_info(f"Waiting for Destination MinIO... ({i+1}/{max_retries})")
            time.sleep(2)
        else:
            print_error("Destination MinIO is not responding")
            return False

    # Create warehouse using REST API with SigV4 signing
    print_info(f"Creating warehouse: {WAREHOUSE_NAME}")
    warehouse_url = f"{MINIO_DEST_ENDPOINT}/_iceberg/v1/warehouses"
    payload = json.dumps({"name": WAREHOUSE_NAME})
    headers_to_sign = {
        "content-type": "application/json",
        "content-length": str(len(payload)),
    }

    try:
        signed_headers = sigv4_sign(
            "POST", warehouse_url, payload, headers_to_sign,
            MINIO_DEST_ENDPOINT, DEST_ACCESS_KEY, DEST_SECRET_KEY
        )
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
    namespace_url = f"{MINIO_DEST_ENDPOINT}/_iceberg/v1/{WAREHOUSE_NAME}/namespaces"
    payload = json.dumps({"namespace": [NAMESPACE_NAME]})
    headers_to_sign = {
        "content-type": "application/json",
        "content-length": str(len(payload)),
    }

    try:
        signed_headers = sigv4_sign(
            "POST", namespace_url, payload, headers_to_sign,
            MINIO_DEST_ENDPOINT, DEST_ACCESS_KEY, DEST_SECRET_KEY
        )
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
# Step 4: Deploy Iceberg Sink Connector (pointing to Destination MinIO)
# =============================================================================
def deploy_connector():
    """Deploy the Iceberg Sink connector to Kafka Connect."""
    print_step(4, "Deploying Iceberg Sink Connector")

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

    # Connector configuration - pointing to DESTINATION MinIO cluster
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
            # Catalog settings (REST catalog pointing to DESTINATION MinIO AIStor)
            "iceberg.catalog.type": "rest",
            "iceberg.catalog.uri": "http://nginx-dest:9000/_iceberg",
            "iceberg.catalog.warehouse": WAREHOUSE_NAME,
            # SigV4 authentication
            "iceberg.catalog.rest.sigv4-enabled": "true",
            "iceberg.catalog.rest.signing-name": "s3tables",
            "iceberg.catalog.rest.signing-region": "us-east-1",
            # S3 settings for data access (DESTINATION MinIO)
            "iceberg.catalog.s3.access-key-id": DEST_ACCESS_KEY,
            "iceberg.catalog.s3.secret-access-key": DEST_SECRET_KEY,
            "iceberg.catalog.s3.endpoint": "http://nginx-dest:9000",
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
# Step 5: Print Summary and Test Commands
# =============================================================================
def print_summary():
    """Print summary and helpful commands."""
    print_step(5, "Setup Complete!")

    print("""
┌──────────────────────────────────────────────────────────────────────────────┐
│           KAFKA CONNECT ICEBERG SINK - DUAL CLUSTER SETUP READY              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  SOURCE CLUSTER (Kafka Log Targets - PR #2463):                              │
│    • MinIO API:     http://localhost:9000   (via nginx-source)               │
│    • MinIO Console: http://localhost:9001   (minioadmin/minioadmin)          │
│    • Sends API logs to Kafka topic: events                                   │
│                                                                              │
│  DESTINATION CLUSTER (Iceberg Tables):                                       │
│    • MinIO API:     http://localhost:9010   (via nginx-dest)                 │
│    • MinIO Console: http://localhost:9011   (minioadmin/minioadmin)          │
│    • Stores Iceberg tables from Kafka                                        │
│                                                                              │
│  KAFKA & CONNECT:                                                            │
│    • Kafka:         localhost:9092                                           │
│    • Kafka Connect: http://localhost:8083                                    │
│                                                                              │
│  ICEBERG CONFIGURATION:                                                      │
│    • Warehouse:     kafkawarehouse                                           │
│    • Namespace:     streaming                                                │
│    • Table:         events                                                   │
│    • Data Topic:    events                                                   │
│    • Control Topic: control-iceberg                                          │
│                                                                              │
│  DATA FLOW:                                                                  │
│    Source MinIO -> Kafka -> Kafka Connect -> Iceberg -> Destination MinIO    │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

Test Commands:
──────────────────────────────────────────────────────────────────────────────

1. Generate logs by interacting with SOURCE MinIO:

   # List buckets (generates API log)
   aws --endpoint-url http://localhost:9000 s3 ls

   # Upload a file (generates more logs)
   echo "test data" | aws --endpoint-url http://localhost:9000 s3 cp - s3://test-logs-bucket/test.txt

2. Check connector status:

   curl -s http://localhost:8083/connectors/iceberg-sink/status | jq

3. View Kafka topics and messages:

   # List topics
   docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

   # View events topic (logs from source MinIO)
   docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic events --from-beginning --max-messages 5

4. View DESTINATION MinIO Console (Iceberg tables):

   Open http://localhost:9011 in your browser
   Navigate to: kafkawarehouse/streaming/events/

5. View SOURCE MinIO Console (Log generator):

   Open http://localhost:9001 in your browser

6. Query Iceberg table with PyIceberg:

   ./kafka-connect-iceberg-sink.sh query

""")


# =============================================================================
# Main
# =============================================================================
def main():
    print("\n" + "=" * 60)
    print("  Kafka Connect Iceberg Sink - Dual Cluster Initialization")
    print("=" * 60)

    # Run setup steps
    if not create_control_topic():
        print_error("Failed at Step 1: Create control topic")
        sys.exit(1)

    if not setup_source_minio():
        print_error("Failed at Step 2: Setup source MinIO")
        sys.exit(1)

    if not setup_dest_aistor_tables():
        print_error("Failed at Step 3: Setup destination AIStor Tables")
        sys.exit(1)

    if not deploy_connector():
        print_error("Failed at Step 4: Deploy connector")
        sys.exit(1)

    print_summary()
    print_success("All setup steps completed successfully!")


if __name__ == "__main__":
    main()
