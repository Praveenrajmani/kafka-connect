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
import trino

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
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

# Log types configuration: (topic_name, table_name, description)
LOG_CONFIGS = [
    ("apilogs", "apilogs", "API logs"),
    ("errorlogs", "errorlogs", "Error logs"),
    ("auditlogs", "auditlogs", "Audit logs"),
]

# =============================================================================
# Iceberg Schema Definitions (based on madmin-go/log structs)
# =============================================================================

# API Logs Schema - from madmin-go/log/api.go (API and CallInfo structs)
APILOGS_SCHEMA = {
    "type": "struct",
    "fields": [
        {"id": 1, "name": "version", "required": False, "type": "string"},
        {"id": 2, "name": "time", "required": True, "type": "timestamptz"},
        {"id": 3, "name": "node", "required": False, "type": "string"},
        {"id": 4, "name": "origin", "required": False, "type": "string"},
        {"id": 5, "name": "type", "required": False, "type": "string"},
        {"id": 6, "name": "name", "required": False, "type": "string"},
        {"id": 7, "name": "bucket", "required": False, "type": "string"},
        {"id": 8, "name": "object", "required": False, "type": "string"},
        {"id": 9, "name": "versionId", "required": False, "type": "string"},
        {
            "id": 10,
            "name": "tags",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 11,
                "key": "string",
                "value-id": 12,
                "value": "string",
                "value-required": False,
            },
        },
        {
            "id": 13,
            "name": "callInfo",
            "required": False,
            "type": {
                "type": "struct",
                "fields": [
                    {"id": 14, "name": "httpStatusCode", "required": False, "type": "int"},
                    {"id": 15, "name": "rx", "required": False, "type": "long"},
                    {"id": 16, "name": "tx", "required": False, "type": "long"},
                    {"id": 17, "name": "txHeaders", "required": False, "type": "long"},
                    {"id": 18, "name": "timeToFirstByte", "required": False, "type": "string"},
                    {"id": 19, "name": "requestReadTime", "required": False, "type": "string"},
                    {"id": 20, "name": "responseWriteTime", "required": False, "type": "string"},
                    {"id": 21, "name": "requestTime", "required": False, "type": "string"},
                    {"id": 22, "name": "timeToResponse", "required": False, "type": "string"},
                    {"id": 23, "name": "readBlocked", "required": False, "type": "string"},
                    {"id": 24, "name": "writeBlocked", "required": False, "type": "string"},
                    {"id": 25, "name": "sourceHost", "required": False, "type": "string"},
                    {"id": 26, "name": "requestID", "required": False, "type": "string"},
                    {"id": 27, "name": "userAgent", "required": False, "type": "string"},
                    {"id": 28, "name": "requestPath", "required": False, "type": "string"},
                    {"id": 29, "name": "requestHost", "required": False, "type": "string"},
                    {
                        "id": 30,
                        "name": "requestClaims",
                        "required": False,
                        "type": {
                            "type": "map",
                            "key-id": 31,
                            "key": "string",
                            "value-id": 32,
                            "value": "string",
                            "value-required": False,
                        },
                    },
                    {
                        "id": 33,
                        "name": "requestQuery",
                        "required": False,
                        "type": {
                            "type": "map",
                            "key-id": 34,
                            "key": "string",
                            "value-id": 35,
                            "value": "string",
                            "value-required": False,
                        },
                    },
                    {
                        "id": 36,
                        "name": "requestHeader",
                        "required": False,
                        "type": {
                            "type": "map",
                            "key-id": 37,
                            "key": "string",
                            "value-id": 38,
                            "value": "string",
                            "value-required": False,
                        },
                    },
                    {
                        "id": 39,
                        "name": "responseHeader",
                        "required": False,
                        "type": {
                            "type": "map",
                            "key-id": 40,
                            "key": "string",
                            "value-id": 41,
                            "value": "string",
                            "value-required": False,
                        },
                    },
                    {"id": 42, "name": "accessKey", "required": False, "type": "string"},
                    {"id": 43, "name": "parentUser", "required": False, "type": "string"},
                ],
            },
        },
    ],
}

# Error Logs Schema - from madmin-go/log/error.go (Error and Trace structs)
ERRORLOGS_SCHEMA = {
    "type": "struct",
    "fields": [
        {"id": 1, "name": "version", "required": False, "type": "string"},
        {"id": 2, "name": "node", "required": False, "type": "string"},
        {"id": 3, "name": "time", "required": True, "type": "timestamptz"},
        {"id": 4, "name": "message", "required": False, "type": "string"},
        {"id": 5, "name": "apiName", "required": False, "type": "string"},
        {
            "id": 6,
            "name": "tags",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 7,
                "key": "string",
                "value-id": 8,
                "value": "string",
                "value-required": False,
            },
        },
        {
            "id": 9,
            "name": "trace",
            "required": False,
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "id": 10,
                        "name": "source",
                        "required": False,
                        "type": {
                            "type": "list",
                            "element-id": 11,
                            "element": "string",
                            "element-required": False,
                        },
                    },
                    {
                        "id": 12,
                        "name": "variables",
                        "required": False,
                        "type": {
                            "type": "map",
                            "key-id": 13,
                            "key": "string",
                            "value-id": 14,
                            "value": "string",
                            "value-required": False,
                        },
                    },
                ],
            },
        },
    ],
}

# Audit Logs Schema - from madmin-go/log/audit.go (Audit struct)
# Note: 'details' is stored as JSON string due to union type with 15+ variants
AUDITLOGS_SCHEMA = {
    "type": "struct",
    "fields": [
        {"id": 1, "name": "version", "required": False, "type": "string"},
        {"id": 2, "name": "time", "required": True, "type": "timestamptz"},
        {"id": 3, "name": "node", "required": False, "type": "string"},
        {"id": 4, "name": "apiName", "required": False, "type": "string"},
        {"id": 5, "name": "category", "required": False, "type": "string"},
        {"id": 6, "name": "action", "required": False, "type": "string"},
        {"id": 7, "name": "bucket", "required": False, "type": "string"},
        {
            "id": 8,
            "name": "tags",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 9,
                "key": "string",
                "value-id": 10,
                "value": "string",
                "value-required": False,
            },
        },
        {"id": 11, "name": "requestID", "required": False, "type": "string"},
        {
            "id": 12,
            "name": "requestClaims",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 13,
                "key": "string",
                "value-id": 14,
                "value": "string",
                "value-required": False,
            },
        },
        {"id": 15, "name": "sourceHost", "required": False, "type": "string"},
        {"id": 16, "name": "accessKey", "required": False, "type": "string"},
        {"id": 17, "name": "parentUser", "required": False, "type": "string"},
        {"id": 18, "name": "details", "required": False, "type": "string"},
    ],
}

# Schema and partition spec mapping for each table
TABLE_CONFIGS = {
    "apilogs": {
        "schema": APILOGS_SCHEMA,
        "partition_spec": {
            "fields": [
                {"source-id": 2, "transform": "day", "name": "time_day"},
                {"source-id": 7, "transform": "identity", "name": "bucket"},
            ]
        },
    },
    "errorlogs": {
        "schema": ERRORLOGS_SCHEMA,
        "partition_spec": {
            "fields": [
                {"source-id": 3, "transform": "day", "name": "time_day"},
            ]
        },
    },
    "auditlogs": {
        "schema": AUDITLOGS_SCHEMA,
        "partition_spec": {
            "fields": [
                {"source-id": 2, "transform": "day", "name": "time_day"},
                {"source-id": 5, "transform": "identity", "name": "category"},
            ]
        },
    },
}


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

    # Create data topics for each log type
    for topic_name, table_name, description in LOG_CONFIGS:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="iceberg-init",
            )
            data_topic = NewTopic(
                name=topic_name,
                num_partitions=3,
                replication_factor=1,
            )
            admin_client.create_topics([data_topic])
            print_success(f"Created data topic: {topic_name} (3 partitions) - {description}")
        except TopicAlreadyExistsError:
            print_info(f"Data topic already exists: {topic_name}")
        except Exception as e:
            print_error(f"Failed to create data topic {topic_name}: {e}")
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
# Step 3: Create Iceberg Tables with Explicit Schemas
# =============================================================================
def create_iceberg_tables():
    """Create Iceberg tables with explicit schemas and partition specs."""
    print_step(3, "Creating Iceberg Tables with Explicit Schemas")

    for table_name, config in TABLE_CONFIGS.items():
        full_table_name = f"{NAMESPACE_NAME}.{table_name}"
        print_info(f"Creating table: {full_table_name}")

        # Build table creation payload
        table_url = f"{MINIO_ENDPOINT}/_iceberg/v1/{WAREHOUSE_NAME}/namespaces/{NAMESPACE_NAME}/tables"
        payload = json.dumps({
            "name": table_name,
            "schema": config["schema"],
            "partition-spec": config["partition_spec"],
        })
        headers_to_sign = {
            "content-type": "application/json",
            "content-length": str(len(payload)),
        }

        try:
            signed_headers = sigv4_sign("POST", table_url, payload, headers_to_sign)
            resp = requests.post(table_url, data=payload, headers=signed_headers, timeout=30)

            if resp.status_code == 200:
                partition_fields = [f["name"] for f in config["partition_spec"]["fields"]]
                print_success(f"Created table: {full_table_name} (partitioned by: {', '.join(partition_fields)})")
            elif resp.status_code == 409:
                print_info(f"Table already exists: {full_table_name}")
            else:
                print_error(f"Failed to create table {full_table_name}: {resp.status_code} - {resp.text}")
                return False
        except Exception as e:
            print_error(f"Error creating table {full_table_name}: {e}")
            return False

    return True


# =============================================================================
# Step 4: Deploy Iceberg Sink Connectors
# =============================================================================
def deploy_connectors():
    """Deploy Iceberg Sink connectors for each log type to Kafka Connect."""
    print_step(4, "Deploying Iceberg Sink Connectors")

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

    # Deploy a connector for each log type
    for topic_name, table_name, description in LOG_CONFIGS:
        connector_name = f"iceberg-sink-{topic_name}"

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
                "topics": topic_name,
                # Table settings (tables pre-created with explicit schemas)
                "iceberg.tables": f"{NAMESPACE_NAME}.{table_name}",
                "iceberg.tables.auto-create-enabled": "false",
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
        print_info(f"Deploying connector: {connector_name} ({description})")
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

    # Wait for connectors to be running
    print_info("Waiting for connectors to start...")
    time.sleep(5)

    # Check status of all connectors
    for topic_name, table_name, description in LOG_CONFIGS:
        connector_name = f"iceberg-sink-{topic_name}"
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status")
        if resp.status_code == 200:
            status = resp.json()
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            task_states = [t.get("state", "UNKNOWN") for t in status.get("tasks", [])]

            print_info(f"{connector_name}: state={connector_state}, tasks={task_states}")

            if connector_state == "RUNNING":
                print_success(f"{connector_name} is running!")
            else:
                print_error(f"{connector_name} is in unexpected state: {connector_state}")

    return True


# =============================================================================
# Step 5: Create Trino Catalog for Querying Iceberg Tables
# =============================================================================
def setup_trino_catalog():
    """Create a dynamic Trino catalog pointing to the Iceberg REST catalog."""
    print_step(5, "Setting Up Trino Catalog")

    # Wait for Trino to be ready
    print_info(f"Connecting to Trino at {TRINO_HOST}:{TRINO_PORT}...")
    max_retries = 30

    for i in range(max_retries):
        try:
            resp = requests.get(f"http://{TRINO_HOST}:{TRINO_PORT}/v1/status", timeout=5)
            if resp.status_code == 200:
                print_success("Trino is ready")
                break
        except Exception:
            pass

        if i < max_retries - 1:
            print_info(f"Waiting for Trino... ({i+1}/{max_retries})")
            time.sleep(2)
        else:
            print_error("Trino is not responding")
            return False

    # Connect to Trino and create dynamic catalog
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user="trino",
        )
        cursor = conn.cursor()

        # Create dynamic Iceberg catalog pointing to MinIO AIStor Tables
        catalog_name = WAREHOUSE_NAME
        create_catalog_sql = f"""
            CREATE CATALOG {catalog_name} USING iceberg
            WITH (
                "iceberg.catalog.type" = 'rest',
                "iceberg.rest-catalog.uri" = '{MINIO_ENDPOINT}/_iceberg',
                "iceberg.rest-catalog.warehouse" = '{WAREHOUSE_NAME}',
                "iceberg.rest-catalog.vended-credentials-enabled" = 'true',
                "iceberg.rest-catalog.security" = 'SIGV4',
                "iceberg.rest-catalog.signing-name" = 's3tables',
                "s3.region" = 'us-east-1',
                "s3.aws-access-key" = '{ACCESS_KEY}',
                "s3.aws-secret-key" = '{SECRET_KEY}',
                "s3.endpoint" = '{MINIO_ENDPOINT}',
                "s3.path-style-access" = 'true',
                "fs.hadoop.enabled" = 'false',
                "fs.native-s3.enabled" = 'true'
            )
        """

        print_info(f"Creating Trino catalog: {catalog_name}")
        cursor.execute(create_catalog_sql)
        print_success(f"Created Trino catalog: {catalog_name}")

        # Verify the catalog by listing schemas
        cursor.execute(f"SHOW SCHEMAS FROM {catalog_name}")
        schemas = cursor.fetchall()
        print_info(f"Available schemas in {catalog_name}: {[s[0] for s in schemas]}")

        cursor.close()
        conn.close()

    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            print_info(f"Catalog already exists: {catalog_name}")
        else:
            print_error(f"Failed to create Trino catalog: {e}")
            return False

    return True


# =============================================================================
# Step 6: Print Summary and Test Commands
# =============================================================================
def print_summary():
    """Print summary and helpful commands."""
    print_step(6, "Setup Complete!")

    print("""
┌──────────────────────────────────────────────────────────────────────────┐
│                    KAFKA CONNECT ICEBERG SINK READY                      │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Services:                                                               │
│    • Kafka:         localhost:9092                                       │
│    • Kafka Connect: http://localhost:8083                                │
│    • MinIO Console: http://localhost:9001 (minioadmin/minioadmin)        │
│    • MinIO API:     http://localhost:9000                                │
│    • Trino:         http://localhost:9999 (Web UI & JDBC)                │
│                                                                          │
│  Configuration:                                                          │
│    • Warehouse:     kafkawarehouse                                       │
│    • Namespace:     streaming                                            │
│    • Control Topic: control-iceberg                                      │
│                                                                          │
│  Log Pipelines (with partitioning):                                      │
│    ┌──────────────┬──────────────┬───────────────────────────────┐       │
│    │ Table        │ Partitions   │ Source Schema                 │       │
│    ├──────────────┼──────────────┼───────────────────────────────┤       │
│    │ apilogs      │ day, bucket  │ madmin-go/log/api.go          │       │
│    │ errorlogs    │ day          │ madmin-go/log/error.go        │       │
│    │ auditlogs    │ day, category│ madmin-go/log/audit.go        │       │
│    └──────────────┴──────────────┴───────────────────────────────┘       │
│                                                                          │
│  Connectors:                                                             │
│    • iceberg-sink-apilogs   (apilogs -> streaming.apilogs)               │
│    • iceberg-sink-errorlogs (errorlogs -> streaming.errorlogs)           │
│    • iceberg-sink-auditlogs (auditlogs -> streaming.auditlogs)           │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

Test Commands:
──────────────────────────────────────────────────────────────────────────

1. Check connector status:

   curl -s http://localhost:8083/connectors/iceberg-sink-apilogs/status | jq
   curl -s http://localhost:8083/connectors/iceberg-sink-errorlogs/status | jq
   curl -s http://localhost:8083/connectors/iceberg-sink-auditlogs/status | jq

2. List Kafka topics:

   docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

3. View MinIO Console:

   Open http://localhost:9001 in your browser
   Navigate to: kafkawarehouse/streaming/apilogs/
                kafkawarehouse/streaming/errorlogs/
                kafkawarehouse/streaming/auditlogs/

4. Query tables with Trino (recommended):

   # Connect to Trino CLI
   docker exec -it trino trino

   # Example queries:
   SHOW CATALOGS;
   SHOW SCHEMAS FROM kafkawarehouse;
   SHOW TABLES FROM kafkawarehouse.streaming;

   SELECT * FROM kafkawarehouse.streaming.apilogs LIMIT 10;
   SELECT * FROM kafkawarehouse.streaming.errorlogs LIMIT 10;
   SELECT * FROM kafkawarehouse.streaming.auditlogs LIMIT 10;

   # Query API logs by bucket
   SELECT bucket, name, time, "callInfo"."httpStatusCode"
   FROM kafkawarehouse.streaming.apilogs
   WHERE bucket IS NOT NULL
   ORDER BY time DESC LIMIT 20;

   # Count requests by API
   SELECT name, COUNT(*) as count
   FROM kafkawarehouse.streaming.apilogs
   GROUP BY name
   ORDER BY count DESC;

5. Trino Web UI:

   Open http://localhost:9999 in your browser

6. Query tables with PyIceberg (requires AWS creds in env):

   ./kafka-connect-iceberg-sink.sh query

7. View topic messages (logs will appear as MinIO handles requests):

   docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic apilogs --from-beginning
   docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic errorlogs --from-beginning
   docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic auditlogs --from-beginning

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

    if not create_iceberg_tables():
        print_error("Failed at Step 3: Create Iceberg tables")
        sys.exit(1)

    if not deploy_connectors():
        print_error("Failed at Step 4: Deploy connectors")
        sys.exit(1)

    if not setup_trino_catalog():
        print_error("Failed at Step 5: Setup Trino catalog")
        sys.exit(1)

    print_summary()
    print_success("All setup steps completed successfully!")


if __name__ == "__main__":
    main()
