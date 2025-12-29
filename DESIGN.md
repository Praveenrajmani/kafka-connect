# Kafka Connect Iceberg Sink - Design Documentation

## 1. Executive Summary

This project implements a **real-time streaming data pipeline** that captures MinIO object storage logs and persists them to Apache Iceberg tables stored on MinIO AIStor. It provides a complete, containerized solution for log ingestion, processing, and queryable analytics storage.

**Key Value Proposition:**
- Real-time visibility into MinIO operations (API calls, errors, audit events)
- Queryable historical log data using standard SQL-compatible tools
- Schema evolution support - automatically adapts to new log fields
- Production-ready containerized deployment

---

## 2. Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                           KAFKA CONNECT ICEBERG SINK                                  │
├───────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                       │
│  ┌─────────────────┐                              ┌─────────────────────────┐         │
│  │  MinIO AIStor   │                              │  MinIO AIStor           │         │
│  │  (Log Source)   │                              │  (Iceberg Tables)       │         │
│  │                 │                              │                         │         │
│  │  ┌───────────┐  │    ┌──────────────────┐     │  ┌───────────────────┐  │         │
│  │  │ API Logs  │──┼───▶│                  │     │  │streaming.apilogs  │  │         │
│  │  └───────────┘  │    │   Kafka Broker   │     │  └───────────────────┘  │         │
│  │  ┌───────────┐  │    │   (KRaft Mode)   │     │  ┌───────────────────┐  │         │
│  │  │Error Logs │──┼───▶│                  │────▶│  │streaming.errorlogs│  │         │
│  │  └───────────┘  │    │  ┌────────────┐  │     │  └───────────────────┘  │         │
│  │  ┌───────────┐  │    │  │  Topics:   │  │     │  ┌───────────────────┐  │         │
│  │  │Audit Logs │──┼───▶│  │ • apilogs  │  │     │  │streaming.auditlogs│  │         │
│  │  └───────────┘  │    │  │ • errorlogs│  │     │  └───────────────────┘  │         │
│  │                 │    │  │ • auditlogs│  │     │           ▲             │         │
│  └─────────────────┘    │  └────────────┘  │     └───────────┼─────────────┘         │
│                         └────────┬─────────┘                 │                       │
│                                  │                           │                       │
│                                  ▼                           │                       │
│                         ┌──────────────────┐                │                       │
│                         │  Kafka Connect   │                │                       │
│                         │  ┌────────────┐  │                │                       │
│                         │  │ Iceberg    │  │────────────────┤                       │
│                         │  │ Sink       │  │   (REST Catalog + S3 Data)             │
│                         │  │ Connectors │  │                │                       │
│                         │  │ (x3)       │  │                │                       │
│                         │  └────────────┘  │                │                       │
│                         │  ┌────────────┐  │                │                       │
│                         │  │ Control    │◀─┼── control-iceberg topic               │
│                         │  │ Topic      │  │   (commit coordination)               │
│                         │  └────────────┘  │                │                       │
│                         └──────────────────┘                │                       │
│                                                             │                       │
│                         ┌──────────────────┐                │                       │
│                         │  Trino Query     │                │                       │
│                         │  Engine          │────────────────┘                       │
│                         │  ┌────────────┐  │   (REST Catalog + S3 Data)             │
│                         │  │ Iceberg    │  │                                        │
│                         │  │ Catalog    │  │◀── SQL Queries (JDBC/CLI/Web UI)       │
│                         │  └────────────┘  │                                        │
│                         └──────────────────┘                                        │
│                                                                                       │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Component Details

### 3.1 Kafka Broker (KRaft Mode)

| Property | Value |
|----------|-------|
| **Image** | `confluentinc/cp-kafka:7.5.0` |
| **Mode** | KRaft (Kafka Raft) - no Zookeeper |
| **Ports** | 9092 (external), 29092 (internal), 29093 (controller) |
| **Node ID** | 1 |
| **Cluster ID** | `MkU3OEVBNTcwNTJENDM2Qk` |

**Key Configuration:**
- Single-node deployment (replication factor: 1)
- Combined broker + controller role
- No Zookeeper dependency (simplified operations)

### 3.2 Kafka Connect

| Property | Value |
|----------|-------|
| **Image** | `confluentinc/cp-kafka-connect:7.5.0` |
| **Port** | 8083 (REST API) |
| **Plugin** | Iceberg Kafka Connect v0.6.19 |
| **Group ID** | `iceberg-connect-cluster` |

**Connector Plugin:**
- **Source:** [databricks/iceberg-kafka-connect](https://github.com/databricks/iceberg-kafka-connect)
- **Version:** 0.6.19
- **Auto-downloaded** on first run

**Internal Topics:**
| Topic | Purpose | Partitions |
|-------|---------|------------|
| `connect-configs` | Connector configurations | 1 |
| `connect-offsets` | Consumer offsets | 5 |
| `connect-status` | Connector status | 3 |

### 3.3 MinIO AIStor

| Property | Value |
|----------|-------|
| **Image** | `praveenminio/eos:log` |
| **Ports** | 9000 (API), 9001 (Console) |
| **Credentials** | `minioadmin` / `minioadmin` |
| **Features** | S3-compatible API, Iceberg Tables REST catalog |

**Dual Role:**
1. **Log Producer:** Generates API, error, and audit logs → Kafka topics
2. **Data Store:** Stores Iceberg table data and metadata

**Log Target Configuration:**
```yaml
# API Logs
MINIO_LOG_API_KAFKA_ENABLE: "on"
MINIO_LOG_API_KAFKA_BROKERS: kafka:29092
MINIO_LOG_API_KAFKA_TOPIC: apilogs

# Error Logs
MINIO_LOG_ERROR_KAFKA_ENABLE: "on"
MINIO_LOG_ERROR_KAFKA_BROKERS: kafka:29092
MINIO_LOG_ERROR_KAFKA_TOPIC: errorlogs

# Audit Logs
MINIO_LOG_AUDIT_KAFKA_ENABLE: "on"
MINIO_LOG_AUDIT_KAFKA_BROKERS: kafka:29092
MINIO_LOG_AUDIT_KAFKA_TOPIC: auditlogs
```

### 3.4 Trino Query Engine

| Property | Value |
|----------|-------|
| **Image** | `trinodb/trino:477` |
| **Port** | 8080 (mapped to 9999 externally) |
| **Catalog Management** | Dynamic (catalogs created via SQL) |

**Key Configuration:**
- Uses dynamic catalog management for runtime catalog creation
- Connects to MinIO AIStor Tables via Iceberg REST catalog
- Uses SigV4 authentication with `s3tables` signing name
- Native S3 filesystem (Hadoop disabled for better performance)

**Trino Catalog Configuration (created automatically):**

| Parameter | Value |
|-----------|-------|
| `iceberg.catalog.type` | `rest` |
| `iceberg.rest-catalog.uri` | `http://minio:9000/_iceberg` |
| `iceberg.rest-catalog.warehouse` | `kafkawarehouse` |
| `iceberg.rest-catalog.security` | `SIGV4` |
| `iceberg.rest-catalog.signing-name` | `s3tables` |
| `s3.path-style-access` | `true` |
| `fs.native-s3.enabled` | `true` |

### 3.5 Init Container

| Property | Value |
|----------|-------|
| **Image** | `python:3.11-slim` |
| **Purpose** | One-time initialization |
| **Script** | `init-setup.py` |

**Initialization Steps:**
1. Create Kafka control topic (`control-iceberg`) - 1 partition
2. Create data topics (`apilogs`, `errorlogs`, `auditlogs`) - 3 partitions each
3. Create warehouse (`kafkawarehouse`) via REST API with SigV4
4. Create namespace (`streaming`)
5. Deploy 3 Iceberg sink connectors
6. Create Trino dynamic catalog for querying tables

---

## 4. Data Flow

### 4.1 Log Generation → Kafka

```
MinIO Operation (e.g., PutObject)
        │
        ▼
┌───────────────────────────────────────┐
│  MinIO AIStor generates log entry     │
│  based on operation type:             │
│  • API call → apilogs topic           │
│  • Error → errorlogs topic            │
│  • Audit event → auditlogs topic      │
└───────────────────────────────────────┘
        │
        ▼
   Kafka Topic (JSON message)
```

### 4.2 Kafka → Iceberg Tables

```
Kafka Topic
        │
        ▼
┌───────────────────────────────────────┐
│  Iceberg Sink Connector               │
│  • Consumes JSON messages             │
│  • Converts to Iceberg records        │
│  • Batches commits (10s interval)     │
└───────────────────────────────────────┘
        │
        ├──▶ REST Catalog (metadata)
        │    http://minio:9000/_iceberg
        │
        └──▶ S3 Storage (data files)
             s3://kafkawarehouse/streaming/{table}/
```

### 4.3 Commit Coordination

The `control-iceberg` topic ensures exactly-once delivery:
- Single partition (required for coordinator election)
- Coordinates distributed commits across connector tasks
- Prevents duplicate writes during failures

---

## 5. Data Schemas & Partitioning

Tables are created with explicit Iceberg schemas based on MinIO's `madmin-go/log` package structs. This ensures type safety and enables efficient partitioning for common query patterns.

### 5.1 API Log Schema (`streaming.apilogs`)

**Source:** `madmin-go/log/api.go` - `API` and `CallInfo` structs

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Log format version |
| `time` | timestamptz | Event timestamp (required) |
| `node` | string | MinIO node identifier |
| `origin` | string | Request origin (client, ilm, batch, etc.) |
| `type` | string | API type (object, bucket, admin, auth) |
| `name` | string | API name (e.g., s3.GetObject) |
| `bucket` | string | Target bucket name |
| `object` | string | Target object key |
| `versionId` | string | Object version ID |
| `tags` | map<string, string> | Custom tags |
| `callInfo` | struct | Nested call details (see below) |

**CallInfo Nested Struct:**

| Field | Type | Description |
|-------|------|-------------|
| `httpStatusCode` | int | HTTP response status |
| `rx` | long | Input bytes received |
| `tx` | long | Output bytes sent |
| `txHeaders` | long | Header bytes sent |
| `timeToFirstByte` | string | TTFB duration |
| `requestReadTime` | string | Request read duration |
| `responseWriteTime` | string | Response write duration |
| `requestTime` | string | Total request duration |
| `timeToResponse` | string | Time to response |
| `readBlocked` | string | Read blocked duration |
| `writeBlocked` | string | Write blocked duration |
| `sourceHost` | string | Client IP address |
| `requestID` | string | Unique request identifier |
| `userAgent` | string | Client user agent |
| `requestPath` | string | Request URL path |
| `requestHost` | string | Request host header |
| `requestClaims` | map<string, string> | JWT claims |
| `requestQuery` | map<string, string> | Query parameters |
| `requestHeader` | map<string, string> | Request headers |
| `responseHeader` | map<string, string> | Response headers |
| `accessKey` | string | Access key used |
| `parentUser` | string | Parent user for service accounts |

**Partitioning:** `day(time), identity(bucket)`
- Optimized for queries like: "API activity for bucket X in the last N days"

**Example JSON:**

```json
{
  "version": "1",
  "time": "2025-01-15T10:30:00.000Z",
  "node": "minio-node-1",
  "origin": "client",
  "type": "object",
  "name": "s3.GetObject",
  "bucket": "testbucket",
  "object": "file.txt",
  "versionId": "",
  "tags": {},
  "callInfo": {
    "httpStatusCode": 200,
    "rx": 0,
    "tx": 1024,
    "txHeaders": 256,
    "timeToFirstByte": "5ms",
    "requestReadTime": "2ms",
    "responseWriteTime": "10ms",
    "requestTime": "15ms",
    "timeToResponse": "12ms",
    "sourceHost": "192.168.1.100",
    "requestID": "17A3C4775707B695",
    "userAgent": "MinIO (linux; amd64) minio-go/v7.0.0",
    "requestPath": "/testbucket/file.txt",
    "requestHost": "localhost:9000",
    "accessKey": "minioadmin"
  }
}
```

### 5.2 Error Log Schema (`streaming.errorlogs`)

**Source:** `madmin-go/log/error.go` - `Error` and `Trace` structs

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Log format version |
| `node` | string | MinIO node identifier |
| `time` | timestamptz | Event timestamp (required) |
| `message` | string | Error message |
| `apiName` | string | API that triggered the error |
| `tags` | map<string, string> | Custom tags |
| `trace` | struct | Nested trace details (see below) |

**Trace Nested Struct:**

| Field | Type | Description |
|-------|------|-------------|
| `source` | list<string> | Stack trace source locations |
| `variables` | map<string, string> | Context variables |

**Partitioning:** `day(time)`
- Optimized for queries like: "Errors in the last hour/day"

**Example JSON:**

```json
{
  "version": "1",
  "node": "minio-node-1",
  "time": "2025-01-15T10:30:00.000Z",
  "message": "The specified bucket does not exist",
  "apiName": "s3.GetObject",
  "trace": {
    "source": ["api-errors.go:150", "bucket-handlers.go:420"],
    "variables": {"bucket": "nonexistent"}
  },
  "tags": {
    "bucket": "nonexistent"
  }
}
```

### 5.3 Audit Log Schema (`streaming.auditlogs`)

**Source:** `madmin-go/log/audit.go` - `Audit` struct

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Log format version |
| `time` | timestamptz | Event timestamp (required) |
| `node` | string | MinIO node identifier |
| `apiName` | string | Admin API name |
| `category` | string | Audit category (user, policy, config, etc.) |
| `action` | string | Action performed (create, update, delete, etc.) |
| `bucket` | string | Target bucket (if applicable) |
| `tags` | map<string, string> | Custom tags |
| `requestID` | string | Unique request identifier |
| `requestClaims` | map<string, string> | JWT claims |
| `sourceHost` | string | Client IP address |
| `accessKey` | string | Access key used |
| `parentUser` | string | Parent user for service accounts |
| `details` | string | JSON string with category-specific details |

**Audit Categories:** `config`, `user`, `service-account`, `policy`, `group`, `bucket`, `lifecycle`, `replication`, `notification`, `encryption`, `cors`, `versioning`, `service`, `kms`, `site-replication`, `pool`, `idp`, `log-recorder`, `heal`, `batch`

**Audit Actions:** `create`, `update`, `delete`, `enable`, `disable`, `set`, `reset`, `restore`, `clear`, `start`, `stop`, `restart`, `attach`, `detach`

**Partitioning:** `day(time), identity(category)`
- Optimized for queries like: "All user changes this month"

**Example JSON:**

```json
{
  "version": "1",
  "time": "2025-01-15T10:30:00.000Z",
  "node": "minio-node-1",
  "apiName": "s3.PutObject",
  "category": "bucket",
  "action": "create",
  "bucket": "testbucket",
  "tags": {},
  "requestID": "17A3C4775707B695",
  "requestClaims": {},
  "sourceHost": "192.168.1.100",
  "accessKey": "minioadmin",
  "parentUser": "",
  "details": "{\"bucketName\":\"testbucket\"}"
}
```

### 5.4 Partitioning Summary

| Table | Partition Columns | Query Optimization |
|-------|-------------------|-------------------|
| `apilogs` | `day(time)`, `bucket` | Time-range + bucket filtering |
| `errorlogs` | `day(time)` | Time-range filtering |
| `auditlogs` | `day(time)`, `category` | Time-range + category filtering |

---

## 6. Connector Configuration

Each log type has a dedicated Iceberg sink connector:

| Connector | Topic | Table |
|-----------|-------|-------|
| `iceberg-sink-apilogs` | `apilogs` | `streaming.apilogs` |
| `iceberg-sink-errorlogs` | `errorlogs` | `streaming.errorlogs` |
| `iceberg-sink-auditlogs` | `auditlogs` | `streaming.auditlogs` |

**Common Configuration:**

```json
{
  "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
  "tasks.max": "2",

  "iceberg.tables.auto-create-enabled": "false",
  "iceberg.tables.evolve-schema-enabled": "true",

  "iceberg.catalog.type": "rest",
  "iceberg.catalog.uri": "http://minio:9000/_iceberg",
  "iceberg.catalog.warehouse": "kafkawarehouse",

  "iceberg.catalog.rest.sigv4-enabled": "true",
  "iceberg.catalog.rest.signing-name": "s3tables",
  "iceberg.catalog.rest.signing-region": "us-east-1",

  "iceberg.catalog.s3.endpoint": "http://minio:9000",
  "iceberg.catalog.s3.path-style-access": "true",

  "iceberg.control.topic": "control-iceberg",
  "iceberg.control.commit.interval-ms": "10000",
  "iceberg.control.commit.timeout-ms": "60000",

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

---

## 7. Security & Authentication

### 7.1 SigV4 Authentication

The system uses AWS Signature Version 4 for authenticating with MinIO's Iceberg REST catalog:

```
┌─────────────────┐        SigV4 Signed Request        ┌─────────────────┐
│ Kafka Connect   │ ──────────────────────────────────▶│ MinIO AIStor    │
│ (Iceberg Sink)  │                                    │ /_iceberg       │
└─────────────────┘                                    └─────────────────┘
```

**Credentials Flow:**
1. Access Key / Secret Key configured in connector
2. Each request signed with SigV4 (service: `s3tables`, region: `us-east-1`)
3. MinIO validates signature before processing

### 7.2 Network Security

All services communicate via Docker bridge network (`kafka-iceberg`):

| Service | Internal Access | External Access |
|---------|-----------------|-----------------|
| Kafka | `kafka:29092` | `localhost:9092` |
| Kafka Connect | `kafka-connect:8083` | `localhost:8083` |
| MinIO | `minio:9000` | `localhost:9000` |
| Trino | `trino:8080` | `localhost:9999` |

---

## 8. Storage Architecture

### 8.1 Warehouse Structure

```
kafkawarehouse/                    (S3 Bucket / Warehouse)
└── streaming/                     (Namespace)
    ├── apilogs/                   (Table)
    │   ├── metadata/
    │   │   └── *.json             (Iceberg metadata files)
    │   └── data/
    │       └── *.parquet          (Data files)
    ├── errorlogs/
    │   ├── metadata/
    │   └── data/
    └── auditlogs/
        ├── metadata/
        └── data/
```

### 8.2 File Formats

| Type | Format | Purpose |
|------|--------|---------|
| Data | Parquet | Columnar storage, efficient analytics |
| Metadata | JSON | Table schema, snapshots, manifest lists |

---

## 9. Operational Commands

### 9.1 Service Management

```bash
# Start all services
./kafka-connect-iceberg-sink.sh start

# Stop all services
./kafka-connect-iceberg-sink.sh stop

# Restart services
./kafka-connect-iceberg-sink.sh restart

# View status
./kafka-connect-iceberg-sink.sh status

# View logs
./kafka-connect-iceberg-sink.sh logs -f

# Clean up (remove all data)
./kafka-connect-iceberg-sink.sh clean
```

### 9.2 Data Operations

```bash
# Produce test messages to all topics
./kafka-connect-iceberg-sink.sh produce

# Produce to specific topic
./kafka-connect-iceberg-sink.sh produce apilogs -n 500

# Query all Iceberg tables
./kafka-connect-iceberg-sink.sh query

# Query specific table
./kafka-connect-iceberg-sink.sh query apilogs
```

### 9.3 Connector Management

```bash
# Check connector status
curl -s http://localhost:8083/connectors/iceberg-sink-apilogs/status | jq

# Restart connector
curl -X POST http://localhost:8083/connectors/iceberg-sink-apilogs/restart

# List all connectors
curl -s http://localhost:8083/connectors | jq
```

### 9.4 Trino Queries

```bash
# Connect to Trino CLI
docker exec -it trino trino

# Access Trino Web UI
open http://localhost:9999
```

**Example SQL Queries:**

```sql
-- List available catalogs
SHOW CATALOGS;

-- List schemas in the warehouse
SHOW SCHEMAS FROM kafkawarehouse;

-- List tables in the streaming namespace
SHOW TABLES FROM kafkawarehouse.streaming;

-- Query API logs
SELECT * FROM kafkawarehouse.streaming.apilogs LIMIT 10;

-- Query API logs by bucket with nested field access
SELECT
    bucket,
    name,
    time,
    "callInfo"."httpStatusCode" as status,
    "callInfo"."sourceHost" as client_ip
FROM kafkawarehouse.streaming.apilogs
WHERE bucket IS NOT NULL
ORDER BY time DESC
LIMIT 20;

-- Count requests by API name
SELECT name, COUNT(*) as count
FROM kafkawarehouse.streaming.apilogs
GROUP BY name
ORDER BY count DESC;

-- Query error logs
SELECT time, message, "apiName"
FROM kafkawarehouse.streaming.errorlogs
ORDER BY time DESC
LIMIT 10;

-- Query audit logs by category
SELECT category, action, time, "accessKey"
FROM kafkawarehouse.streaming.auditlogs
WHERE category = 'bucket'
ORDER BY time DESC
LIMIT 10;

-- View table metadata (snapshots)
SELECT * FROM kafkawarehouse.streaming."apilogs$snapshots";

-- View table history
SELECT * FROM kafkawarehouse.streaming."apilogs$history";
```

---

## 10. Dependencies

### 10.1 Runtime Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| Docker | Latest | Container runtime |
| Docker Compose | Latest | Multi-container orchestration |
| curl | Any | Plugin download, health checks |
| jq | Any | JSON parsing |
| unzip | Any | Plugin extraction |

### 10.2 Container Images

| Image | Version | Size |
|-------|---------|------|
| `confluentinc/cp-kafka` | 7.5.0 | ~800MB |
| `confluentinc/cp-kafka-connect` | 7.5.0 | ~1.2GB |
| `praveenminio/eos:log` | Latest | ~200MB |
| `trinodb/trino` | 477 | ~1.5GB |
| `python:3.11-slim` | 3.11 | ~150MB |

### 10.3 Python Libraries (Init Container)

| Library | Purpose |
|---------|---------|
| `boto3` | AWS SDK for SigV4 signing |
| `requests` | HTTP client |
| `kafka-python` | Kafka admin operations |
| `trino` | Trino DBAPI client for catalog creation |

### 10.4 Kafka Connect Plugin

| Plugin | Version | Source |
|--------|---------|--------|
| `iceberg-kafka-connect` | 0.6.19 | [GitHub](https://github.com/databricks/iceberg-kafka-connect) |

---

## 11. Configuration Reference

### 11.1 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_LICENSE` | Required | MinIO AIStor license key |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO API endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka bootstrap servers |
| `KAFKA_CONNECT_URL` | `http://kafka-connect:8083` | Kafka Connect REST URL |
| `WAREHOUSE_NAME` | `kafkawarehouse` | Iceberg warehouse name |
| `NAMESPACE_NAME` | `streaming` | Iceberg namespace |

### 11.2 Kafka Topic Configuration

| Topic | Partitions | Replication | Purpose |
|-------|------------|-------------|---------|
| `control-iceberg` | 1 | 1 | Commit coordination |
| `apilogs` | 3 | 1 | API log messages |
| `errorlogs` | 3 | 1 | Error log messages |
| `auditlogs` | 3 | 1 | Audit log messages |

### 11.3 Commit Settings

| Setting | Value | Description |
|---------|-------|-------------|
| Commit Interval | 10 seconds | How often to commit to Iceberg |
| Commit Timeout | 60 seconds | Max time to wait for commit |
| Tasks per Connector | 2 | Parallelism level |

---

## 12. Limitations & Considerations

### 12.1 Current Limitations

1. **Single-node Kafka:** Not production-ready (replication factor 1)
2. **License Required:** MinIO AIStor requires a valid license
3. **No TLS:** Inter-service communication is unencrypted
4. **No Authentication:** Kafka and Kafka Connect have no auth enabled

### 12.2 Production Recommendations

For production deployment:

1. **Kafka Cluster:** Deploy 3+ broker cluster with replication factor 3
2. **Enable TLS:** Configure SSL/TLS for all inter-service communication
3. **Authentication:** Enable SASL authentication for Kafka
4. **Secrets Management:** Use proper secrets management (not environment variables)
5. **Monitoring:** Add Prometheus metrics and alerting
6. **High Availability:** Deploy MinIO in distributed mode

---

## 13. Troubleshooting

### 13.1 Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Connector FAILED | MinIO not ready | Wait for MinIO health check, restart connector |
| No data in tables | Commit interval not elapsed | Wait 10+ seconds, check Kafka Connect logs |
| SigV4 auth errors | Wrong credentials | Verify ACCESS_KEY/SECRET_KEY match MinIO |
| Topic not found | Init container failed | Check init container logs, re-run |

### 13.2 Debug Commands

```bash
# Check all container logs
docker compose logs

# Check specific service
docker compose logs kafka-connect

# Enter container shell
docker exec -it kafka-connect bash

# Check Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

# Consume from topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic apilogs \
  --from-beginning
```

---

## 14. File Structure

```
kafka-connect/
├── docker-compose.yaml          # Service definitions
├── init-setup.py                # Initialization script
├── kafka-connect-iceberg-sink.sh # CLI management script
├── README.md                    # User documentation
├── DESIGN.md                    # This document
├── .env                         # Environment variables (MINIO_LICENSE)
├── .gitignore                   # Git ignore rules
└── plugins/                     # Kafka Connect plugins (auto-downloaded)
    └── iceberg-kafka-connect/   # Iceberg sink connector
```

---

## 15. Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | 2025-01 | Initial release with 3 log pipelines |
