# Kafka Connect Iceberg Sink - Design Documentation

## 1. Executive Summary

This project implements a **real-time streaming data pipeline** that captures MinIO object storage logs and persists them to Apache Iceberg tables using a **dual-cluster architecture**. The source MinIO cluster generates logs, and the destination MinIO cluster stores the Iceberg tables.

**Key Value Proposition:**
- Real-time visibility into MinIO operations (API calls, errors, audit events)
- Separation of concerns: log generation vs. data storage
- High availability with 4-node distributed MinIO clusters
- Queryable historical log data using standard SQL (Trino)
- Schema evolution support - automatically adapts to new log fields

---

## 2. Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA CONNECT ICEBERG SINK (Dual Cluster Architecture)              │
├───────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                        │
│  ┌─────────────────────────────┐              ┌─────────────────────────────┐         │
│  │  SOURCE MINIO CLUSTER       │              │  DESTINATION MINIO CLUSTER  │         │
│  │  (4 nodes + nginx LB)       │              │  (4 nodes + nginx LB)       │         │
│  │                             │              │                             │         │
│  │  ┌─────────────────────┐    │              │  ┌─────────────────────┐    │         │
│  │  │ minio-source1..4    │    │              │  │ minio-dest1..4      │    │         │
│  │  │                     │    │              │  │                     │    │         │
│  │  │ • Log Generation    │    │              │  │ • Iceberg Tables    │    │         │
│  │  │ • Kafka Targets     │    │              │  │ • REST Catalog      │    │         │
│  │  └─────────────────────┘    │              │  └─────────────────────┘    │         │
│  │           │                 │              │           ▲                 │         │
│  │           ▼                 │              │           │                 │         │
│  │  ┌─────────────────────┐    │              │  ┌─────────────────────┐    │         │
│  │  │ nginx-source        │    │              │  │ nginx-dest          │    │         │
│  │  │ :9000 (API)         │    │              │  │ :9010 (API)         │    │         │
│  │  │ :9001 (Console)     │    │              │  │ :9011 (Console)     │    │         │
│  │  └─────────────────────┘    │              │  └─────────────────────┘    │         │
│  └─────────────────────────────┘              └─────────────────────────────┘         │
│               │                                           ▲                           │
│               │                                           │                           │
│               ▼                                           │                           │
│  ┌─────────────────────────────┐              ┌───────────┴───────────────┐           │
│  │  KAFKA (KRaft Mode)         │              │  KAFKA CONNECT            │           │
│  │                             │              │                           │           │
│  │  Topics:                    │              │  Connectors:              │           │
│  │  • apilogs (3 partitions)   │─────────────▶│  • iceberg-sink-apilogs   │           │
│  │  • errorlogs (3 partitions) │              │  • iceberg-sink-errorlogs │           │
│  │  • auditlogs (3 partitions) │              │  • iceberg-sink-auditlogs │           │
│  │  • control-iceberg (1 part) │              │                           │           │
│  └─────────────────────────────┘              └───────────────────────────┘           │
│                                                           │                           │
│                                                           ▼                           │
│                                               ┌───────────────────────────┐           │
│                                               │  TRINO QUERY ENGINE       │           │
│                                               │  :9999 (Web UI / JDBC)    │           │
│                                               │                           │           │
│                                               │  • SQL Analytics          │           │
│                                               │  • Iceberg REST Catalog   │           │
│                                               └───────────────────────────┘           │
│                                                                                        │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
┌─────────────┐     ┌─────────┐     ┌───────────────┐     ┌──────────────┐     ┌─────────┐
│ Source      │     │         │     │               │     │ Destination  │     │         │
│ MinIO       │────▶│  Kafka  │────▶│ Kafka Connect │────▶│ MinIO        │────▶│  Trino  │
│ (Logs)      │     │         │     │ (Iceberg Sink)│     │ (Tables)     │     │ (Query) │
└─────────────┘     └─────────┘     └───────────────┘     └──────────────┘     └─────────┘
  nginx-source        :9092              :8083              nginx-dest          :9999
  :9000/:9001                                              :9010/:9011
```

---

## 3. Component Details

### 3.1 Source MinIO Cluster

| Property | Value |
|----------|-------|
| **Image** | `praveenminio/eos:log` (configurable via `MINIO_SOURCE_IMAGE`) |
| **Nodes** | 4 (`minio-source1` through `minio-source4`) |
| **Load Balancer** | nginx (`nginx-source`) |
| **Ports** | 9000 (API), 9001 (Console) |
| **Role** | Log generation with Kafka targets |

**Log Target Configuration:**
```yaml
MINIO_LOG_API_KAFKA_ENABLE: "on"
MINIO_LOG_API_KAFKA_BROKERS: kafka:29092
MINIO_LOG_API_KAFKA_TOPIC: apilogs

MINIO_LOG_ERROR_KAFKA_ENABLE: "on"
MINIO_LOG_ERROR_KAFKA_BROKERS: kafka:29092
MINIO_LOG_ERROR_KAFKA_TOPIC: errorlogs

MINIO_LOG_AUDIT_KAFKA_ENABLE: "on"
MINIO_LOG_AUDIT_KAFKA_BROKERS: kafka:29092
MINIO_LOG_AUDIT_KAFKA_TOPIC: auditlogs
```

### 3.2 Destination MinIO Cluster

| Property | Value |
|----------|-------|
| **Image** | `quay.io/minio/aistor/minio:EDGE.2025-12-13T08-46-12Z` (configurable via `MINIO_DEST_IMAGE`) |
| **Nodes** | 4 (`minio-dest1` through `minio-dest4`) |
| **Load Balancer** | nginx (`nginx-dest`) |
| **Ports** | 9010 (API), 9011 (Console) |
| **Role** | Iceberg tables storage with REST catalog |

### 3.3 Nginx Load Balancers

Each cluster has a dedicated nginx load balancer:

**nginx-source** (`nginx-source.conf`):
- Upstream: `minio-source1:9000` through `minio-source4:9000`
- Console uses `ip_hash` for session affinity
- External ports: 9000 (API), 9001 (Console)

**nginx-dest** (`nginx-dest.conf`):
- Upstream: `minio-dest1:9000` through `minio-dest4:9000`
- Console uses `ip_hash` for session affinity
- External ports: 9010 (API), 9011 (Console)

### 3.4 Kafka Broker (KRaft Mode)

| Property | Value |
|----------|-------|
| **Image** | `confluentinc/cp-kafka:7.5.0` |
| **Mode** | KRaft (Kafka Raft) - no Zookeeper |
| **Ports** | 9092 (external), 29092 (internal), 29093 (controller) |
| **Cluster ID** | `MkU3OEVBNTcwNTJENDM2Qk` |

### 3.5 Kafka Connect

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

### 3.6 Trino Query Engine

| Property | Value |
|----------|-------|
| **Image** | `trinodb/trino:477` |
| **Port** | 9999 (mapped from internal 8080) |
| **Catalog Management** | Dynamic |

**Trino Catalog Configuration (created automatically):**

| Parameter | Value |
|-----------|-------|
| `iceberg.catalog.type` | `rest` |
| `iceberg.rest-catalog.uri` | `http://nginx-dest:9000/_iceberg` |
| `iceberg.rest-catalog.warehouse` | `kafkawarehouse` |
| `iceberg.rest-catalog.security` | `SIGV4` |
| `iceberg.rest-catalog.signing-name` | `s3tables` |
| `s3.endpoint` | `http://nginx-dest:9000` |

### 3.7 Init Container

| Property | Value |
|----------|-------|
| **Image** | `python:3.11-slim` |
| **Purpose** | One-time initialization |
| **Script** | `init-setup.py` |

**Initialization Steps:**
1. Check source cluster health
2. Check destination cluster health
3. Create Kafka control topic (`control-iceberg`) - 1 partition
4. Create data topics (`apilogs`, `errorlogs`, `auditlogs`) - 3 partitions each
5. Create warehouse (`kafkawarehouse`) on destination cluster
6. Create namespace (`streaming`)
7. Create Iceberg tables with explicit schemas
8. Deploy 3 Iceberg sink connectors (pointing to destination cluster)
9. Create Trino dynamic catalog

---

## 4. Container Summary

| Container | Image | Ports | Purpose |
|-----------|-------|-------|---------|
| `kafka` | cp-kafka:7.5.0 | 9092, 9093 | Message broker |
| `kafka-connect` | cp-kafka-connect:7.5.0 | 8083 | Iceberg sink connectors |
| `minio-source1` | praveenminio/eos:log | (internal) | Source node 1 |
| `minio-source2` | praveenminio/eos:log | (internal) | Source node 2 |
| `minio-source3` | praveenminio/eos:log | (internal) | Source node 3 |
| `minio-source4` | praveenminio/eos:log | (internal) | Source node 4 |
| `nginx-source` | nginx:alpine | 9000, 9001 | Source load balancer |
| `minio-dest1` | aistor/minio:EDGE | (internal) | Destination node 1 |
| `minio-dest2` | aistor/minio:EDGE | (internal) | Destination node 2 |
| `minio-dest3` | aistor/minio:EDGE | (internal) | Destination node 3 |
| `minio-dest4` | aistor/minio:EDGE | (internal) | Destination node 4 |
| `nginx-dest` | nginx:alpine | 9010, 9011 | Destination load balancer |
| `trino` | trinodb/trino:477 | 9999 | Query engine |
| `kafka-iceberg-init` | python:3.11-slim | - | Initialization |

**Total: 14 containers**

---

## 5. Data Schemas & Partitioning

Tables are created with explicit Iceberg schemas based on MinIO's `madmin-go/log` package structs.

### 5.1 API Log Schema (`streaming.apilogs`)

**Source:** `madmin-go/log/api.go` - `API` and `CallInfo` structs

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Log format version |
| `time` | timestamptz | Event timestamp (required) |
| `node` | string | MinIO node identifier |
| `origin` | string | Request origin |
| `type` | string | API type |
| `name` | string | API name (e.g., s3.GetObject) |
| `bucket` | string | Target bucket name |
| `object` | string | Target object key |
| `versionId` | string | Object version ID |
| `tags` | map<string, string> | Custom tags |
| `callInfo` | struct | Nested call details |

**Partitioning:** `day(time), identity(bucket)`

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
| `trace` | struct | Nested trace details |

**Partitioning:** `day(time)`

### 5.3 Audit Log Schema (`streaming.auditlogs`)

**Source:** `madmin-go/log/audit.go` - `Audit` struct

| Field | Type | Description |
|-------|------|-------------|
| `version` | string | Log format version |
| `time` | timestamptz | Event timestamp (required) |
| `node` | string | MinIO node identifier |
| `apiName` | string | Admin API name |
| `category` | string | Audit category |
| `action` | string | Action performed |
| `bucket` | string | Target bucket |
| `tags` | map<string, string> | Custom tags |
| `requestID` | string | Unique request identifier |
| `requestClaims` | map<string, string> | JWT claims |
| `sourceHost` | string | Client IP address |
| `accessKey` | string | Access key used |
| `parentUser` | string | Parent user |
| `details` | string | JSON string with category-specific details |

**Partitioning:** `day(time), identity(category)`

---

## 6. Connector Configuration

Each log type has a dedicated Iceberg sink connector pointing to the **destination cluster**:

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
  "iceberg.catalog.uri": "http://nginx-dest:9000/_iceberg",
  "iceberg.catalog.warehouse": "kafkawarehouse",

  "iceberg.catalog.rest.sigv4-enabled": "true",
  "iceberg.catalog.rest.signing-name": "s3tables",
  "iceberg.catalog.rest.signing-region": "us-east-1",

  "iceberg.catalog.s3.endpoint": "http://nginx-dest:9000",
  "iceberg.catalog.s3.path-style-access": "true",

  "iceberg.control.topic": "control-iceberg",
  "iceberg.control.commit.interval-ms": "10000",
  "iceberg.control.commit.timeout-ms": "60000"
}
```

---

## 7. Network Architecture

All services communicate via Docker bridge network (`kafka-iceberg`):

| Service | Internal Access | External Access |
|---------|-----------------|-----------------|
| Kafka | `kafka:29092` | `localhost:9092` |
| Kafka Connect | `kafka-connect:8083` | `localhost:8083` |
| Source MinIO | `nginx-source:9000` | `localhost:9000` |
| Source Console | `nginx-source:9001` | `localhost:9001` |
| Destination MinIO | `nginx-dest:9000` | `localhost:9010` |
| Destination Console | `nginx-dest:9001` | `localhost:9011` |
| Trino | `trino:8080` | `localhost:9999` |

---

## 8. Storage Architecture

### 8.1 Docker Volumes

| Volume | Container | Purpose |
|--------|-----------|---------|
| `minio-source1-data` | minio-source1 | Source node 1 data |
| `minio-source2-data` | minio-source2 | Source node 2 data |
| `minio-source3-data` | minio-source3 | Source node 3 data |
| `minio-source4-data` | minio-source4 | Source node 4 data |
| `minio-dest1-data` | minio-dest1 | Destination node 1 data |
| `minio-dest2-data` | minio-dest2 | Destination node 2 data |
| `minio-dest3-data` | minio-dest3 | Destination node 3 data |
| `minio-dest4-data` | minio-dest4 | Destination node 4 data |

### 8.2 Warehouse Structure (Destination Cluster)

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

---

## 9. Configuration Reference

### 9.1 Environment Variables (.env)

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_LICENSE` | Required | MinIO AIStor license key |
| `MINIO_SOURCE_IMAGE` | `praveenminio/eos:log` | Source cluster image |
| `MINIO_DEST_IMAGE` | `quay.io/minio/aistor/...` | Destination cluster image |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO credentials |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO credentials |
| `KAFKA_IMAGE` | `confluentinc/cp-kafka:7.5.0` | Kafka image |
| `KAFKA_CONNECT_IMAGE` | `confluentinc/cp-kafka-connect:7.5.0` | Kafka Connect image |
| `TRINO_IMAGE` | `trinodb/trino:477` | Trino image |
| `WAREHOUSE_NAME` | `kafkawarehouse` | Iceberg warehouse name |
| `NAMESPACE_NAME` | `streaming` | Iceberg namespace |
| `API_LOGS_TOPIC` | `apilogs` | API logs topic name |
| `ERROR_LOGS_TOPIC` | `errorlogs` | Error logs topic name |
| `AUDIT_LOGS_TOPIC` | `auditlogs` | Audit logs topic name |
| `SOURCE_API_PORT` | `9000` | Source cluster API port |
| `SOURCE_CONSOLE_PORT` | `9001` | Source cluster console port |
| `DEST_API_PORT` | `9010` | Destination cluster API port |
| `DEST_CONSOLE_PORT` | `9011` | Destination cluster console port |
| `KAFKA_EXTERNAL_PORT` | `9092` | Kafka external port |
| `KAFKA_CONNECT_PORT` | `8083` | Kafka Connect port |
| `TRINO_PORT` | `9999` | Trino port |

### 9.2 Kafka Topic Configuration

| Topic | Partitions | Replication | Purpose |
|-------|------------|-------------|---------|
| `control-iceberg` | 1 | 1 | Commit coordination |
| `apilogs` | 3 | 1 | API log messages |
| `errorlogs` | 3 | 1 | Error log messages |
| `auditlogs` | 3 | 1 | Audit log messages |

---

## 10. Operational Commands

### 10.1 Service Management

```bash
# Start all services
./kafka-connect-iceberg-sink.sh start

# Stop all services
./kafka-connect-iceberg-sink.sh stop

# View status
./kafka-connect-iceberg-sink.sh status

# View logs
./kafka-connect-iceberg-sink.sh logs -f

# Clean up
./kafka-connect-iceberg-sink.sh clean
```

### 10.2 Cluster Health Checks

```bash
# Source cluster
curl http://localhost:9000/minio/health/live

# Destination cluster
curl http://localhost:9010/minio/health/live
```

### 10.3 Connector Management

```bash
# Check connector status
curl -s http://localhost:8083/connectors/iceberg-sink-apilogs/status | jq

# Restart connector
curl -X POST http://localhost:8083/connectors/iceberg-sink-apilogs/restart

# List all connectors
curl -s http://localhost:8083/connectors | jq
```

### 10.4 Trino Queries

```bash
# Connect to Trino CLI
docker exec -it trino trino

# Access Trino Web UI
open http://localhost:9999
```

**Example SQL Queries:**

```sql
-- List catalogs
SHOW CATALOGS;

-- List tables
SHOW TABLES FROM kafkawarehouse.streaming;

-- Query API logs
SELECT * FROM kafkawarehouse.streaming.apilogs LIMIT 10;

-- Query with nested fields
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
```

---

## 11. File Structure

```
kafka-connect/
├── docker-compose.yaml          # Service definitions (14 containers)
├── init-setup.py                # Initialization script
├── kafka-connect-iceberg-sink.sh # CLI management script
├── nginx-source.conf            # Source cluster nginx config
├── nginx-dest.conf              # Destination cluster nginx config
├── .env.sample                  # Sample environment configuration
├── .env                         # Environment variables (gitignored)
├── .gitignore                   # Git ignore rules
├── README.md                    # User documentation
├── DESIGN.md                    # This document
└── plugins/                     # Kafka Connect plugins (auto-downloaded)
    └── iceberg-kafka-connect/   # Iceberg sink connector
```

---

## 12. Limitations & Considerations

### 12.1 Current Limitations

1. **Single-node Kafka:** Not production-ready (replication factor 1)
2. **License Required:** MinIO AIStor requires a valid license
3. **No TLS:** Inter-service communication is unencrypted
4. **No Authentication:** Kafka and Kafka Connect have no auth enabled

### 12.2 Production Recommendations

1. **Kafka Cluster:** Deploy 3+ broker cluster with replication factor 3
2. **Enable TLS:** Configure SSL/TLS for all inter-service communication
3. **Authentication:** Enable SASL authentication for Kafka
4. **Secrets Management:** Use proper secrets management (not environment variables)
5. **Monitoring:** Add Prometheus metrics and alerting
6. **Distributed MinIO:** Already using 4-node clusters for HA

---

## 13. Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.0.0 | 2025-01 | Dual-cluster architecture with nginx load balancers |
| 1.0.0 | 2025-01 | Initial release with single MinIO node |
