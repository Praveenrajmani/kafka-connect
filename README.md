# Kafka Connect Iceberg Sink to MinIO AIStor

Stream MinIO logs (API, Error, Audit) to Apache Iceberg tables via Kafka Connect using a dual-cluster architecture.

## Overview

This project provides a complete setup for streaming MinIO server logs to Iceberg tables stored on a separate MinIO AIStor cluster. It includes:

- **Two 4-node MinIO clusters** with nginx load balancers
  - **Source Cluster**: Generates logs and sends them to Kafka
  - **Destination Cluster**: Stores Iceberg tables with log data
- **Kafka** (KRaft mode - no Zookeeper required)
- **Kafka Connect** with three Iceberg Sink connectors
- **Trino** for querying Iceberg tables
- **Automatic initialization** (topics, warehouse, namespace, connector deployment)

### Architecture

```
Source MinIO (4-node)  →  Kafka  →  Kafka Connect  →  Destination MinIO (4-node)
        ↓                                                        ↑
   nginx-source                                             nginx-dest
   :9000/:9001                                             :9010/:9011
```

### Log Pipelines

| Topic | Iceberg Table | Description |
|-------|---------------|-------------|
| `apilogs` | `streaming.apilogs` | MinIO API request/response logs |
| `errorlogs` | `streaming.errorlogs` | MinIO error logs |
| `auditlogs` | `streaming.auditlogs` | MinIO audit trail logs |

## Prerequisites

- Docker and Docker Compose
- MinIO AIStor license (`MINIO_LICENSE` environment variable)
- `curl`, `jq`, and `unzip` (for plugin download)

## Quick Start

1. **Configure environment**:
   ```bash
   # Copy the sample environment file
   cp .env.sample .env

   # Edit .env and add your MinIO license key
   vim .env
   ```

2. **Start all services**:
   ```bash
   ./kafka-connect-iceberg-sink.sh start
   ```

   This will:
   - Download the Iceberg Kafka Connect plugin (first run only)
   - Start two 4-node MinIO clusters with nginx load balancers
   - Start Kafka and Kafka Connect
   - Create the Kafka topics (`apilogs`, `errorlogs`, `auditlogs`, `control-iceberg`)
   - Create the warehouse (`kafkawarehouse`) and namespace (`streaming`) on destination cluster
   - Deploy three Iceberg sink connectors (one per log type)
   - Create Trino catalog for querying

3. **Generate logs** by interacting with the source MinIO cluster:
   ```bash
   # Using mc client
   mc alias set source http://localhost:9000 minioadmin minioadmin

   # Create a bucket
   mc mb source/testbucket

   # Upload a file
   mc cp /etc/hosts source/testbucket/test.txt

   # List objects
   mc ls source/testbucket
   ```

4. **View logs from Kafka topics**:
   ```bash
   # View API logs
   docker exec kafka kafka-console-consumer \
     --bootstrap-server localhost:29092 \
     --topic apilogs \
     --from-beginning
   ```

5. **Query the Iceberg tables with Trino**:
   ```bash
   # Connect to Trino CLI
   docker exec -it trino trino

   # Example queries
   SHOW CATALOGS;
   SHOW SCHEMAS FROM kafkawarehouse;
   SHOW TABLES FROM kafkawarehouse.streaming;

   SELECT * FROM kafkawarehouse.streaming.apilogs LIMIT 10;
   ```

## Commands

| Command | Description |
|---------|-------------|
| `./kafka-connect-iceberg-sink.sh start` | Start all services |
| `./kafka-connect-iceberg-sink.sh stop` | Stop all services |
| `./kafka-connect-iceberg-sink.sh status` | Show status of services and connectors |
| `./kafka-connect-iceberg-sink.sh logs [-f]` | Show logs (add `-f` to follow) |
| `./kafka-connect-iceberg-sink.sh generate <type>` | Generate logs on source cluster |
| `./kafka-connect-iceberg-sink.sh query [table]` | Query Iceberg tables with PyIceberg |
| `./kafka-connect-iceberg-sink.sh restart` | Restart all services |
| `./kafka-connect-iceberg-sink.sh clean` | Stop and remove all data/volumes |

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka | `localhost:9092` | - |
| Kafka Connect REST API | `http://localhost:8083` | - |
| Source MinIO Console | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| Source MinIO API | `http://localhost:9000` | `minioadmin` / `minioadmin` |
| Destination MinIO Console | `http://localhost:9011` | `minioadmin` / `minioadmin` |
| Destination MinIO API | `http://localhost:9010` | `minioadmin` / `minioadmin` |
| Trino Web UI | `http://localhost:9999` | - |

## Configuration

Configuration is managed via the `.env` file. Copy `.env.sample` to `.env` and customize as needed.

### Key Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_LICENSE` | Required | MinIO AIStor license key |
| `MINIO_SOURCE_IMAGE` | `praveenminio/eos:log` | Source cluster image (with Kafka log targets) |
| `MINIO_DEST_IMAGE` | `quay.io/minio/aistor/minio:EDGE...` | Destination cluster image (with Iceberg) |
| `MINIO_ROOT_USER` | `minioadmin` | MinIO credentials |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO credentials |
| `WAREHOUSE_NAME` | `kafkawarehouse` | Iceberg warehouse name |
| `NAMESPACE_NAME` | `streaming` | Iceberg namespace |

### Connectors

| Connector | Topic | Table |
|-----------|-------|-------|
| `iceberg-sink-apilogs` | `apilogs` | `streaming.apilogs` |
| `iceberg-sink-errorlogs` | `errorlogs` | `streaming.errorlogs` |
| `iceberg-sink-auditlogs` | `auditlogs` | `streaming.auditlogs` |

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA CONNECT ICEBERG SINK                                │
│                     (Dual Cluster Architecture)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────────┐                    ┌─────────────────────┐         │
│  │  Source MinIO       │                    │  Destination MinIO  │         │
│  │  (4-node cluster)   │                    │  (4-node cluster)   │         │
│  │                     │                    │                     │         │
│  │  ┌───────────┐      │                    │  ┌───────────────┐  │         │
│  │  │ API Logs  │──────┼──┐                 │  │ apilogs       │  │         │
│  │  └───────────┘      │  │                 │  └───────────────┘  │         │
│  │  ┌───────────┐      │  │   ┌──────────┐  │  ┌───────────────┐  │         │
│  │  │Error Logs │──────┼──┼──▶│  Kafka   │──┼─▶│ errorlogs     │  │         │
│  │  └───────────┘      │  │   │ Connect  │  │  └───────────────┘  │         │
│  │  ┌───────────┐      │  │   └──────────┘  │  ┌───────────────┐  │         │
│  │  │Audit Logs │──────┼──┘                 │  │ auditlogs     │  │         │
│  │  └───────────┘      │                    │  └───────────────┘  │         │
│  │                     │                    │                     │         │
│  │  nginx-source       │                    │  nginx-dest         │         │
│  │  :9000 / :9001      │                    │  :9010 / :9011      │         │
│  └─────────────────────┘                    └─────────────────────┘         │
│           │                                           ▲                     │
│           ▼                                           │                     │
│  ┌─────────────────┐                         ┌────────┴────────┐            │
│  │  Kafka Topics   │                         │  Trino Query    │            │
│  │  - apilogs      │                         │  Engine         │            │
│  │  - errorlogs    │                         │  :9999          │            │
│  │  - auditlogs    │                         └─────────────────┘            │
│  └─────────────────┘                                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Files

| File | Description |
|------|-------------|
| `docker-compose.yaml` | Docker Compose configuration for all services |
| `init-setup.py` | Initialization script (creates topics, warehouse, deploys connectors) |
| `kafka-connect-iceberg-sink.sh` | Main CLI script for managing the stack |
| `nginx-source.conf` | Nginx config for source cluster load balancer |
| `nginx-dest.conf` | Nginx config for destination cluster load balancer |
| `.env.sample` | Sample environment configuration |
| `plugins/` | Kafka Connect plugins (auto-downloaded on first run) |
| `generate/` | Log generation scripts for testing |

## Generating Logs

Use the built-in log generation scripts to create realistic log data on the source cluster. Logs automatically flow through Kafka to Iceberg tables on the destination cluster.

### Using the Generate Command

```bash
# Generate API logs (S3 operations like GET, PUT, LIST, DELETE)
./kafka-connect-iceberg-sink.sh generate api

# Generate error logs (IAM errors, config errors, replication failures)
./kafka-connect-iceberg-sink.sh generate error

# Generate audit logs (admin operations, policy changes, IAM events)
./kafka-connect-iceberg-sink.sh generate audit

# Generate all log types at once
./kafka-connect-iceberg-sink.sh generate all

# Specify number of operations (default: 100)
./kafka-connect-iceberg-sink.sh generate api --count 500
./kafka-connect-iceberg-sink.sh generate all --count 50
```

### Log Types

| Type | Description | Operations |
|------|-------------|------------|
| `api` | API logs | ListObjects, GetObject, PutObject, DeleteObject, HeadObject, etc. |
| `error` | Error logs | IAM failures, config errors, replication errors, healing errors |
| `audit` | Audit logs | User management, policy changes, service account ops, config changes |
| `all` | All types | Generates all three log types |

### Manual Log Generation

You can also interact directly with the source MinIO cluster:

```bash
# Setup mc alias for source cluster
mc alias set source http://localhost:9000 minioadmin minioadmin

# Create a bucket
mc mb source/testbucket

# Upload a file
mc cp README.md source/testbucket/

# List objects
mc ls source/testbucket

# Download a file
mc cat source/testbucket/README.md

# Delete objects
mc rm source/testbucket/README.md
```

The connector supports schema evolution - new fields will be automatically added to the Iceberg tables.

## Querying with Trino

```bash
# Connect to Trino CLI
docker exec -it trino trino

# Or access the Web UI
open http://localhost:9999
```

**Example Queries:**

```sql
-- List catalogs
SHOW CATALOGS;

-- List tables
SHOW TABLES FROM kafkawarehouse.streaming;

-- Query API logs
SELECT * FROM kafkawarehouse.streaming.apilogs LIMIT 10;

-- Query API logs by bucket with timing info
SELECT
    bucket,
    name,
    time,
    "callInfo"."httpStatusCode" as status,
    "callInfo"."timeToFirstByte" as ttfb
FROM kafkawarehouse.streaming.apilogs
WHERE bucket IS NOT NULL
ORDER BY time DESC
LIMIT 20;

-- Count requests by API
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
```

## Troubleshooting

**Check connector status**:
```bash
curl -s http://localhost:8083/connectors/iceberg-sink-apilogs/status | jq
curl -s http://localhost:8083/connectors/iceberg-sink-errorlogs/status | jq
curl -s http://localhost:8083/connectors/iceberg-sink-auditlogs/status | jq
```

**View connector logs**:
```bash
./kafka-connect-iceberg-sink.sh logs kafka-connect
```

**Restart a connector**:
```bash
curl -X POST http://localhost:8083/connectors/iceberg-sink-apilogs/restart
```

**Check if data files exist in destination MinIO**:
Navigate to http://localhost:9011 and browse to:
- `kafkawarehouse/streaming/apilogs/`
- `kafkawarehouse/streaming/errorlogs/`
- `kafkawarehouse/streaming/auditlogs/`

**Check cluster health**:
```bash
# Source cluster
curl http://localhost:9000/minio/health/live

# Destination cluster
curl http://localhost:9010/minio/health/live
```

## License

This project requires a valid MinIO AIStor license for the MinIO server components.
