# Kafka Connect Iceberg Sink to MinIO AIStor

Stream MinIO logs (API, Error, Audit) to Apache Iceberg tables via Kafka Connect.

## Overview

This project provides a complete setup for streaming MinIO server logs to Iceberg tables stored on MinIO AIStor. It includes:

- **Kafka** (KRaft mode - no Zookeeper required)
- **Kafka Connect** with three Iceberg Sink connectors
- **MinIO AIStor** with Tables support (REST catalog)
- **Automatic initialization** (topics, warehouse, namespace, connector deployment)

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

1. **Set your MinIO license**:
   ```bash
   # Option 1: Export directly
   export MINIO_LICENSE="your-license-key"

   # Option 2: Create a .env file
   echo 'MINIO_LICENSE=your-license-key' > .env
   ```

2. **Start all services**:
   ```bash
   ./kafka-connect-iceberg-sink.sh start
   ```

   This will:
   - Download the Iceberg Kafka Connect plugin (first run only)
   - Start Kafka, MinIO AIStor, and Kafka Connect
   - Create the Kafka topics (`apilogs`, `errorlogs`, `auditlogs`, `control-iceberg`)
   - Create the warehouse (`kafkawarehouse`) and namespace (`streaming`)
   - Deploy three Iceberg sink connectors (one per log type)

3. **Generate logs** by interacting with MinIO:
   ```bash
   # Create a bucket
   docker exec minio mc mb local/testbucket

   # Upload a file
   echo 'hello' | docker exec -i minio mc pipe local/testbucket/test.txt

   # List objects
   docker exec minio mc ls local/testbucket
   ```

4. **View logs from Kafka topics**:
   ```bash
   # View API logs
   ./kafka-connect-iceberg-sink.sh produce apilogs

   # View error logs
   ./kafka-connect-iceberg-sink.sh produce errorlogs

   # View audit logs
   ./kafka-connect-iceberg-sink.sh produce auditlogs
   ```

5. **Query the Iceberg tables**:
   ```bash
   # Query all tables
   ./kafka-connect-iceberg-sink.sh query

   # Query a specific table
   ./kafka-connect-iceberg-sink.sh query apilogs
   ./kafka-connect-iceberg-sink.sh query errorlogs
   ./kafka-connect-iceberg-sink.sh query auditlogs
   ```

## Commands

| Command | Description |
|---------|-------------|
| `./kafka-connect-iceberg-sink.sh start` | Start all services |
| `./kafka-connect-iceberg-sink.sh stop` | Stop all services |
| `./kafka-connect-iceberg-sink.sh status` | Show status of services and connectors |
| `./kafka-connect-iceberg-sink.sh logs [-f]` | Show logs (add `-f` to follow) |
| `./kafka-connect-iceberg-sink.sh produce` | Show how to generate logs |
| `./kafka-connect-iceberg-sink.sh produce <topic>` | Consume messages from a topic (apilogs, errorlogs, auditlogs) |
| `./kafka-connect-iceberg-sink.sh query` | Query all Iceberg tables |
| `./kafka-connect-iceberg-sink.sh query <table>` | Query a specific table (apilogs, errorlogs, auditlogs) |
| `./kafka-connect-iceberg-sink.sh restart` | Restart all services |
| `./kafka-connect-iceberg-sink.sh clean` | Stop and remove all data/volumes |

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka | `localhost:9092` | - |
| Kafka Connect REST API | `http://localhost:8083` | - |
| MinIO Console | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| MinIO API | `http://localhost:9000` | `minioadmin` / `minioadmin` |

## Configuration

| Item | Value |
|------|-------|
| Warehouse | `kafkawarehouse` |
| Namespace | `streaming` |
| Control Topic | `control-iceberg` |
| Commit Interval | 10 seconds |

### Connectors

| Connector | Topic | Table |
|-----------|-------|-------|
| `iceberg-sink-apilogs` | `apilogs` | `streaming.apilogs` |
| `iceberg-sink-errorlogs` | `errorlogs` | `streaming.errorlogs` |
| `iceberg-sink-auditlogs` | `auditlogs` | `streaming.auditlogs` |

## Architecture

```
                                                    ┌─────────────────────┐
┌─────────────────┐                                 │  MinIO AIStor       │
│                 │     ┌─────────────────────┐     │  (Iceberg Tables)   │
│  MinIO Server   │     │                     │     ├─────────────────────┤
│                 │     │                     │     │ streaming.apilogs   │
│  ┌───────────┐  │     │   Kafka Connect     │────▶│ streaming.errorlogs │
│  │ API Logs  │──┼────▶│   (3 Iceberg Sinks) │     │ streaming.auditlogs │
│  │Error Logs │  │     │                     │     │                     │
│  │Audit Logs │  │     │                     │     └─────────────────────┘
│  └───────────┘  │     └─────────────────────┘
│                 │                │
└─────────────────┘                ▼
        │               ┌─────────────────┐
        │               │ Control Topic   │
        ▼               │ (coordination)  │
┌─────────────────┐     └─────────────────┘
│  Kafka Topics   │
├─────────────────┤
│ apilogs         │
│ errorlogs       │
│ auditlogs       │
└─────────────────┘
```

## Files

- `docker-compose.yaml` - Docker Compose configuration for all services
- `init-setup.py` - Initialization script (creates topics, warehouse, deploys connector)
- `kafka-connect-iceberg-sink.sh` - Main CLI script for managing the stack
- `plugins/` - Kafka Connect plugins (auto-downloaded on first run)

## Generating Logs

Logs are automatically generated when MinIO handles requests. Interact with MinIO to produce logs:

```bash
# Create a bucket
docker exec minio mc mb local/testbucket

# Upload a file
echo 'hello world' | docker exec -i minio mc pipe local/testbucket/test.txt

# List objects
docker exec minio mc ls local/testbucket

# Download a file
docker exec minio mc cat local/testbucket/test.txt

# Delete objects
docker exec minio mc rm local/testbucket/test.txt
```

The connector supports schema evolution - new fields will be automatically added to the Iceberg tables.

## Connector Configuration

The Iceberg sink connector is configured with:

- **Auto-create tables**: Tables are created automatically from incoming data
- **Schema evolution**: New columns are added automatically
- **REST catalog**: Uses MinIO AIStor's Iceberg REST API
- **SigV4 authentication**: Secure authentication to MinIO

See `init-setup.py` for the full connector configuration.

## Troubleshooting

**Check connector status**:
```bash
# Check all connectors
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
curl -X POST http://localhost:8083/connectors/iceberg-sink-errorlogs/restart
curl -X POST http://localhost:8083/connectors/iceberg-sink-auditlogs/restart
```

**Check if data files exist in MinIO**:
Navigate to http://localhost:9001 and browse to:
- `kafkawarehouse/streaming/apilogs/`
- `kafkawarehouse/streaming/errorlogs/`
- `kafkawarehouse/streaming/auditlogs/`

## License

This project requires a valid MinIO AIStor license for the MinIO server component.
