# Kafka Connect Iceberg Sink to MinIO AIStor

Stream data from Kafka topics to Apache Iceberg tables on MinIO AIStor using Kafka Connect.

## Overview

This project provides a complete setup for streaming data from Kafka to Iceberg tables stored on MinIO AIStor. It includes:

- **Kafka** (KRaft mode - no Zookeeper required)
- **Kafka Connect** with the Iceberg Sink connector
- **MinIO AIStor** with Tables support (REST catalog)
- **Automatic initialization** (topics, warehouse, namespace, connector deployment)

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
   - Create the Kafka topics (`events`, `control-iceberg`)
   - Create the warehouse (`kafkawarehouse`) and namespace (`streaming`)
   - Deploy the Iceberg sink connector

3. **Send test messages**:
   ```bash
   ./kafka-connect-iceberg-sink.sh produce
   ```

4. **Query the Iceberg table**:
   ```bash
   ./kafka-connect-iceberg-sink.sh query
   ```

## Commands

| Command | Description |
|---------|-------------|
| `./kafka-connect-iceberg-sink.sh start` | Start all services |
| `./kafka-connect-iceberg-sink.sh stop` | Stop all services |
| `./kafka-connect-iceberg-sink.sh status` | Show status of services and connectors |
| `./kafka-connect-iceberg-sink.sh logs [-f]` | Show logs (add `-f` to follow) |
| `./kafka-connect-iceberg-sink.sh produce` | Send sample messages to Kafka |
| `./kafka-connect-iceberg-sink.sh query` | Query the Iceberg table using PyIceberg |
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
| Table | `events` |
| Data Topic | `events` |
| Control Topic | `control-iceberg` |
| Commit Interval | 10 seconds |

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────┐
│                 │     │                 │     │                     │
│  Kafka Topic    │────▶│  Kafka Connect  │────▶│  MinIO AIStor       │
│  (events)       │     │  (Iceberg Sink) │     │  (Iceberg Tables)   │
│                 │     │                 │     │                     │
└─────────────────┘     └─────────────────┘     └─────────────────────┘
                               │
                               ▼
                        ┌─────────────────┐
                        │ Control Topic   │
                        │ (coordination)  │
                        └─────────────────┘
```

## Files

- `docker-compose.yaml` - Docker Compose configuration for all services
- `init-setup.py` - Initialization script (creates topics, warehouse, deploys connector)
- `kafka-connect-iceberg-sink.sh` - Main CLI script for managing the stack
- `plugins/` - Kafka Connect plugins (auto-downloaded on first run)

## Producing Messages

Messages should be JSON objects sent to the `events` topic:

```bash
echo '{"event_id": "evt-001", "event_type": "click", "user_id": 123, "timestamp": "2024-12-24T10:00:00Z"}' | \
  docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic events
```

The connector supports schema evolution - new fields will be automatically added to the Iceberg table.

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
curl -s http://localhost:8083/connectors/iceberg-sink/status | jq
```

**View connector logs**:
```bash
./kafka-connect-iceberg-sink.sh logs kafka-connect
```

**Restart connector**:
```bash
curl -X POST http://localhost:8083/connectors/iceberg-sink/restart
```

**Check if data files exist in MinIO**:
Navigate to http://localhost:9001 and browse to `kafkawarehouse/streaming/events/`

## License

This project requires a valid MinIO AIStor license for the MinIO server component.
