# Kafka Connect Iceberg Sink - Dual MinIO Cluster Setup

Stream MinIO API access logs from a source cluster to Apache Iceberg tables on a destination cluster via Kafka.

## Architecture

```
┌────────────────────────────────────────┐     ┌────────────────────────────────────────┐
│         SOURCE MINIO CLUSTER           │     │       DESTINATION MINIO CLUSTER        │
│    (Kafka Log Targets - PR #2463)      │     │      (Iceberg Tables / REST Catalog)   │
│                                        │     │                                        │
│   ┌──────────┐       ┌──────────┐      │     │   ┌──────────┐       ┌──────────┐      │
│   │minio-src │       │minio-src │      │     │   │minio-dst │       │minio-dst │      │
│   │    1     │       │    2     │      │     │   │    1     │       │    2     │      │
│   └────┬─────┘       └────┬─────┘      │     │   └────┬─────┘       └────┬─────┘      │
│        │                  │            │     │        │                  │            │
│   ┌────┴─────┐       ┌────┴─────┐      │     │   ┌────┴─────┐       ┌────┴─────┐      │
│   │minio-src │       │minio-src │      │     │   │minio-dst │       │minio-dst │      │
│   │    3     │       │    4     │      │     │   │    3     │       │    4     │      │
│   └──────────┴───┬───┴──────────┘      │     │   └──────────┴───┬───┴──────────┘      │
│                  │                     │     │                  │                     │
│           ┌──────┴──────┐              │     │           ┌──────┴──────┐              │
│           │nginx-source │              │     │           │ nginx-dest  │              │
│           │ :9000/:9001 │              │     │           │ :9010/:9011 │              │
│           └──────┬──────┘              │     │           └──────▲──────┘              │
│                  │                     │     │                  │                     │
└──────────────────┼─────────────────────┘     └──────────────────┼─────────────────────┘
                   │                                              │
                   │ MINIO_LOG_API_KAFKA_*                        │
                   │ (API access logs)                            │
                   ▼                                              │
┌─────────────────────────────────────────────────────────────────┴─────────────────────┐
│                                                                                       │
│                                      KAFKA                                            │
│                                  localhost:9092                                       │
│                                        │                                              │
│                              ┌─────────┴─────────┐                                    │
│                              │   Kafka Connect   │                                    │
│                              │   localhost:8083  │                                    │
│                              │                   │                                    │
│                              │  ┌─────────────┐  │                                    │
│                              │  │Iceberg Sink │──┼────────────────────────────────────┘
│                              │  │ Connector   │  │
│                              │  └─────────────┘  │
│                              └───────────────────┘
│                                                                                       │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

## Components

| Component | Image | Description |
|-----------|-------|-------------|
| Source MinIO (4 nodes) | `praveenminio/kafka-connect:v1.0.0` | MinIO with Kafka log targets (PR #2463) |
| Destination MinIO (4 nodes) | `quay.io/minio/aistor/minio:EDGE.2025-12-13T08-46-12Z` | MinIO with Iceberg Tables support |
| Nginx (source) | `nginx:1.25-alpine` | Load balancer for source cluster |
| Nginx (dest) | `nginx:1.25-alpine` | Load balancer for destination cluster |
| Kafka | `confluentinc/cp-kafka:7.5.0` | Message broker (KRaft mode) |
| Kafka Connect | `confluentinc/cp-kafka-connect:7.5.0` | Runs Iceberg sink connector |
| Init Container | `python:3.11-slim` | Sets up topics, warehouse, and connector |

## Data Flow

1. **Source MinIO** generates API access logs for every S3 operation
2. Logs are sent to **Kafka** `events` topic via `MINIO_LOG_API_KAFKA_*` configuration
3. **Kafka Connect** consumes from `events` topic using the Iceberg Sink Connector
4. Data is written to **Apache Iceberg** tables on **Destination MinIO**
5. Iceberg REST Catalog API is built into Destination MinIO (Tables feature)

## Prerequisites

- Docker and Docker Compose
- MinIO AIStor License (required for both clusters)
- ~8GB RAM available for containers

## Quick Start

### 1. Configure License

Copy the sample environment file and add your MinIO license:

```bash
# Copy sample config to .env
cp .env.sample .env

# Edit and add your license key
vi .env

# Set MINIO_LICENSE=<your-license-key>
```

### 2. Start Services

```bash
./kafka-connect-iceberg-sink.sh start
```

This will:
- Download the Iceberg Kafka Connect plugin (if not present)
- Start both 4-node MinIO clusters
- Start Nginx load balancers
- Start Kafka and Kafka Connect
- Create Kafka topics (`events`, `control-iceberg`)
- Create Iceberg warehouse and namespace on destination
- Deploy the Iceberg sink connector

### 3. Generate Logs

Interact with the source MinIO to generate API logs:

```bash
./kafka-connect-iceberg-sink.sh generate-logs
```

Or manually using AWS CLI:

```bash
# Configure AWS CLI for source MinIO
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

# List buckets (generates log)
aws --endpoint-url http://localhost:9000 s3 ls

# Upload a file (generates log)
echo "test data" | aws --endpoint-url http://localhost:9000 s3 cp - s3://test-logs-bucket/myfile.txt
```

### 4. Query Iceberg Table

Query the Iceberg table on the destination cluster:

```bash
./kafka-connect-iceberg-sink.sh query
```

### 5. View Consoles

- **Source MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Destination MinIO Console**: http://localhost:9011 (minioadmin/minioadmin)

## Commands

| Command | Description |
|---------|-------------|
| `./kafka-connect-iceberg-sink.sh start` | Start all services |
| `./kafka-connect-iceberg-sink.sh stop` | Stop all services |
| `./kafka-connect-iceberg-sink.sh status` | Show status of all services |
| `./kafka-connect-iceberg-sink.sh generate-logs` | Generate API logs on source MinIO |
| `./kafka-connect-iceberg-sink.sh query` | Query Iceberg table on destination |
| `./kafka-connect-iceberg-sink.sh logs [-f]` | View container logs |
| `./kafka-connect-iceberg-sink.sh restart` | Restart all services |
| `./kafka-connect-iceberg-sink.sh clean` | Stop and remove all data/volumes |

## Endpoints

| Service | Endpoint | Credentials |
|---------|----------|-------------|
| Source MinIO API | http://localhost:9000 | minioadmin/minioadmin |
| Source MinIO Console | http://localhost:9001 | minioadmin/minioadmin |
| Destination MinIO API | http://localhost:9010 | minioadmin/minioadmin |
| Destination MinIO Console | http://localhost:9011 | minioadmin/minioadmin |
| Kafka | localhost:9092 | - |
| Kafka Connect REST API | http://localhost:8083 | - |

## Configuration

### Environment Variables (.env)

```bash
# Source MinIO (with Kafka log targets from PR #2463)
MINIO_SOURCE_IMAGE=praveenminio/kafka-connect:v1.0.0

# Destination MinIO (with Iceberg Tables support)
MINIO_DEST_IMAGE=quay.io/minio/aistor/minio:EDGE.2025-12-13T08-46-12Z

# MinIO License (required)
MINIO_LICENSE=<your-license-key>

# Credentials
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Iceberg configuration
WAREHOUSE_NAME=kafkawarehouse
NAMESPACE_NAME=streaming
TABLE_NAME=events
```

### Iceberg Connector Configuration

The connector is configured with:

| Setting | Value |
|---------|-------|
| `iceberg.catalog.type` | `rest` |
| `iceberg.catalog.uri` | `http://nginx-dest:9000/_iceberg` |
| `iceberg.catalog.warehouse` | `kafkawarehouse` |
| `iceberg.tables` | `streaming.events` |
| `iceberg.tables.auto-create-enabled` | `true` |
| `iceberg.tables.evolve-schema-enabled` | `true` |
| `iceberg.control.topic` | `control-iceberg` |
| `iceberg.control.commit.interval-ms` | `10000` |

## Viewing Data

### Kafka Messages

```bash
# View events topic
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic events \
  --from-beginning \
  --max-messages 5
```

### Connector Status

```bash
curl -s http://localhost:8083/connectors/iceberg-sink/status | jq
```

### Iceberg Table (PyIceberg)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    'aistor',
    type='rest',
    uri='http://localhost:9010/_iceberg',
    warehouse='kafkawarehouse',
    **{
        'rest.sigv4-enabled': 'true',
        'rest.signing-name': 's3tables',
        'rest.signing-region': 'us-east-1',
        's3.endpoint': 'http://localhost:9010',
        's3.access-key-id': 'minioadmin',
        's3.secret-access-key': 'minioadmin',
        's3.path-style-access': 'true',
    }
)

table = catalog.load_table('streaming.events')
df = table.scan().to_pandas()
print(df)
```

## Troubleshooting

### Services not starting

Check container logs:
```bash
./kafka-connect-iceberg-sink.sh logs -f
```

### Connector not running

Check connector status:
```bash
curl -s http://localhost:8083/connectors/iceberg-sink/status | jq
```

Restart connector:
```bash
curl -X POST http://localhost:8083/connectors/iceberg-sink/restart
```

### No data in Iceberg table

1. Verify logs are being generated:
   ```bash
   docker exec kafka kafka-console-consumer \
     --bootstrap-server localhost:29092 \
     --topic events \
     --from-beginning \
     --max-messages 1
   ```

2. Check connector tasks:
   ```bash
   curl -s http://localhost:8083/connectors/iceberg-sink/tasks/0/status | jq
   ```

3. The connector commits every 10 seconds - wait for commit interval

### MinIO cluster unhealthy

Check individual node logs:
```bash
docker logs minio-src-1
docker logs minio-dst-1
```

Verify license is set:
```bash
grep MINIO_LICENSE .env
```

## File Structure

```
.
├── .env.sample                   # Sample config (copy to .env and add license)
├── .gitignore                    # Ignores plugins directory and .env
├── docker-compose.yaml           # All service definitions
├── init-setup.py                 # Initialization script
├── kafka-connect-iceberg-sink.sh # Management script
├── nginx/
│   ├── nginx-source.conf         # Source cluster load balancer config
│   └── nginx-dest.conf           # Destination cluster load balancer config
├── plugins/                      # Iceberg Kafka Connect plugin (auto-downloaded)
└── README.md                     # This file
```

## License

This setup requires a valid MinIO AIStor license for both source and destination clusters.

- Source cluster uses the Kafka log targets feature (PR #2463)
- Destination cluster uses the Tables feature (Iceberg REST Catalog)

Get your license at https://min.io/pricing
