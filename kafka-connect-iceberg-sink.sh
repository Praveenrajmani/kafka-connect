#!/bin/bash
#
# Kafka Connect Iceberg Sink - Dual Cluster Setup
# Source MinIO (Kafka Log Targets) -> Kafka -> Iceberg -> Destination MinIO (Tables)
#

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

print_banner() {
    echo -e "${CYAN}"
    cat << 'EOF'
    ╔══════════════════════════════════════════════════════════════════════╗
    ║     Kafka Connect Iceberg Sink - Dual Cluster Setup                  ║
    ║     Source MinIO -> Kafka -> Iceberg -> Destination MinIO            ║
    ╚══════════════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
}

print_header() {
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
}

print_msg() {
    local color=$1
    local msg=$2
    echo -e "${color}${msg}${NC}"
}

usage() {
    cat << EOF
Kafka Connect Iceberg Sink - Dual Cluster Setup

USAGE:
  $0 [command] [options]

COMMANDS:
  start           Start all services (default)
  stop            Stop all services
  status          Show status of all services
  logs            Show logs (use -f for follow)
  generate-logs   Generate API logs by interacting with source MinIO
  query           Query the Iceberg table on destination MinIO
  restart         Restart all services
  clean           Stop and remove all data

OPTIONS:
  -h, --help      Show this help message

ARCHITECTURE:
  ┌─────────────────────┐     ┌─────────────────────┐
  │   SOURCE CLUSTER    │     │  DESTINATION CLUSTER │
  │   (4-node MinIO)    │     │   (4-node MinIO)     │
  │   Kafka Log Targets │     │   Iceberg Tables     │
  │   localhost:9000/01 │     │   localhost:9010/11  │
  └──────────┬──────────┘     └──────────▲───────────┘
             │                           │
             ▼                           │
  ┌──────────────────────────────────────┴───────────┐
  │                    KAFKA                          │
  │              localhost:9092                       │
  │                     │                             │
  │              Kafka Connect                        │
  │            localhost:8083                         │
  │           (Iceberg Sink)                          │
  └───────────────────────────────────────────────────┘

EXAMPLES:
  # Start everything
  $0 start

  # Check status
  $0 status

  # Generate logs on source MinIO
  $0 generate-logs

  # View logs
  $0 logs -f

  # Query Iceberg table on destination
  $0 query

  # Stop services
  $0 stop

  # Clean everything
  $0 clean

EOF
    exit 0
}

# Load config from .env
load_config() {
    if [ -f "${PROJECT_ROOT}/.env" ]; then
        print_msg "$YELLOW" "Loading configuration from ${PROJECT_ROOT}/.env..."
        set -a
        source "${PROJECT_ROOT}/.env"
        set +a
    fi

    # Check if license is set
    if [ -z "$MINIO_LICENSE" ]; then
        print_msg "$RED" "Error: MINIO_LICENSE not set"
        print_msg "$YELLOW" "Please set MINIO_LICENSE in ${PROJECT_ROOT}/.env"
        exit 1
    fi
}

# Download Iceberg Kafka Connect plugin if not present
download_plugin() {
    local plugin_dir="$SCRIPT_DIR/plugins/iceberg-kafka-connect"
    local plugin_version="${ICEBERG_CONNECTOR_VERSION:-0.6.19}"
    local plugin_zip="iceberg-kafka-connect-runtime-${plugin_version}.zip"
    local download_url="https://github.com/databricks/iceberg-kafka-connect/releases/download/v${plugin_version}/${plugin_zip}"

    if [ -d "$plugin_dir" ] && [ "$(ls -A $plugin_dir 2>/dev/null)" ]; then
        print_msg "$GREEN" "✓ Iceberg Kafka Connect plugin already installed"
        return 0
    fi

    print_header "Downloading Iceberg Kafka Connect Plugin"

    mkdir -p "$SCRIPT_DIR/plugins"

    print_msg "$YELLOW" "Downloading from: $download_url"
    if ! curl -L -o "/tmp/${plugin_zip}" "$download_url"; then
        print_msg "$RED" "Failed to download plugin"
        exit 1
    fi

    print_msg "$YELLOW" "Extracting plugin..."
    unzip -q -o "/tmp/${plugin_zip}" -d "$SCRIPT_DIR/plugins/"
    rm "/tmp/${plugin_zip}"

    # Rename to standard directory name
    local extracted_dir=$(ls -d "$SCRIPT_DIR/plugins"/iceberg-kafka-connect-runtime-* 2>/dev/null | head -1)
    if [ -n "$extracted_dir" ] && [ "$extracted_dir" != "$plugin_dir" ]; then
        mv "$extracted_dir" "$plugin_dir"
    fi

    print_msg "$GREEN" "✓ Plugin installed to: $plugin_dir"
}

# Start services
start_services() {
    print_banner
    load_config
    download_plugin

    print_header "Starting Dual Cluster Setup"

    cd "$SCRIPT_DIR"

    # Export for docker-compose
    export MINIO_LICENSE
    export MINIO_SOURCE_IMAGE
    export MINIO_DEST_IMAGE
    export MINIO_ROOT_USER
    export MINIO_ROOT_PASSWORD
    export KAFKA_IMAGE
    export KAFKA_CONNECT_IMAGE

    print_msg "$YELLOW" "Starting Source MinIO cluster (4 nodes)..."
    docker compose up -d minio-src-1 minio-src-2 minio-src-3 minio-src-4

    print_msg "$YELLOW" "Starting Destination MinIO cluster (4 nodes)..."
    docker compose up -d minio-dst-1 minio-dst-2 minio-dst-3 minio-dst-4

    print_msg "$YELLOW" "Starting Kafka..."
    docker compose up -d kafka

    print_msg "$YELLOW" "Waiting for Source MinIO cluster to be healthy..."
    for i in {1..60}; do
        healthy=0
        for node in minio-src-1 minio-src-2 minio-src-3 minio-src-4; do
            if docker exec $node curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
                ((healthy++))
            fi
        done
        if [ $healthy -eq 4 ]; then
            print_msg "$GREEN" "✓ Source MinIO cluster is healthy (4/4 nodes)"
            break
        fi
        if [ $i -eq 60 ]; then
            print_msg "$RED" "Timeout waiting for Source MinIO cluster"
            exit 1
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Waiting for Destination MinIO cluster to be healthy..."
    for i in {1..60}; do
        healthy=0
        for node in minio-dst-1 minio-dst-2 minio-dst-3 minio-dst-4; do
            if docker exec $node curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
                ((healthy++))
            fi
        done
        if [ $healthy -eq 4 ]; then
            print_msg "$GREEN" "✓ Destination MinIO cluster is healthy (4/4 nodes)"
            break
        fi
        if [ $i -eq 60 ]; then
            print_msg "$RED" "Timeout waiting for Destination MinIO cluster"
            exit 1
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Starting Nginx load balancers..."
    docker compose up -d nginx-source nginx-dest

    print_msg "$YELLOW" "Waiting for Nginx proxies to be ready..."
    for i in {1..30}; do
        if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1 && \
           curl -sf http://localhost:9010/minio/health/live >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ Nginx proxies are ready"
            break
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Waiting for Kafka to be ready..."
    for i in {1..30}; do
        if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ Kafka is ready"
            break
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Starting Kafka Connect..."
    docker compose up -d kafka-connect

    print_msg "$YELLOW" "Waiting for Kafka Connect to be ready (this may take a minute)..."
    for i in {1..60}; do
        if curl -sf http://localhost:8083/connectors >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ Kafka Connect is ready"
            break
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Running initialization..."
    docker compose up init

    print_header "All Services Started!"

    echo -e "${GREEN}"
    cat << 'EOF'
    ┌──────────────────────────────────────────────────────────────────────────┐
    │                    DUAL CLUSTER SETUP READY                              │
    ├──────────────────────────────────────────────────────────────────────────┤
    │                                                                          │
    │  SOURCE CLUSTER (Kafka Log Targets):                                     │
    │    • MinIO API:     http://localhost:9000   (via nginx-source)           │
    │    • MinIO Console: http://localhost:9001   (minioadmin/minioadmin)      │
    │    • 4 distributed nodes with Kafka log targets enabled                  │
    │                                                                          │
    │  DESTINATION CLUSTER (Iceberg Tables):                                   │
    │    • MinIO API:     http://localhost:9010   (via nginx-dest)             │
    │    • MinIO Console: http://localhost:9011   (minioadmin/minioadmin)      │
    │    • 4 distributed nodes with Iceberg REST catalog                       │
    │                                                                          │
    │  KAFKA & CONNECT:                                                        │
    │    • Kafka:         localhost:9092                                       │
    │    • Kafka Connect: http://localhost:8083                                │
    │                                                                          │
    │  ICEBERG CONFIGURATION:                                                  │
    │    • Warehouse:     kafkawarehouse                                       │
    │    • Namespace:     streaming                                            │
    │    • Table:         events                                               │
    │                                                                          │
    │  DATA FLOW:                                                              │
    │    Source MinIO API logs -> Kafka -> Iceberg -> Destination MinIO        │
    │                                                                          │
    └──────────────────────────────────────────────────────────────────────────┘
EOF
    echo -e "${NC}"

    print_msg "$CYAN" "Quick commands:"
    echo "  $0 generate-logs  - Generate API logs on source MinIO"
    echo "  $0 status         - Check service status"
    echo "  $0 logs -f        - View logs"
    echo "  $0 query          - Query Iceberg table on destination"
    echo "  $0 stop           - Stop all services"
}

# Stop services
stop_services() {
    print_header "Stopping Services"
    cd "$SCRIPT_DIR"
    docker compose down
    print_msg "$GREEN" "✓ All services stopped"
}

# Clean everything
clean_services() {
    print_header "Cleaning Up"
    cd "$SCRIPT_DIR"
    docker compose down -v --remove-orphans
    print_msg "$GREEN" "✓ All services and volumes removed"
}

# Show status
show_status() {
    print_header "Service Status"
    cd "$SCRIPT_DIR"

    echo -e "${YELLOW}Docker Containers:${NC}"
    docker compose ps

    echo ""
    echo -e "${YELLOW}Source MinIO Cluster Health:${NC}"
    for node in minio-src-1 minio-src-2 minio-src-3 minio-src-4; do
        if docker exec $node curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
            echo -e "  ${GREEN}✓ $node: healthy${NC}"
        else
            echo -e "  ${RED}✗ $node: unhealthy${NC}"
        fi
    done

    echo ""
    echo -e "${YELLOW}Destination MinIO Cluster Health:${NC}"
    for node in minio-dst-1 minio-dst-2 minio-dst-3 minio-dst-4; do
        if docker exec $node curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
            echo -e "  ${GREEN}✓ $node: healthy${NC}"
        else
            echo -e "  ${RED}✗ $node: unhealthy${NC}"
        fi
    done

    echo ""
    echo -e "${YELLOW}Nginx Load Balancers:${NC}"
    if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        echo -e "  ${GREEN}✓ nginx-source (localhost:9000/9001): healthy${NC}"
    else
        echo -e "  ${RED}✗ nginx-source: unhealthy${NC}"
    fi
    if curl -sf http://localhost:9010/minio/health/live >/dev/null 2>&1; then
        echo -e "  ${GREEN}✓ nginx-dest (localhost:9010/9011): healthy${NC}"
    else
        echo -e "  ${RED}✗ nginx-dest: unhealthy${NC}"
    fi

    echo ""
    echo -e "${YELLOW}Kafka Connect Connectors:${NC}"
    if curl -sf http://localhost:8083/connectors >/dev/null 2>&1; then
        curl -s http://localhost:8083/connectors | jq -r '.[]' 2>/dev/null | while read connector; do
            status=$(curl -s "http://localhost:8083/connectors/${connector}/status" | jq -r '.connector.state')
            echo "  • $connector: $status"
        done
    else
        echo "  Kafka Connect is not running"
    fi

    echo ""
    echo -e "${YELLOW}Kafka Topics:${NC}"
    if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null; then
        :
    else
        echo "  Kafka is not running"
    fi
}

# Show logs
show_logs() {
    cd "$SCRIPT_DIR"
    docker compose logs "$@"
}

# Generate logs by interacting with source MinIO
generate_logs() {
    print_header "Generating API Logs on Source MinIO"

    if ! curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
        print_msg "$RED" "Error: Source MinIO is not running"
        exit 1
    fi

    print_msg "$YELLOW" "Creating test bucket and uploading objects to generate API logs..."

    # Use mc (MinIO Client) in a container to interact with source MinIO
    docker run --rm --network kafka-connect_kafka-iceberg \
        -e MC_HOST_source=http://minioadmin:minioadmin@nginx-source:9000 \
        minio/mc:latest bash -c "
        echo 'Creating test bucket...'
        mc mb source/test-logs-bucket --ignore-existing

        echo 'Uploading test objects...'
        for i in 1 2 3 4 5; do
            echo \"Test object \$i - \$(date)\" | mc pipe source/test-logs-bucket/test-\$i.txt
            echo \"  Uploaded: test-\$i.txt\"
        done

        echo ''
        echo 'Listing bucket contents...'
        mc ls source/test-logs-bucket/

        echo ''
        echo 'Getting object stats...'
        mc stat source/test-logs-bucket/test-1.txt
        "

    print_msg "$GREEN" ""
    print_msg "$GREEN" "✓ API logs generated!"
    print_msg "$CYAN" ""
    print_msg "$CYAN" "Each S3 API call generates a log entry sent to Kafka."
    print_msg "$CYAN" "The Iceberg sink commits every 10 seconds."
    print_msg "$CYAN" ""
    print_msg "$CYAN" "View the events in Kafka:"
    print_msg "$CYAN" "  docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic events --from-beginning --max-messages 5"
    print_msg "$CYAN" ""
    print_msg "$CYAN" "Query the Iceberg table:"
    print_msg "$CYAN" "  $0 query"
    print_msg "$CYAN" ""
    print_msg "$CYAN" "View Destination MinIO Console (Iceberg data):"
    print_msg "$CYAN" "  http://localhost:9011"
}

# Query the Iceberg table on destination MinIO
query_table() {
    print_header "Querying Iceberg Table on Destination MinIO"

    print_msg "$YELLOW" "Installing PyIceberg and querying table..."

    docker run --rm --network kafka-connect_kafka-iceberg \
        -e AWS_ACCESS_KEY_ID=minioadmin \
        -e AWS_SECRET_ACCESS_KEY=minioadmin \
        -e AWS_REGION=us-east-1 \
        python:3.11-slim bash -c "
        pip install -q pyiceberg[s3,pandas] boto3 2>/dev/null
        python3 << 'PYEOF'
import json
from pyiceberg.catalog import load_catalog

print('Connecting to Destination MinIO Iceberg catalog...')
try:
    catalog = load_catalog(
        'aistor',
        type='rest',
        uri='http://nginx-dest:9000/_iceberg',
        warehouse='kafkawarehouse',
        **{
            'rest.sigv4-enabled': 'true',
            'rest.signing-name': 's3tables',
            'rest.signing-region': 'us-east-1',
            's3.endpoint': 'http://nginx-dest:9000',
            's3.access-key-id': 'minioadmin',
            's3.secret-access-key': 'minioadmin',
            's3.path-style-access': 'true',
            's3.region': 'us-east-1',
        }
    )

    print('\\nNamespaces:', catalog.list_namespaces())

    table = catalog.load_table('streaming.events')

    print('\\nTable Data (last 10 rows - summary view):')
    df = table.scan().to_pandas()
    if len(df) > 0:
        # Show summary columns first
        summary_cols = ['time', 'name', 'type', 'bucket', 'object', 'node', 'origin']
        available_cols = [c for c in summary_cols if c in df.columns]
        if available_cols:
            print(df[available_cols].tail(10).to_string())
        else:
            print(df.tail(10).to_string())
        print(f'\\nTotal rows: {len(df)}')

        # Pretty print last row with full details
        print('\\n' + '='*60)
        print('Last log entry (full details):')
        print('='*60)
        last_row = df.iloc[-1].to_dict()
        # Convert nested dicts for pretty printing
        for key, value in last_row.items():
            if isinstance(value, dict):
                print(f'\\n{key}:')
                print(json.dumps(value, indent=2, default=str))
            elif value is not None:
                print(f'{key}: {value}')
    else:
        print('No data yet. Run: ./kafka-connect-iceberg-sink.sh generate-logs')

except Exception as e:
    print(f'Error: {e}')
    print('\\nTable may not exist yet. Generate some logs first:')
    print('  ./kafka-connect-iceberg-sink.sh generate-logs')
PYEOF
        "
}

# Restart services
restart_services() {
    stop_services
    start_services
}

# Main
main() {
    local command="${1:-start}"

    case "$command" in
        start)
            start_services
            ;;
        stop)
            stop_services
            ;;
        status)
            show_status
            ;;
        logs)
            shift
            show_logs "$@"
            ;;
        generate-logs|produce)
            generate_logs
            ;;
        query)
            query_table
            ;;
        restart)
            restart_services
            ;;
        clean)
            clean_services
            ;;
        -h|--help|help)
            usage
            ;;
        *)
            print_msg "$RED" "Unknown command: $command"
            usage
            ;;
    esac
}

main "$@"
