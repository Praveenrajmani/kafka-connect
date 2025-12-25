#!/bin/bash
#
# Kafka Connect Iceberg Sink to MinIO AIStor
# One-click setup for streaming Kafka data to Iceberg tables
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
    ╔═══════════════════════════════════════════════════════════════╗
    ║     Kafka Connect Iceberg Sink → MinIO AIStor                 ║
    ║     Stream data from Kafka to Iceberg tables                  ║
    ╚═══════════════════════════════════════════════════════════════╝
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
Kafka Connect Iceberg Sink to MinIO AIStor

USAGE:
  $0 [command] [options]

COMMANDS:
  start           Start all services (default)
  stop            Stop all services
  status          Show status of all services
  logs            Show logs (use -f for follow)
  produce         Produce sample messages to Kafka
  query           Query the Iceberg table
  restart         Restart all services
  clean           Stop and remove all data

OPTIONS:
  -h, --help      Show this help message

EXAMPLES:
  # Start everything
  $0 start

  # Check status
  $0 status

  # Produce test messages
  $0 produce

  # View logs
  $0 logs -f

  # Query Iceberg table
  $0 query

  # Stop services
  $0 stop

  # Clean everything
  $0 clean

EOF
    exit 0
}

# Load MinIO license from parent .env if available
load_config() {
    if [ -f "${PROJECT_ROOT}/.env" ]; then
        print_msg "$YELLOW" "Loading MinIO license from ${PROJECT_ROOT}/.env..."
        set -a
        source "${PROJECT_ROOT}/.env"
        set +a
    fi

    # Check if license is set
    if [ -z "$MINIO_LICENSE" ]; then
        print_msg "$RED" "Error: MINIO_LICENSE not set"
        print_msg "$YELLOW" "Please set MINIO_LICENSE in ${PROJECT_ROOT}/.env or export it"
        exit 1
    fi
}

# Download Iceberg Kafka Connect plugin if not present
download_plugin() {
    local plugin_dir="$SCRIPT_DIR/plugins/iceberg-kafka-connect"
    local plugin_zip="iceberg-kafka-connect-runtime-0.6.19.zip"
    local download_url="https://github.com/databricks/iceberg-kafka-connect/releases/download/v0.6.19/${plugin_zip}"

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

    print_header "Starting Services"

    cd "$SCRIPT_DIR"

    # Export for docker-compose
    export MINIO_LICENSE
    export MINIO_TEST_IMAGE="${MINIO_TEST_IMAGE:-quay.io/minio/aistor/minio:log-targets}"

    print_msg "$YELLOW" "Starting Kafka, Kafka Connect, and MinIO..."
    docker compose up -d kafka minio

    print_msg "$YELLOW" "Waiting for Kafka to be ready..."
    for i in {1..30}; do
        if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ Kafka is ready"
            break
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Waiting for MinIO to be ready..."
    for i in {1..30}; do
        if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ MinIO is ready"
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
    ┌─────────────────────────────────────────────────────────────────┐
    │                    SERVICES READY                               │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                 │
    │  Kafka:              localhost:9092                             │
    │  Kafka Connect:      http://localhost:8083                      │
    │  MinIO Console:      http://localhost:9001                      │
    │                      (user: minioadmin / pass: minioadmin)      │
    │  MinIO API:          http://localhost:9000                      │
    │                                                                 │
    │  Connector:          iceberg-sink                               │
    │  Data Topic:         events                                     │
    │  Iceberg Table:      kafkawarehouse.streaming.events            │
    │                                                                 │
    └─────────────────────────────────────────────────────────────────┘
EOF
    echo -e "${NC}"

    print_msg "$CYAN" "Quick commands:"
    echo "  $0 produce   - Send test messages"
    echo "  $0 status    - Check service status"
    echo "  $0 logs -f   - View logs"
    echo "  $0 query     - Query Iceberg table"
    echo "  $0 stop      - Stop all services"
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

# Produce sample messages
produce_messages() {
    print_header "Producing Sample Messages"

    if ! docker compose exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null | grep -q events; then
        print_msg "$RED" "Error: Kafka is not running or 'events' topic doesn't exist"
        exit 1
    fi

    print_msg "$YELLOW" "Sending 5 sample events to 'events' topic..."

    for i in {1..5}; do
        timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        event_id="evt-$(date +%s)-$i"
        event_types=("click" "view" "purchase" "signup" "logout")
        event_type=${event_types[$((RANDOM % 5))]}
        user_id=$((RANDOM % 1000 + 1))

        message="{\"event_id\": \"$event_id\", \"event_type\": \"$event_type\", \"user_id\": $user_id, \"timestamp\": \"$timestamp\", \"payload\": {\"page\": \"/home\", \"device\": \"mobile\"}}"

        echo "$message" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic events

        print_msg "$GREEN" "✓ Sent: $event_id ($event_type)"
    done

    print_msg "$CYAN" ""
    print_msg "$CYAN" "Messages sent! The Iceberg sink commits every 10 seconds."
    print_msg "$CYAN" "Check MinIO Console at http://localhost:9001 to see the data files."
    print_msg "$CYAN" "Run '$0 query' to query the Iceberg table."
}

# Query the Iceberg table
query_table() {
    print_header "Querying Iceberg Table"

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

print('Connecting to catalog...')
try:
    catalog = load_catalog(
        'aistor',
        type='rest',
        uri='http://minio:9000/_iceberg',
        warehouse='kafkawarehouse',
        **{
            'rest.sigv4-enabled': 'true',
            'rest.signing-name': 's3tables',
            'rest.signing-region': 'us-east-1',
            's3.endpoint': 'http://minio:9000',
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
        print(df[available_cols].tail(10).to_string())
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
        print('No data yet. Run: ./kafka-connect-iceberg-sink.sh produce')

except Exception as e:
    print(f'Error: {e}')
    print('\\nTable may not exist yet. Send some messages first:')
    print('  ./kafka-connect-iceberg-sink.sh produce')
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
        produce)
            produce_messages
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
