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
Kafka Connect Iceberg Sink to MinIO AIStor (Dual Cluster Architecture)

Streams MinIO logs (API, Error, Audit) from a source cluster to Iceberg tables
on a destination cluster via Kafka Connect.

ARCHITECTURE:
  Source MinIO (4-node) → Kafka → Kafka Connect → Dest MinIO (4-node)
        ↓                                              ↑
   nginx-source                                   nginx-dest
   :9000/:9001                                   :9010/:9011

USAGE:
  $0 [command] [options]

COMMANDS:
  start           Start all services (default)
  stop            Stop all services
  status          Show status of all services
  logs            Show logs (use -f for follow)
  generate <type> Generate logs on source cluster (api|error|audit|all)
  continuous      Generate logs continuously (Ctrl+C to stop)
  query [table]   Query Iceberg table(s) with PyIceberg
  restart         Restart all services
  clean           Stop and remove all data

SERVICES:
  Source MinIO:      http://localhost:9000 (API), http://localhost:9001 (Console)
  Destination MinIO: http://localhost:9010 (API), http://localhost:9011 (Console)
  Kafka:             localhost:9092
  Kafka Connect:     http://localhost:8083
  Trino:             http://localhost:9999

LOG PIPELINES:
  Topic       Table                  Description
  ─────────   ────────────────────   ─────────────────────
  apilogs     streaming.apilogs      MinIO API logs
  errorlogs   streaming.errorlogs    MinIO Error logs
  auditlogs   streaming.auditlogs    MinIO Audit logs

OPTIONS:
  -h, --help      Show this help message

EXAMPLES:
  # Start everything
  $0 start

  # Check status
  $0 status

  # Generate logs on source cluster (flows to Iceberg tables)
  $0 generate api              # Generate 100 API logs
  $0 generate api --count 500  # Generate 500 API logs
  $0 generate error            # Generate error logs
  $0 generate audit            # Generate audit logs
  $0 generate all --count 50   # Generate all log types (50 each)

  # Continuous log generation (Ctrl+C to stop)
  $0 continuous                     # Default: 2s interval, 5 ops/batch
  $0 continuous -i 5 -b 10          # 5s interval, 10 ops per batch

  # Query Iceberg tables with Trino
  docker exec -it trino trino
  SELECT * FROM kafkawarehouse.streaming.apilogs LIMIT 10;

  # Query tables with PyIceberg
  $0 query
  $0 query apilogs

  # View logs
  $0 logs -f

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

    print_msg "$YELLOW" "Starting Kafka and MinIO clusters..."
    docker compose up -d kafka nginx-source nginx-dest

    print_msg "$YELLOW" "Waiting for Kafka to be ready..."
    for i in {1..30}; do
        if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ Kafka is ready"
            break
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Waiting for Source MinIO cluster to be ready..."
    for i in {1..30}; do
        if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ Source MinIO cluster is ready"
            break
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Waiting for Destination MinIO cluster to be ready..."
    for i in {1..30}; do
        if curl -sf http://localhost:9010/minio/health/live >/dev/null 2>&1; then
            print_msg "$GREEN" "✓ Destination MinIO cluster is ready"
            break
        fi
        sleep 2
    done

    print_msg "$YELLOW" "Starting Kafka Connect and Trino..."
    docker compose up -d kafka-connect trino

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
    │                 (Dual Cluster Architecture)                     │
    ├─────────────────────────────────────────────────────────────────┤
    │                                                                 │
    │  Kafka:                  localhost:9092                         │
    │  Kafka Connect:          http://localhost:8083                  │
    │                                                                 │
    │  Source MinIO (logs):                                           │
    │    • Console:            http://localhost:9001                  │
    │    • API:                http://localhost:9000                  │
    │                                                                 │
    │  Destination MinIO (Iceberg):                                   │
    │    • Console:            http://localhost:9011                  │
    │    • API:                http://localhost:9010                  │
    │                                                                 │
    │  Trino:                  http://localhost:9999                  │
    │                                                                 │
    │  Credentials:            minioadmin / minioadmin                │
    │                                                                 │
    │  Log Pipelines:                                                 │
    │    • apilogs   → streaming.apilogs   (API logs)                 │
    │    • errorlogs → streaming.errorlogs (Error logs)               │
    │    • auditlogs → streaming.auditlogs (Audit logs)               │
    │                                                                 │
    └─────────────────────────────────────────────────────────────────┘
EOF
    echo -e "${NC}"

    print_msg "$CYAN" "Quick start:"
    echo "  # Generate API logs on source cluster"
    echo "  $0 generate api --count 100"
    echo ""
    echo "  # Query logs in Trino"
    echo "  docker exec -it trino trino --execute 'SELECT * FROM kafkawarehouse.streaming.apilogs LIMIT 10'"
    echo ""
    print_msg "$CYAN" "Generate logs with custom count:"
    echo "  $0 generate api --count 500"
    echo "  $0 generate error --count 100"
    echo "  $0 generate audit --count 200"
    echo ""
    print_msg "$CYAN" "Other commands:"
    echo "  $0 status              - Check service status"
    echo "  $0 logs -f             - View logs"
    echo "  $0 generate all        - Generate all log types"
    echo "  $0 query               - Query Iceberg tables"
    echo "  $0 stop                - Stop all services"
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

# Generate a single log message for a given type
# Args: $1 = type (api|error|audit), $2 = index (for variation)
generate_log_message() {
    local msg_type="$1"
    local index="$2"
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
    local request_id=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen 2>/dev/null || echo "17A3C4775707B695-$index")

    # Arrays for variation
    local api_names=("s3.GetObject" "s3.PutObject" "s3.DeleteObject" "s3.ListObjects" "s3.HeadObject")
    local buckets=("testbucket" "databucket" "logsbucket" "backupbucket")
    local objects=("file-${index}.txt" "data-${index}.json" "image-${index}.png" "doc-${index}.pdf")
    local sources=("192.168.1.$((100 + index % 50))" "10.0.0.$((1 + index % 254))" "172.16.0.$((1 + index % 100))")
    local status_codes=(200 200 200 200 201 204 404 403 500)
    local error_messages=("The specified bucket does not exist" "Access Denied" "Object not found" "Internal server error" "Request timeout")

    # Pick values based on index for variation
    local api_name="${api_names[$((index % ${#api_names[@]}))]}"
    local bucket="${buckets[$((index % ${#buckets[@]}))]}"
    local object="${objects[$((index % ${#objects[@]}))]}"
    local source="${sources[$((index % ${#sources[@]}))]}"
    local status_code="${status_codes[$((index % ${#status_codes[@]}))]}"
    local error_msg="${error_messages[$((index % ${#error_messages[@]}))]}"

    case "$msg_type" in
        api)
            # API log message based on log.API struct
            cat <<EOF
{"version":"1","time":"$timestamp","node":"minio-node-$((1 + index % 3))","origin":"client","type":"object","name":"$api_name","bucket":"$bucket","object":"$object","versionId":"","tags":{},"callInfo":{"httpStatusCode":$status_code,"rx":$((index * 10)),"tx":$((1024 + index * 100)),"txHeaders":256,"timeToFirstByte":"$((2 + index % 10))ms","requestReadTime":"$((1 + index % 5))ms","responseWriteTime":"$((5 + index % 20))ms","requestTime":"$((10 + index % 50))ms","timeToResponse":"$((8 + index % 30))ms","sourceHost":"$source","requestID":"$request_id","userAgent":"MinIO (linux; amd64) minio-go/v7.0.0","requestPath":"/$bucket/$object","requestHost":"localhost:9000","accessKey":"minioadmin"}}
EOF
            ;;
        error)
            # Error log message based on log.Error struct
            cat <<EOF
{"version":"1","node":"minio-node-$((1 + index % 3))","time":"$timestamp","message":"$error_msg","apiName":"$api_name","trace":{"message":"$error_msg","source":["api-errors.go:$((100 + index))","bucket-handlers.go:$((400 + index))"]},"tags":{"bucket":"$bucket"}}
EOF
            ;;
        audit)
            # Audit log message based on log.Audit struct
            local actions=("read" "write" "delete" "list")
            local action="${actions[$((index % ${#actions[@]}))]}"
            cat <<EOF
{"version":"1","time":"$timestamp","node":"minio-node-$((1 + index % 3))","apiName":"$api_name","category":"object","action":"$action","bucket":"$bucket","tags":{},"requestID":"$request_id","requestClaims":{},"sourceHost":"$source","accessKey":"minioadmin","parentUser":"","details":{"object":"$object","versionId":""}}
EOF
            ;;
    esac
}

# Produce sample messages to Kafka topics based on MinIO log structs
# See: https://github.com/minio/madmin-go/blob/main/log
produce_messages() {
    local topic=""
    local count=100

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -n)
                count="$2"
                shift 2
                ;;
            *)
                if [ -z "$topic" ]; then
                    topic="$1"
                fi
                shift
                ;;
        esac
    done

    # Validate count
    if ! [[ "$count" =~ ^[0-9]+$ ]] || [ "$count" -lt 1 ]; then
        print_msg "$RED" "Invalid count: $count. Must be a positive integer."
        exit 1
    fi

    # If a specific topic is provided, produce to it
    if [ -n "$topic" ]; then
        case "$topic" in
            apilogs)
                print_header "Producing $count messages to '$topic' topic"
                for ((i=1; i<=count; i++)); do
                    generate_log_message "api" "$i" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic "$topic"
                    if [ $((i % 10)) -eq 0 ]; then
                        printf "\r${YELLOW}Progress: %d/%d messages${NC}" "$i" "$count"
                    fi
                done
                echo ""
                print_msg "$GREEN" "✓ $count API log messages produced to '$topic'"
                ;;
            errorlogs)
                print_header "Producing $count messages to '$topic' topic"
                for ((i=1; i<=count; i++)); do
                    generate_log_message "error" "$i" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic "$topic"
                    if [ $((i % 10)) -eq 0 ]; then
                        printf "\r${YELLOW}Progress: %d/%d messages${NC}" "$i" "$count"
                    fi
                done
                echo ""
                print_msg "$GREEN" "✓ $count error log messages produced to '$topic'"
                ;;
            auditlogs)
                print_header "Producing $count messages to '$topic' topic"
                for ((i=1; i<=count; i++)); do
                    generate_log_message "audit" "$i" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic "$topic"
                    if [ $((i % 10)) -eq 0 ]; then  
                        printf "\r${YELLOW}Progress: %d/%d messages${NC}" "$i" "$count"
                    fi
                done
                echo ""
                print_msg "$GREEN" "✓ $count audit log messages produced to '$topic'"
                ;;
            *)
                print_msg "$RED" "Unknown topic: $topic"
                print_msg "$YELLOW" "Valid topics: apilogs, errorlogs, auditlogs"
                exit 1
                ;;
        esac
        return
    fi

    # No topic specified - produce to all topics
    print_header "Producing $count Messages to Each Topic"

    print_msg "$YELLOW" "Sending $count messages to each log topic (total: $((count * 3)) messages)..."
    echo ""

    print_msg "$CYAN" "Producing to 'apilogs'..."
    for ((i=1; i<=count; i++)); do
        generate_log_message "api" "$i" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic apilogs
        if [ $((i % 10)) -eq 0 ]; then
            printf "\r${YELLOW}Progress: %d/%d messages${NC}" "$i" "$count"
        fi
    done
    echo ""
    print_msg "$GREEN" "✓ $count API log messages produced to 'apilogs'"

    print_msg "$CYAN" "Producing to 'errorlogs'..."
    for ((i=1; i<=count; i++)); do
        generate_log_message "error" "$i" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic errorlogs
        if [ $((i % 10)) -eq 0 ]; then
            printf "\r${YELLOW}Progress: %d/%d messages${NC}" "$i" "$count"
        fi
    done
    echo ""
    print_msg "$GREEN" "✓ $count error log messages produced to 'errorlogs'"

    print_msg "$CYAN" "Producing to 'auditlogs'..."
    for ((i=1; i<=count; i++)); do
        generate_log_message "audit" "$i" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:29092 --topic auditlogs
        if [ $((i % 10)) -eq 0 ]; then
            printf "\r${YELLOW}Progress: %d/%d messages${NC}" "$i" "$count"
        fi
    done
    echo ""
    print_msg "$GREEN" "✓ $count audit log messages produced to 'auditlogs'"

    echo ""
    print_msg "$CYAN" "The Iceberg sink commits every 10 seconds."
    print_msg "$CYAN" "Check Destination MinIO Console at http://localhost:9011 to see the data files."
    print_msg "$CYAN" "Run '$0 query' to query the Iceberg tables with Trino."
}

# Query the Iceberg tables
query_table() {
    local table_filter="$1"

    # Validate table filter if provided
    if [ -n "$table_filter" ]; then
        case "$table_filter" in
            apilogs|errorlogs|auditlogs)
                print_header "Querying Iceberg Table: streaming.$table_filter"
                ;;
            *)
                print_msg "$RED" "Unknown table: $table_filter"
                print_msg "$YELLOW" "Valid tables: apilogs, errorlogs, auditlogs"
                exit 1
                ;;
        esac
    else
        print_header "Querying All Iceberg Tables"
    fi

    print_msg "$YELLOW" "Installing PyIceberg and querying tables..."

    docker run --rm --network kafka-connect_kafka-iceberg \
        -e AWS_ACCESS_KEY_ID=minioadmin \
        -e AWS_SECRET_ACCESS_KEY=minioadmin \
        -e AWS_REGION=us-east-1 \
        -e TABLE_FILTER="$table_filter" \
        python:3.11-slim bash -c "
        pip install -q pyiceberg[s3,pandas] boto3 2>/dev/null
        python3 << 'PYEOF'
import os
import json
from pyiceberg.catalog import load_catalog

table_filter = os.getenv('TABLE_FILTER', '')

print('Connecting to catalog on destination cluster...')
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
    print('\\nTables:', catalog.list_tables('streaming'))

    # Define all tables
    all_tables = [
        ('streaming.apilogs', 'API Logs'),
        ('streaming.errorlogs', 'Error Logs'),
        ('streaming.auditlogs', 'Audit Logs'),
    ]

    # Filter if specific table requested
    if table_filter:
        tables = [(f'streaming.{table_filter}', f'{table_filter.replace(\"logs\", \" Logs\").title()}')]
    else:
        tables = all_tables

    for table_name, description in tables:
        print('\\n' + '='*70)
        print(f'{description} ({table_name})')
        print('='*70)

        try:
            table = catalog.load_table(table_name)
            df = table.scan().to_pandas()

            if len(df) > 0:
                # Show summary columns
                summary_cols = ['time', 'name', 'type', 'bucket', 'object', 'node', 'origin']
                available_cols = [c for c in summary_cols if c in df.columns]
                if available_cols:
                    print(df[available_cols].tail(10 if table_filter else 5).to_string())
                else:
                    print(df.tail(10 if table_filter else 5).to_string())
                print(f'\\nTotal rows: {len(df)}')

                # Show full details of last row if querying single table
                if table_filter and len(df) > 0:
                    print('\\n' + '-'*70)
                    print('Last entry (full details):')
                    print('-'*70)
                    last_row = df.iloc[-1].to_dict()
                    for key, value in last_row.items():
                        if isinstance(value, dict):
                            print(f'\\n{key}:')
                            print(json.dumps(value, indent=2, default=str))
                        elif value is not None:
                            print(f'{key}: {value}')
            else:
                print('No data yet.')
        except Exception as e:
            print(f'Table not found or empty: {e}')

    print('\\n' + '='*70)
    print('To generate logs, interact with source MinIO cluster:')
    print('  mc alias set source http://localhost:9000 minioadmin minioadmin')
    print('  mc mb source/testbucket')
    print('  mc cp /etc/hosts source/testbucket/')
    print('='*70)

except Exception as e:
    print(f'Error: {e}')
    print('\\nTables may not exist yet. Interact with source MinIO to generate logs.')
PYEOF
        "
}

# Generate logs using the scripts in generate/ directory
# These scripts interact with the source MinIO cluster to generate real logs
generate_logs() {
    local log_type="$1"
    shift

    # Default count
    local count=100

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --count|-n)
                count="$2"
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done

    # Check if mc is available
    if ! command -v mc &> /dev/null; then
        print_msg "$RED" "Error: mc (MinIO Client) is not installed."
        print_msg "$YELLOW" "Please install mc: https://min.io/docs/minio/linux/reference/minio-mc.html"
        exit 1
    fi

    # Generate unique alias name with UUID
    local UUID=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen 2>/dev/null || echo "$$-$(date +%s)")
    local ALIAS="source-${UUID:0:8}"

    # Setup mc alias for source cluster with unique name
    print_msg "$YELLOW" "Setting up temporary mc alias: $ALIAS"
    mc alias set "$ALIAS" http://localhost:9000 minioadmin minioadmin > /dev/null 2>&1

    # Cleanup function to remove alias
    cleanup_alias() {
        print_msg "$YELLOW" "Cleaning up temporary alias: $ALIAS"
        mc alias rm "$ALIAS" > /dev/null 2>&1 || true
    }

    # Trap to ensure cleanup on exit (including Ctrl+C)
    trap cleanup_alias EXIT INT TERM

    case "$log_type" in
        api|api-logs)
            print_header "Generating API Logs on Source Cluster"
            print_msg "$CYAN" "This will generate $count API operations on the source MinIO cluster."
            print_msg "$CYAN" "Logs will flow: Source MinIO → Kafka (apilogs) → Iceberg → Dest MinIO"
            echo ""
            "$SCRIPT_DIR/generate/generate-api-logs.sh" "$ALIAS" --count "$count"
            ;;
        error|error-logs)
            print_header "Generating Error Logs on Source Cluster"
            print_msg "$CYAN" "This will generate $count error-triggering operations on source MinIO."
            print_msg "$CYAN" "Logs will flow: Source MinIO → Kafka (errorlogs) → Iceberg → Dest MinIO"
            echo ""
            "$SCRIPT_DIR/generate/generate-error-logs.sh" "$ALIAS" --count "$count"
            ;;
        audit|audit-logs)
            print_header "Generating Audit Logs on Source Cluster"
            print_msg "$CYAN" "This will generate $count audit-triggering operations on source MinIO."
            print_msg "$CYAN" "Logs will flow: Source MinIO → Kafka (auditlogs) → Iceberg → Dest MinIO"
            echo ""
            "$SCRIPT_DIR/generate/generate-audit-logs.sh" "$ALIAS" --count "$count"
            ;;
        all)
            print_header "Generating All Log Types on Source Cluster"
            print_msg "$CYAN" "This will generate $count operations for each log type."
            echo ""

            print_msg "$YELLOW" "=== API Logs ==="
            "$SCRIPT_DIR/generate/generate-api-logs.sh" "$ALIAS" --count "$count"
            echo ""

            print_msg "$YELLOW" "=== Error Logs ==="
            "$SCRIPT_DIR/generate/generate-error-logs.sh" "$ALIAS" --count "$count"
            echo ""

            print_msg "$YELLOW" "=== Audit Logs ==="
            "$SCRIPT_DIR/generate/generate-audit-logs.sh" "$ALIAS" --count "$count"
            ;;
        *)
            print_msg "$RED" "Unknown log type: $log_type"
            echo ""
            echo "Usage: $0 generate <type> [--count N]"
            echo ""
            echo "Log types:"
            echo "  api         Generate API logs (S3 operations)"
            echo "  error       Generate error logs (error-triggering operations)"
            echo "  audit       Generate audit logs (admin/IAM operations)"
            echo "  all         Generate all log types"
            echo ""
            echo "Options:"
            echo "  --count N   Number of operations to generate (default: 100)"
            echo ""
            echo "Examples:"
            echo "  $0 generate api"
            echo "  $0 generate error --count 500"
            echo "  $0 generate audit --count 200"
            echo "  $0 generate all --count 50"
            exit 1
            ;;
    esac

    echo ""
    print_msg "$GREEN" "✓ Log generation complete!"
    print_msg "$CYAN" "Check the Kafka topics and Iceberg tables:"
    echo "  - View Kafka topics: docker exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic apilogs --from-beginning"
    echo "  - Query with Trino:  docker exec -it trino trino"
    echo "  - Check Dest MinIO:  http://localhost:9011 → kafkawarehouse/streaming/"
}

# Generate logs continuously until Ctrl+C
# Usage: generate_logs_continuous [--interval SECONDS] [--batch SIZE]
generate_logs_continuous() {
    local interval=2
    local batch_size=5

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --interval|-i)
                interval="$2"
                shift 2
                ;;
            --batch|-b)
                batch_size="$2"
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done

    print_header "Continuous Log Generation Mode"

    # Load config to get port settings
    if [ -f "${PROJECT_ROOT}/.env" ]; then
        set -a
        source "${PROJECT_ROOT}/.env"
        set +a
    fi

    local source_api_port="${SOURCE_API_PORT:-9000}"

    if ! curl -sf "http://localhost:${source_api_port}/minio/health/live" >/dev/null 2>&1; then
        print_msg "$RED" "Error: Source MinIO is not running on port ${source_api_port}"
        exit 1
    fi

    print_msg "$YELLOW" "Generating logs continuously..."
    print_msg "$CYAN" "  Interval: ${interval}s between batches"
    print_msg "$CYAN" "  Batch size: ${batch_size} operations per batch"
    print_msg "$CYAN" ""
    print_msg "$GREEN" "Press Ctrl+C to stop"
    print_msg "$CYAN" ""

    # Trap Ctrl+C
    trap 'echo ""; print_msg "$YELLOW" "Stopping continuous log generation..."; exit 0' INT TERM

    local iteration=0
    local total_ops=0

    # Create test bucket once
    docker run --rm --network kafka-connect_kafka-iceberg \
        -e MC_HOST_source=http://minioadmin:minioadmin@nginx-source:9000 \
        minio/mc:latest mc mb source/continuous-logs-bucket --ignore-existing 2>/dev/null

    while true; do
        iteration=$((iteration + 1))
        local timestamp=$(date '+%Y-%m-%d %H:%M:%S')

        # Run batch of operations
        docker run --rm --network kafka-connect_kafka-iceberg \
            -e MC_HOST_source=http://minioadmin:minioadmin@nginx-source:9000 \
            minio/mc:latest bash -c "
            for i in \$(seq 1 ${batch_size}); do
                # Mix of operations: put, get, list, stat
                op=\$((RANDOM % 4))
                obj_id=\"obj-\${RANDOM}\"

                case \$op in
                    0) # PUT
                        echo \"data-\$(date +%s)-\$i\" | mc pipe source/continuous-logs-bucket/\${obj_id}.txt 2>/dev/null
                        ;;
                    1) # GET (may fail if object doesn't exist, that's fine)
                        mc cat source/continuous-logs-bucket/\${obj_id}.txt 2>/dev/null || true
                        ;;
                    2) # LIST
                        mc ls source/continuous-logs-bucket/ >/dev/null 2>&1
                        ;;
                    3) # STAT
                        mc stat source/continuous-logs-bucket/\${obj_id}.txt 2>/dev/null || true
                        ;;
                esac
            done
            " 2>/dev/null

        total_ops=$((total_ops + batch_size))
        printf "\r${CYAN}[%s] Iteration: %d | Total operations: %d${NC}" "$timestamp" "$iteration" "$total_ops"

        sleep "$interval"
    done
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
            shift
            produce_messages "$@"
            ;;
        query)
            shift
            query_table "$1"
            ;;
        generate)
            shift
            generate_logs "$@"
            ;;
        continuous)
            shift
            generate_logs_continuous "$@"
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
