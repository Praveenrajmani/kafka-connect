#!/bin/bash

# Script to generate server-side error logs for MinIO AIStor
# Usage: ./generate-error-logs.sh [alias] [--count N] [--target-alias TARGET]
#
# Based on source code analysis, this script triggers operations that generate
# actual server-side error logs via subsystem loggers:
# - storageLogIf() - Storage layer errors
# - healingLogIf() - Healing operation errors
# - replLogIf() - Replication errors
# - iamLogIf() - IAM/authentication errors
# - transitionLogIf() - Lifecycle/tiering errors
# - batchLogIf() - Batch job errors
# - configLogIf() - Configuration errors

set -e

ALIAS="myminio"
TARGET_ALIAS=""
COUNT=100

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --count)
            COUNT="$2"
            shift 2
            ;;
        --target-alias)
            TARGET_ALIAS="$2"
            shift 2
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Usage: $0 [alias] [--count N] [--target-alias TARGET]"
            echo "Example: $0 myminio --count 500 --target-alias remoteminio"
            exit 1
            ;;
        *)
            ALIAS="$1"
            shift
            ;;
    esac
done

# Validate count is a positive integer
if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -le 0 ]; then
    echo "Error: --count must be a positive integer"
    exit 1
fi

# Check if mc is available
if ! command -v mc &> /dev/null; then
    echo "Error: mc command not found. Please install MinIO Client."
    exit 1
fi

# Check if alias exists
if ! mc alias list "$ALIAS" &> /dev/null; then
    echo "Error: Alias '$ALIAS' not found. Please set up the alias first."
    echo "Example: mc alias set $ALIAS http://localhost:9000 minioadmin minioadmin"
    exit 1
fi

# Check target alias if provided
if [ -n "$TARGET_ALIAS" ]; then
    if ! mc alias list "$TARGET_ALIAS" &> /dev/null; then
        echo "Error: Target alias '$TARGET_ALIAS' not found."
        exit 1
    fi
fi

echo "Generating server-side error logs for alias: $ALIAS"
if [ -n "$TARGET_ALIAS" ]; then
    echo "Target alias for multi-site error scenarios: $TARGET_ALIAS"
fi
echo "============================================"
echo "Target operations: $COUNT"
echo "============================================"
echo ""

# Create test resources
BUCKET="test-error-logs-$(date +%s)"
REPL_BUCKET="test-error-repl-$(date +%s)"
TEST_USER="error-test-user-$$"
TEST_POLICY="error-test-policy-$$"
NONEXISTENT_TIER="nonexistent-tier-$$"

echo "Creating test bucket: $BUCKET..."
mc mb "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
mc version enable "$ALIAS/$BUCKET" > /dev/null 2>&1 || true

# Upload test objects
echo "Uploading test objects..."
for j in {1..5}; do
    dd if=/dev/urandom bs=1024 count=10 2>/dev/null | mc pipe "$ALIAS/$BUCKET/test-object-$j.bin" > /dev/null 2>&1 || true
done

# Create target bucket for replication tests
if [ -n "$TARGET_ALIAS" ]; then
    mc mb "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
    mc version enable "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
fi

COUNTER=0
START_TIME=$(date +%s)

echo "Generating $COUNT error-triggering operations..."
echo ""
echo "Error categories being triggered:"
echo "  - IAM errors (invalid credentials, policy conflicts)"
echo "  - Replication errors (invalid targets, failed syncs)"
echo "  - Healing errors (corrupt metadata scenarios)"
echo "  - Configuration errors (invalid settings)"
echo "  - Tiering errors (non-existent tier references)"
echo "  - Batch job errors (invalid job specs)"
echo ""

for ((i=1; i<=COUNT; i++)); do
    OP_INDEX=$((RANDOM % 100))

    if [ $OP_INDEX -lt 15 ]; then
        # IAM errors: Create user with invalid credentials (iamLogIf)
        # Invalid access key format triggers IAM validation errors
        mc admin user add "$ALIAS" "invalid user with spaces" "short" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 25 ]; then
        # IAM errors: Attach non-existent policy (iamLogIf)
        mc admin policy attach "$ALIAS" "nonexistent-policy-$RANDOM" --user "nonexistent-user-$RANDOM" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 35 ]; then
        # IAM errors: Invalid policy document (iamLogIf)
        echo '{"invalid": "policy"}' | mc admin policy create "$ALIAS" "invalid-policy-$RANDOM" /dev/stdin > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 42 ]; then
        # Configuration errors: Set invalid config values (configLogIf)
        mc admin config set "$ALIAS" api requests_max=-1 > /dev/null 2>&1 || true
        mc admin config set "$ALIAS" scanner delay=invalid > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 49 ]; then
        # Healing errors: Trigger heal on non-existent paths (healingLogIf)
        mc admin heal "$ALIAS/$BUCKET/nonexistent-prefix-$RANDOM/" --recursive > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 56 ]; then
        # Tiering errors: Reference non-existent tier in lifecycle (transitionLogIf)
        LIFECYCLE_INVALID=$(cat <<EOF
{
    "Rules": [{
        "ID": "transition-to-invalid-tier",
        "Status": "Enabled",
        "Filter": {"Prefix": "archive/"},
        "Transition": {"Days": 1, "StorageClass": "$NONEXISTENT_TIER"}
    }]
}
EOF
)
        echo "$LIFECYCLE_INVALID" | mc ilm import "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 63 ]; then
        # Replication errors: Configure replication to invalid endpoint (replLogIf)
        mc replicate add "$ALIAS/$BUCKET" \
            --remote-bucket "http://invalid-endpoint:9000/nonexistent" \
            --replicate "delete,delete-marker,existing-objects" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 70 ]; then
        # Batch job errors: Submit invalid batch job (batchLogIf)
        BATCH_INVALID=$(cat <<EOF
replicate:
  apiVersion: v1
  source:
    type: minio
    bucket: nonexistent-source-bucket
    prefix: ""
  target:
    type: minio
    bucket: nonexistent-target-bucket
    endpoint: "http://invalid:9000"
    credentials:
      accessKey: "invalid"
      secretKey: "invalid"
EOF
)
        echo "$BATCH_INVALID" | mc batch start "$ALIAS" /dev/stdin > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 77 ]; then
        # Object lock errors: Invalid retention on non-locked bucket
        mc retention set --default governance 30d "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 84 ]; then
        # Encryption errors: Reference non-existent KMS key (encLogIf)
        mc encrypt set sse-kms "nonexistent-key-$RANDOM" "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ -n "$TARGET_ALIAS" ] && [ $OP_INDEX -lt 90 ]; then
        # Site replication errors: Add invalid peer (siteReplLogOnceIf)
        mc admin replicate add "$ALIAS" "http://invalid-peer-$RANDOM:9000" > /dev/null 2>&1 || true
    elif [ -n "$TARGET_ALIAS" ] && [ $OP_INDEX -lt 95 ]; then
        # Cross-cluster replication with invalid credentials
        mc replicate add "$ALIAS/$BUCKET" \
            --remote-bucket "$TARGET_ALIAS/$REPL_BUCKET" \
            --replicate "delete,delete-marker" \
            --priority 1 > /dev/null 2>&1 || true
        # Then break it by removing target bucket
        mc rb --force "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
        # Upload to trigger replication failure
        echo "trigger-repl-error-$i" | mc pipe "$ALIAS/$BUCKET/repl-test-$RANDOM.txt" > /dev/null 2>&1 || true
        # Recreate for next iteration
        mc mb "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
        mc version enable "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
    else
        # Storage errors: Operations that may trigger storage layer logging
        # Large metadata operations, rapid create/delete cycles
        for k in {1..10}; do
            OBJ="stress-$RANDOM-$k.txt"
            echo "data" | mc pipe "$ALIAS/$BUCKET/$OBJ" > /dev/null 2>&1 || true
            mc rm "$ALIAS/$BUCKET/$OBJ" > /dev/null 2>&1 || true
        done
    fi

    COUNTER=$((COUNTER + 1))

    # Progress update
    UPDATE_INTERVAL=$((COUNT / 10))
    if [ $UPDATE_INTERVAL -gt 50 ]; then
        UPDATE_INTERVAL=50
    fi
    if [ $UPDATE_INTERVAL -lt 1 ]; then
        UPDATE_INTERVAL=1
    fi

    if [ $((COUNTER % UPDATE_INTERVAL)) -eq 0 ]; then
        ELAPSED=$(($(date +%s) - START_TIME))
        PERCENT=$((COUNTER * 100 / COUNT))
        OPS_PER_SEC=$((COUNTER / (ELAPSED > 0 ? ELAPSED : 1)))
        echo "Progress: $COUNTER/$COUNT ($PERCENT%) - $OPS_PER_SEC ops/sec"
    fi
done

TOTAL_TIME=$(($(date +%s) - START_TIME))
AVG_OPS_PER_SEC=$((COUNT / (TOTAL_TIME > 0 ? TOTAL_TIME : 1)))

echo ""
echo "Cleanup: Removing test resources..."

# Remove replication rules first
mc replicate rm "$ALIAS/$BUCKET" --all --force > /dev/null 2>&1 || true

# Remove lifecycle rules
mc ilm rm "$ALIAS/$BUCKET" --all --force > /dev/null 2>&1 || true

# Remove test bucket
mc rm --recursive --force "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
mc rb "$ALIAS/$BUCKET" > /dev/null 2>&1 || true

# Remove target bucket
if [ -n "$TARGET_ALIAS" ]; then
    mc rm --recursive --force "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
    mc rb "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
fi

# Remove test policies created
mc admin policy remove "$ALIAS" "invalid-policy-*" > /dev/null 2>&1 || true

echo ""
echo "============================================"
echo "Error log generation complete!"
echo "============================================"
echo "Total operations: $COUNT"
echo "Total time: ${TOTAL_TIME}s"
echo "Average throughput: $AVG_OPS_PER_SEC ops/sec"
echo ""
echo "Server-side error logs triggered (check MinIO server logs):"
echo "  - iamLogIf: IAM validation failures, policy errors"
echo "  - configLogIf: Invalid configuration values"
echo "  - healingLogIf: Healing operation failures"
echo "  - transitionLogIf: Tiering/lifecycle errors"
echo "  - replLogIf: Replication configuration/sync errors"
echo "  - batchLogIf: Batch job failures"
echo "  - encLogIf: Encryption/KMS errors"
if [ -n "$TARGET_ALIAS" ]; then
    echo "  - siteReplLogOnceIf: Site replication peer errors"
fi
