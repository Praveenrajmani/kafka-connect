#!/bin/bash

# Script to generate server-side audit logs for MinIO AIStor
# Usage: ./generate-audit-logs.sh [alias] [--count N] [--target-alias TARGET]
#
# Based on source code analysis, this script triggers operations that generate
# audit log entries via logger.AuditLog() and logger.Record*AuditLog():
#
# Audit Categories Triggered:
# - User: User account management (create/delete/status)
# - ServiceAccount: Service account operations
# - Policy: Policy management (create/attach/detach)
# - Group: Group management
# - Config: Configuration changes
# - Bucket: Bucket operations, quota, QoS
# - Lifecycle: Lifecycle policy changes
# - Replication: Bucket replication configuration
# - Versioning: Versioning configuration
# - Encryption: Encryption configuration
# - CORS: CORS policy changes
# - Notification: Event notification config
# - SiteRepl: Multi-site replication operations
# - Batch: Batch job operations
# - Heal: Healing operations
# - Service: Server status/info operations

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

echo "Generating audit logs for alias: $ALIAS"
if [ -n "$TARGET_ALIAS" ]; then
    echo "Target alias for multi-site audit events: $TARGET_ALIAS"
fi
echo "============================================"
echo "Target operations: $COUNT"
echo "============================================"
echo ""

# Create test resources
BUCKET="test-audit-logs-$(date +%s)"
REPL_BUCKET="test-audit-repl-$(date +%s)"
TEST_USER="audit-user-$$"
TEST_GROUP="audit-group-$$"
TEST_POLICY="audit-policy-$$"
TEST_SVCACCT=""

echo "Creating test bucket: $BUCKET..."
mc mb "$ALIAS/$BUCKET" > /dev/null 2>&1 || true

# Upload test objects
echo "Uploading test objects..."
for j in {1..5}; do
    echo "test data $j $(date)" | mc pipe "$ALIAS/$BUCKET/test-object-$j.txt" > /dev/null 2>&1 || true
done

# Create target bucket for replication tests
if [ -n "$TARGET_ALIAS" ]; then
    echo "Creating target bucket: $REPL_BUCKET on $TARGET_ALIAS..."
    mc mb "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
    mc version enable "$TARGET_ALIAS/$REPL_BUCKET" > /dev/null 2>&1 || true
fi

# Create test user for IAM audit events
echo "Creating test user: $TEST_USER..."
mc admin user add "$ALIAS" "$TEST_USER" "password123456" > /dev/null 2>&1 || true

# Create test group
echo "Creating test group: $TEST_GROUP..."
mc admin group add "$ALIAS" "$TEST_GROUP" "$TEST_USER" > /dev/null 2>&1 || true

# Create test policy
echo "Creating test policy: $TEST_POLICY..."
POLICY_DOC=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:ListBucket"],
        "Resource": ["arn:aws:s3:::$BUCKET/*", "arn:aws:s3:::$BUCKET"]
    }]
}
EOF
)
echo "$POLICY_DOC" | mc admin policy create "$ALIAS" "$TEST_POLICY" /dev/stdin > /dev/null 2>&1 || true

COUNTER=0
START_TIME=$(date +%s)

echo ""
echo "Generating $COUNT audit-triggering operations..."
echo ""
echo "Audit categories being triggered:"
echo "  - User: User account management"
echo "  - ServiceAccount: Service account operations"
echo "  - Policy: Policy management"
echo "  - Group: Group membership changes"
echo "  - Config: Configuration queries"
echo "  - Bucket: Bucket configuration (quota, tags)"
echo "  - Lifecycle: Lifecycle rule management"
echo "  - Replication: Replication configuration"
echo "  - Versioning: Bucket versioning"
echo "  - Encryption: Bucket encryption settings"
echo "  - CORS: Cross-origin resource sharing"
echo "  - Notification: Event notifications"
if [ -n "$TARGET_ALIAS" ]; then
    echo "  - SiteRepl: Site replication status"
    echo "  - Batch: Batch job operations"
fi
echo "  - Heal: Healing status queries"
echo "  - Service: Server info/status"
echo ""

for ((i=1; i<=COUNT; i++)); do
    OP_INDEX=$((RANDOM % 100))

    if [ $OP_INDEX -lt 8 ]; then
        # Category: User - User status toggle (Record audit with Action: Update)
        mc admin user disable "$ALIAS" "$TEST_USER" > /dev/null 2>&1 || true
        mc admin user enable "$ALIAS" "$TEST_USER" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 14 ]; then
        # Category: User - List users
        mc admin user list "$ALIAS" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 20 ]; then
        # Category: ServiceAccount - Create and delete service account
        SVCACCT_OUTPUT=$(mc admin user svcacct add "$ALIAS" "$TEST_USER" --json 2>/dev/null || echo "{}")
        TEST_SVCACCT=$(echo "$SVCACCT_OUTPUT" | grep -o '"accessKey":"[^"]*"' | cut -d'"' -f4 || echo "")
        if [ -n "$TEST_SVCACCT" ]; then
            mc admin user svcacct rm "$ALIAS" "$TEST_SVCACCT" > /dev/null 2>&1 || true
        fi
    elif [ $OP_INDEX -lt 26 ]; then
        # Category: ServiceAccount - List service accounts
        mc admin user svcacct list "$ALIAS" "$TEST_USER" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 32 ]; then
        # Category: Policy - Attach/detach policy (Record audit with Action: Attach/Detach)
        mc admin policy attach "$ALIAS" "$TEST_POLICY" --user "$TEST_USER" > /dev/null 2>&1 || true
        mc admin policy detach "$ALIAS" "$TEST_POLICY" --user "$TEST_USER" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 38 ]; then
        # Category: Policy - List policies
        mc admin policy list "$ALIAS" > /dev/null 2>&1 || true
        mc admin policy info "$ALIAS" "$TEST_POLICY" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 44 ]; then
        # Category: Group - Update group membership
        mc admin group add "$ALIAS" "$TEST_GROUP" "$TEST_USER" > /dev/null 2>&1 || true
        mc admin group info "$ALIAS" "$TEST_GROUP" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 50 ]; then
        # Category: Config - Get configuration (audited query)
        mc admin config get "$ALIAS" api > /dev/null 2>&1 || true
        mc admin config get "$ALIAS" scanner > /dev/null 2>&1 || true
        mc admin config get "$ALIAS" heal > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 55 ]; then
        # Category: Bucket - Bucket tags (S3 API audit)
        mc tag set "$ALIAS/$BUCKET" "env=test&audit=true&iter=$i" > /dev/null 2>&1 || true
        mc tag list "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc tag remove "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 60 ]; then
        # Category: Versioning - Enable/suspend versioning (S3 API audit)
        mc version enable "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc version info "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 65 ]; then
        # Category: Lifecycle - Lifecycle rule management
        LIFECYCLE_JSON=$(cat <<EOF
{
    "Rules": [{
        "ID": "expire-old-$i",
        "Status": "Enabled",
        "Filter": {"Prefix": "temp/"},
        "Expiration": {"Days": 30}
    }]
}
EOF
)
        echo "$LIFECYCLE_JSON" | mc ilm import "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc ilm ls "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc ilm rm "$ALIAS/$BUCKET" --id "expire-old-$i" --force > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 70 ]; then
        # Category: Encryption - Bucket encryption settings
        mc encrypt set sse-s3 "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc encrypt info "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc encrypt clear "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 75 ]; then
        # Category: Notification - Event notification (query only, setup requires target)
        mc event list "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 80 ]; then
        # Category: Heal - Healing status queries
        mc admin heal "$ALIAS" --dry-run > /dev/null 2>&1 || true
    elif [ $OP_INDEX -lt 85 ]; then
        # Category: Service - Server info and status
        mc admin info "$ALIAS" > /dev/null 2>&1 || true
    elif [ -n "$TARGET_ALIAS" ] && [ $OP_INDEX -lt 90 ]; then
        # Category: SiteRepl - Site replication info queries
        mc admin replicate info "$ALIAS" > /dev/null 2>&1 || true
    elif [ -n "$TARGET_ALIAS" ] && [ $OP_INDEX -lt 93 ]; then
        # Category: Replication - Bucket replication configuration
        mc replicate ls "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc replicate status "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
    elif [ -n "$TARGET_ALIAS" ] && [ $OP_INDEX -lt 96 ]; then
        # Category: Batch - Batch job operations
        mc batch list "$ALIAS" > /dev/null 2>&1 || true
    elif [ -n "$TARGET_ALIAS" ] && [ $OP_INDEX -lt 98 ]; then
        # Multi-site: Cross-cluster object copy (generates S3 audit on both sides)
        OBJ_IDX=$((RANDOM % 5 + 1))
        mc cp "$ALIAS/$BUCKET/test-object-$OBJ_IDX.txt" "$TARGET_ALIAS/$REPL_BUCKET/audit-copy-$RANDOM.txt" > /dev/null 2>&1 || true
    else
        # S3 API operations (standard HTTP audit via logger.AuditLog)
        # These generate audit entries for: ListBuckets, ListObjects, GetObject, PutObject
        mc ls "$ALIAS" > /dev/null 2>&1 || true
        mc ls "$ALIAS/$BUCKET" > /dev/null 2>&1 || true
        mc stat "$ALIAS/$BUCKET/test-object-1.txt" > /dev/null 2>&1 || true
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

# Remove policy attachment first
mc admin policy detach "$ALIAS" "$TEST_POLICY" --user "$TEST_USER" > /dev/null 2>&1 || true

# Remove test group
mc admin group remove "$ALIAS" "$TEST_GROUP" "$TEST_USER" > /dev/null 2>&1 || true

# Remove test policy
mc admin policy remove "$ALIAS" "$TEST_POLICY" > /dev/null 2>&1 || true

# Remove test user
mc admin user remove "$ALIAS" "$TEST_USER" > /dev/null 2>&1 || true

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

echo ""
echo "============================================"
echo "Audit log generation complete!"
echo "============================================"
echo "Total operations: $COUNT"
echo "Total time: ${TOTAL_TIME}s"
echo "Average throughput: $AVG_OPS_PER_SEC ops/sec"
echo ""
echo "Audit log categories triggered:"
echo ""
echo "  Admin API Audits (logger.Record*AuditLog):"
echo "    - User: Account enable/disable, list users"
echo "    - ServiceAccount: Create/delete/list service accounts"
echo "    - Policy: Create/attach/detach/list policies"
echo "    - Group: Add members, group info"
echo "    - Config: Configuration queries"
echo "    - Heal: Healing status queries"
echo "    - Service: Server info queries"
echo ""
echo "  S3 API Audits (logger.AuditLog):"
echo "    - Bucket: Tags, versioning, encryption"
echo "    - Lifecycle: Rule create/delete/list"
echo "    - Notification: Event list"
echo "    - Object: List, stat, copy operations"
if [ -n "$TARGET_ALIAS" ]; then
    echo ""
    echo "  Multi-site Audits:"
    echo "    - SiteRepl: Site replication info"
    echo "    - Replication: Bucket replication status"
    echo "    - Batch: Batch job listing"
    echo "    - Cross-cluster: Object copy operations"
fi
