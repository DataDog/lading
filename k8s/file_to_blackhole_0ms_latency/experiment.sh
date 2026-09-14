#!/bin/bash
set -e

# Parse arguments
AGENT_MEMORY_MB=""
DURATION=300
DD_TAGS_VALUE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --agent-memory)
            AGENT_MEMORY_MB="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --tags)
            DD_TAGS_VALUE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 --agent-memory <MB> [--duration <seconds>] --tags <DD_TAGS>"
            exit 1
            ;;
    esac
done

if [ -z "$AGENT_MEMORY_MB" ]; then
    echo "ERROR: --agent-memory is required"
    echo "Usage: $0 --agent-memory <MB> [--duration <seconds>] --tags <DD_TAGS>"
    exit 1
fi

if [ -z "$DD_TAGS_VALUE" ]; then
    echo "ERROR: --tags is required"
    echo "Usage: $0 --agent-memory <MB> [--duration <seconds>] --tags <DD_TAGS>"
    exit 1
fi

echo "========================================"
echo "Datadog Agent Log Tailing Test"
echo "========================================"
echo "Agent memory limit: ${AGENT_MEMORY_MB} MB"
echo "Test duration: ${DURATION} seconds"
echo "Tags: ${DD_TAGS_VALUE}"
echo "Started at: $(date)"
echo

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "[1/6] Checking prerequisites..."
command -v kind >/dev/null 2>&1 || { echo "ERROR: kind not found"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not found"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "ERROR: helm not found"; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq not found"; exit 1; }
echo "      ✓ Prerequisites available"
echo

echo "[2/6] Creating fresh cluster..."
if kind get clusters 2>/dev/null | grep -q "^lading-test$"; then
    echo "      Deleting existing cluster..."
    kind delete cluster --name lading-test
fi
kind create cluster --name lading-test
echo "      ✓ Cluster ready"
echo

echo "[3/6] Installing Prometheus..."
kubectl create namespace monitoring 2>/dev/null || true
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
helm repo update >/dev/null 2>&1
helm install prometheus prometheus-community/prometheus \
    --namespace monitoring \
    --set server.service.type=ClusterIP \
    --set alertmanager.enabled=false \
    --set prometheus-pushgateway.enabled=false \
    --set kube-state-metrics.enabled=true >/dev/null 2>&1
echo "      ✓ Prometheus installed"
echo

echo "[4/6] Installing Datadog Operator..."
helm repo add datadog https://helm.datadoghq.com >/dev/null 2>&1 || true
helm repo update >/dev/null 2>&1
helm install datadog-operator datadog/datadog-operator --version 2.15.2 >/dev/null 2>&1
echo "      Waiting for operator..."
kubectl wait --for=condition=available --timeout=120s deployment/datadog-operator 2>/dev/null || sleep 30
echo "      ✓ Operator ready"
echo

echo "[5/6] Applying manifests..."
kubectl apply -f "$SCRIPT_DIR/manifests/datadog-secret.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/deny-egress.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/lading-intake.yaml"

# Deploy lading first so logs exist before agent starts
kubectl apply -f "$SCRIPT_DIR/manifests/lading.yaml"
echo "      ✓ Lading file generator deployed"

# Wait for lading to create log files
echo "      Waiting for log files..."
TIMEOUT=120
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    LADING_POD=$(kubectl get pods -l app=lading -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "$LADING_POD" ]; then
        POD_READY=$(kubectl get pod "$LADING_POD" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
        if [ "$POD_READY" = "true" ]; then
            LOG_COUNT=$(kubectl exec "$LADING_POD" -- sh -c 'ls /var/log/lading/*.log 2>/dev/null | wc -l' || echo 0)
            if [ "$LOG_COUNT" -gt 0 ]; then
                echo "      ✓ Log files created ($LOG_COUNT files)"
                break
            fi
        fi
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "      ✗ Timeout waiting for log files"
    kubectl logs -l app=lading --tail=20
    exit 1
fi

# Now deploy agent
AGENT_MANIFEST=$(cat "$SCRIPT_DIR/manifests/datadog-agent.yaml" | \
    sed "s/{{ AGENT_MEMORY_MB }}/${AGENT_MEMORY_MB}/g" | \
    sed "s|{{ DD_TAGS }}|${DD_TAGS_VALUE}|g")

if echo "$AGENT_MANIFEST" | grep -q "{{ AGENT_MEMORY_MB }}"; then
    echo "      ✗ ERROR: Template substitution failed for memory placeholder"
    exit 1
fi
if echo "$AGENT_MANIFEST" | grep -q "{{ DD_TAGS }}"; then
    echo "      ✗ ERROR: Template substitution failed for DD_TAGS"
    exit 1
fi

echo "$AGENT_MANIFEST" | kubectl apply -f -
echo "      ✓ Agent deployed (egress blocked)"

echo "      Waiting for agent..."
TIMEOUT=120
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    AGENT_POD=$(kubectl get pods -l app.kubernetes.io/name=datadog-agent-deployment -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "$AGENT_POD" ]; then
        READY=$(kubectl get pod "$AGENT_POD" -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
        if [ "$READY" = "true" ]; then
            echo "      ✓ Agent ready"
            break
        fi
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "      ✗ Timeout waiting for agent"
    kubectl describe pods -l app.kubernetes.io/name=datadog-agent-deployment
    exit 1
fi

# Check for any failed pods
FAILED_PODS=$(kubectl get pods -o json | jq -r '.items[] | select(.status.phase == "Failed" or .status.phase == "Unknown") | .metadata.name')
if [ -n "$FAILED_PODS" ]; then
    echo "      ✗ Found failed pods:"
    kubectl get pods
    exit 1
fi
echo "      ✓ All systems healthy"
echo

# Monitor for restarts
echo "[6/6] Monitoring for restarts (${DURATION}s)..."
echo "      Started at: $(date)"
ELAPSED=0
LAST_REPORT=0

while [ $ELAPSED -lt $DURATION ]; do
    RESTART_DATA=$(kubectl get pods -l app.kubernetes.io/name=datadog-agent-deployment -o json 2>/dev/null)
    if [ $? -ne 0 ]; then
        sleep 5
        ELAPSED=$((ELAPSED + 5))
        continue
    fi

    RESTART_COUNT=$(echo "$RESTART_DATA" | jq '[.items[].status.containerStatuses[]?.restartCount // 0] | add' 2>/dev/null || echo 0)
    if [ -z "$RESTART_COUNT" ] || [ "$RESTART_COUNT" = "null" ]; then
        RESTART_COUNT=0
    fi

    if [ $((ELAPSED - LAST_REPORT)) -ge 30 ]; then
        REMAINING=$((DURATION - ELAPSED))
        echo "      ${ELAPSED}s elapsed, ${REMAINING}s remaining (restarts: ${RESTART_COUNT})"
        LAST_REPORT=$ELAPSED
    fi

    if [ "$RESTART_COUNT" -gt 0 ]; then
        CONTAINER_NAME=$(echo "$RESTART_DATA" | jq -r '.items[].status.containerStatuses[]? | select(.restartCount > 0) | .name' 2>/dev/null | head -1)
        REASON=$(echo "$RESTART_DATA" | jq -r '.items[].status.containerStatuses[]? | select(.restartCount > 0) | .lastState.terminated.reason // "Unknown"' 2>/dev/null | head -1)

        echo
        echo "========================================"
        echo "RESULT: FAILURE"
        echo "========================================"
        echo "Container restarted: ${CONTAINER_NAME}"
        echo "Restart count: ${RESTART_COUNT}"
        echo "Reason: ${REASON}"
        echo "Time to failure: ${ELAPSED}s"
        echo

        if [ "$REASON" = "OOMKilled" ]; then
            echo "💡 Container needs MORE memory"
        else
            echo "⚠️  Non-OOM restart:"
            kubectl logs -l app.kubernetes.io/name=datadog-agent-deployment -c "${CONTAINER_NAME}" --previous --tail=20
        fi
        echo "========================================"
        exit 1
    fi

    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

echo "      Completed at: $(date)"
echo

echo "========================================"
echo "RESULT: SUCCESS"
echo "========================================"
echo "No restarts detected"
echo "Test duration: ${DURATION} seconds"
echo "Tags: ${DD_TAGS_VALUE}"
echo

echo "💡 Agent stable - cluster is still running for examination"
echo "   View lading metrics: kubectl port-forward svc/lading 9000:9000"
echo "   View agent logs: kubectl logs -l app.kubernetes.io/name=datadog-agent-deployment -f"
