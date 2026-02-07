#!/bin/bash
set -e

# Parse arguments
TOTAL_LIMIT=""
AGENT_MEMORY_MB=""
TRACE_MEMORY_MB=""
SYSPROBE_MEMORY_MB=""
PROCESS_MEMORY_MB=""
DURATION=300
DD_TAGS_VALUE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --total-limit)
            TOTAL_LIMIT="$2"
            shift 2
            ;;
        --agent-memory)
            AGENT_MEMORY_MB="$2"
            shift 2
            ;;
        --trace-memory)
            TRACE_MEMORY_MB="$2"
            shift 2
            ;;
        --sysprobe-memory)
            SYSPROBE_MEMORY_MB="$2"
            shift 2
            ;;
        --process-memory)
            PROCESS_MEMORY_MB="$2"
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
            echo "Usage: $0 --total-limit <MB> --agent-memory <MB> --trace-memory <MB> --sysprobe-memory <MB> --process-memory <MB> [--duration <seconds>] --tags <DD_TAGS>"
            exit 1
            ;;
    esac
done

if [ -z "$TOTAL_LIMIT" ]; then
    echo "ERROR: --total-limit is required"
    echo "Usage: $0 --total-limit <MB> --agent-memory <MB> --trace-memory <MB> --sysprobe-memory <MB> --process-memory <MB> --tags <DD_TAGS>"
    exit 1
fi

if [ -z "$AGENT_MEMORY_MB" ]; then
    echo "ERROR: --agent-memory is required"
    echo "Usage: $0 --total-limit <MB> --agent-memory <MB> --trace-memory <MB> --sysprobe-memory <MB> --process-memory <MB> --tags <DD_TAGS>"
    exit 1
fi

if [ -z "$TRACE_MEMORY_MB" ]; then
    echo "ERROR: --trace-memory is required"
    echo "Usage: $0 --total-limit <MB> --agent-memory <MB> --trace-memory <MB> --sysprobe-memory <MB> --process-memory <MB> --tags <DD_TAGS>"
    exit 1
fi

if [ -z "$SYSPROBE_MEMORY_MB" ]; then
    echo "ERROR: --sysprobe-memory is required"
    echo "Usage: $0 --total-limit <MB> --agent-memory <MB> --trace-memory <MB> --sysprobe-memory <MB> --process-memory <MB> --tags <DD_TAGS>"
    exit 1
fi

if [ -z "$PROCESS_MEMORY_MB" ]; then
    echo "ERROR: --process-memory is required"
    echo "Usage: $0 --total-limit <MB> --agent-memory <MB> --trace-memory <MB> --sysprobe-memory <MB> --process-memory <MB> --tags <DD_TAGS>"
    exit 1
fi

if [ -z "$DD_TAGS_VALUE" ]; then
    echo "ERROR: --tags is required"
    echo "Usage: $0 --total-limit <MB> --agent-memory <MB> --trace-memory <MB> --sysprobe-memory <MB> --process-memory <MB> --tags <DD_TAGS>"
    exit 1
fi

# Verify individual limits sum to total.
CALCULATED_TOTAL=$((AGENT_MEMORY_MB + TRACE_MEMORY_MB + SYSPROBE_MEMORY_MB + PROCESS_MEMORY_MB))
if [ "$CALCULATED_TOTAL" -ne "$TOTAL_LIMIT" ]; then
    echo "ERROR: Individual memory limits do not sum to total limit"
    echo "Total limit: ${TOTAL_LIMIT} MB"
    echo "Sum of individual limits: ${CALCULATED_TOTAL} MB (agent=${AGENT_MEMORY_MB} + trace=${TRACE_MEMORY_MB} + sysprobe=${SYSPROBE_MEMORY_MB} + process=${PROCESS_MEMORY_MB})"
    exit 1
fi

TOTAL_MEMORY_MB=$TOTAL_LIMIT

echo "========================================"
echo "Datadog Agent Memory Limit Test"
echo "========================================"
echo "Memory limits per container:"
echo "  agent:        ${AGENT_MEMORY_MB} MB"
echo "  trace-agent:  ${TRACE_MEMORY_MB} MB"
echo "  system-probe: ${SYSPROBE_MEMORY_MB} MB"
echo "  process-agent: ${PROCESS_MEMORY_MB} MB"
echo "  TOTAL:        ${TOTAL_MEMORY_MB} MB"
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
command -v bc >/dev/null 2>&1 || { echo "ERROR: bc not found"; exit 1; }
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

echo "[5/6] Applying manifests with ${TOTAL_MEMORY_MB} MB limit..."
kubectl apply -f "$SCRIPT_DIR/manifests/datadog-secret.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/deny-egress.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/lading-intake.yaml"

AGENT_MANIFEST=$(cat "$SCRIPT_DIR/manifests/datadog-agent.yaml" | \
    sed "s/{{ AGENT_MEMORY_MB }}/${AGENT_MEMORY_MB}/g" | \
    sed "s/{{ TRACE_MEMORY_MB }}/${TRACE_MEMORY_MB}/g" | \
    sed "s/{{ SYSPROBE_MEMORY_MB }}/${SYSPROBE_MEMORY_MB}/g" | \
    sed "s/{{ PROCESS_MEMORY_MB }}/${PROCESS_MEMORY_MB}/g" | \
    sed "s|{{ DD_TAGS }}|${DD_TAGS_VALUE}|g")

if echo "$AGENT_MANIFEST" | grep -q "{{ .*_MEMORY_MB }}"; then
    echo "      ✗ ERROR: Template substitution failed for memory placeholders"
    exit 1
fi
if echo "$AGENT_MANIFEST" | grep -q "{{ DD_TAGS }}"; then
    echo "      ✗ ERROR: Template substitution failed for DD_TAGS"
    exit 1
fi

echo "$AGENT_MANIFEST" | kubectl apply -f -
echo "      ✓ Agent deployed (egress blocked)"

# We wait for agent pods to be ready and socket to be created before starting
# the lading load generator instance.
echo "      Waiting for agent and DogStatsD socket..."
TIMEOUT=120
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    AGENT_POD=$(kubectl get pods -l app.kubernetes.io/name=datadog-agent-deployment -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "$AGENT_POD" ]; then
        SOCKET_EXISTS=$(kubectl exec "$AGENT_POD" -c agent -- test -S /var/run/datadog/dsd.socket 2>/dev/null && echo "yes" || echo "no")
        if [ "$SOCKET_EXISTS" = "yes" ]; then
            echo "      ✓ DogStatsD socket ready"
            break
        fi
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "      ✗ Timeout waiting for DogStatsD socket"
    exit 1
fi

# Now deploy lading load generator instance.
kubectl apply -f "$SCRIPT_DIR/manifests/lading.yaml"
echo "      ✓ Manifests applied"
echo

echo "      Waiting for lading health..."
TIMEOUT=60
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
    LADING_POD=$(kubectl get pods -l app=lading -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
    if [ -n "$LADING_POD" ]; then
        HTTP_CODE=$(kubectl exec "$LADING_POD" -- wget -q -O- --timeout=2 http://localhost:9000/metrics 2>/dev/null | head -1 && echo "ok" || echo "fail")
        if [ "$HTTP_CODE" = "ok" ]; then
            echo "      ✓ Lading Prometheus endpoint healthy"
            break
        fi
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "      ✗ Timeout waiting for lading health"
    kubectl logs -l app=lading --tail=20
    exit 1
fi

# Check for any failed pods. No failures are expected. Failure signals invalid
# memory limits maybe but at this point more likely misconfiguration.
FAILED_PODS=$(kubectl get pods -o json | jq -r '.items[] | select(.status.phase == "Failed" or .status.phase == "Unknown" or .status.phase == "CrashLoopBackOff") | .metadata.name')
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
MONITOR_START_TIME=$(date +%s)
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

# Query Prometheus for per-container memory usage
echo "Container memory usage:"
AGENT_POD=$(kubectl get pods -l app.kubernetes.io/name=datadog-agent-deployment -o jsonpath='{.items[0].metadata.name}')

# Port-forward Prometheus to localhost
kubectl port-forward -n monitoring svc/prometheus-server 9090:80 >/dev/null 2>&1 &
PROM_PID=$!
sleep 3

# Run Python analysis script
python3 "$SCRIPT_DIR/analyze_memory.py" "http://localhost:9090/api/v1/query" "${AGENT_POD}" "${DURATION}" "${AGENT_MEMORY_MB}" "${TRACE_MEMORY_MB}" "${SYSPROBE_MEMORY_MB}" "${PROCESS_MEMORY_MB}"

# Kill port-forward quietly
kill $PROM_PID >/dev/null 2>&1
wait $PROM_PID 2>/dev/null
echo

echo "💡 Agent stable - cluster is still running for examination"
