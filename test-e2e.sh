#!/bin/bash
set -e

echo "============================================================"
echo "  Distributed Task Scheduler — Automated E2E Test Suite"
echo "============================================================"

# Helper for colorful output
green="\033[32m"
red="\033[31m"
reset="\033[0m"

function pass() { echo -e "  ${green}✔${reset} $1"; }
function fail() { echo -e "  ${red}✘${reset} $1"; exit 1; }
function info() { echo -e "  ➜ $1"; }

API_URL="http://localhost:3000"

# Ensure cluster services are running (in case a previous run was aborted)
info "Ensuring cluster services are running..."
docker compose start worker scheduler recovery

# Step 1: Check health endpoint
info "Step 1: Checking API health..."
HEALTH_RES=$(curl -s $API_URL/v1/health)
if echo "$HEALTH_RES" | grep -q '"status":"healthy"'; then
  pass "API is healthy"
else
  fail "API is not healthy: $HEALTH_RES"
fi

# Step 2: Submit a test task and wait for completion
info "Step 2: Submitting HIGH priority email task..."
TASK_RES=$(curl -s -X POST $API_URL/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"type": "email:send", "payload": {"to": "test@example.com"}, "priority": "HIGH"}')

TASK_ID=$(echo "$TASK_RES" | grep -o '"taskId":"[^"]*' | cut -d'"' -f4)
if [ -z "$TASK_ID" ]; then
  fail "Failed to extract taskId from response: $TASK_RES"
fi
info "Task created with ID: $TASK_ID"

# Polling for completion
info "Waiting for task to complete..."
for i in {1..20}; do
  STATUS_RES=$(curl -s $API_URL/v1/tasks/$TASK_ID)
  STATUS=$(echo "$STATUS_RES" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
  if [ "$STATUS" == "COMPLETED" ]; then
    pass "Task reached COMPLETED status"
    break
  fi
  if [ "$i" -eq 20 ]; then
    fail "Task failed to complete within 10 seconds. Last status: $STATUS"
  fi
  sleep 0.5
done

# Step 3: Check Metrics
info "Step 3: Checking Metrics..."
METRICS_RES=$(curl -s $API_URL/v1/metrics)
if echo "$METRICS_RES" | grep -q '"queues"'; then
  pass "Metrics endpoint is responsive"
else
  fail "Metrics endpoint failed: $METRICS_RES"
fi

# Step 4: Delayed task test
info "Step 4: Submitting Delayed task (+5 seconds)..."
DELAYED_TIME=$(date -u -d '+5 seconds' '+%Y-%m-%dT%H:%M:%SZ')
DELAYED_RES=$(curl -s -X POST $API_URL/v1/tasks \
  -H "Content-Type: application/json" \
  -d "{\"type\": \"email:send\", \"payload\": {\"to\": \"delayed@example.com\"}, \"scheduledAt\": \"$DELAYED_TIME\"}")

DELAYED_ID=$(echo "$DELAYED_RES" | grep -o '"taskId":"[^"]*' | cut -d'"' -f4)
info "Delayed Task ID: $DELAYED_ID"

# Wait 8 seconds to ensure it executes
info "Waiting 8 seconds for delayed task to execute..."
sleep 8
DELAY_STATUS_RES=$(curl -s $API_URL/v1/tasks/$DELAYED_ID)
DELAY_STATUS=$(echo "$DELAY_STATUS_RES" | grep -o '"status":"[^"]*' | cut -d'"' -f4)
if [ "$DELAY_STATUS" == "COMPLETED" ]; then
  pass "Delayed task executed successfully"
else
  fail "Delayed task did not complete. Last status: $DELAY_STATUS"
fi

# Step 5: Run Engine Verification script via API container
info "Step 5: Running Engine Verification script locally..."
info "Pausing background cluster services so they don't steal test messages..."
docker compose stop worker scheduler recovery

# Run the local .ts version so it includes our latest fixes!
if npx ts-node src/scripts/verify-engine.ts; then
  pass "Engine verification suite passed!"
else
  fail "Engine verification suite failed!"
fi

info "Restarting background cluster services..."
docker compose start worker scheduler recovery

# Step 6: Test Admin CLI
info "Step 6: Testing Admin CLI locally..."
if npx ts-node src/cli/admin.ts status; then
  pass "Admin CLI status works"
else
  fail "Admin CLI status failed"
fi

echo "============================================================"
echo -e "${green}All automated tests passed successfully!${reset}"
echo "============================================================"
