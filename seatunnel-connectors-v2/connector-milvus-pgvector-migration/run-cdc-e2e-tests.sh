#!/usr/bin/env bash
# =============================================================================
# Milvus CDC → PgVector E2E Test Runner
# =============================================================================
# Runs the comprehensive CDC E2E test suite against a pre-deployed Milvus
# (docker-compose at /home/zhangqiang/project/vts/vts.yaml) and PostgreSQL
# with pgvector (data dir /home/zhangqiang/pg/data).
#
# Usage:
#   ./run-cdc-e2e-tests.sh                  # run all CDC E2E tests
#   ./run-cdc-e2e-tests.sh full             # run only full-sync tests
#   ./run-cdc-e2e-tests.sh incremental      # run only incremental tests
#   ./run-cdc-e2e-tests.sh resilience       # run only resilience tests
#   ./run-cdc-e2e-tests.sh compile          # compile tests only (no execution)
#
# Environment overrides:
#   MILVUS_URL=http://localhost:19530
#   PG_HOST=localhost
#   PG_PORT=5432
#   PG_USER=zhangqiang
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MODULE_REL="seatunnel-connectors-v2/connector-milvus-pgvector-migration"
REPORT_DIR="$SCRIPT_DIR/target/cdc-e2e-reports"

# ---- Configuration (defaults, overridable via env) ----
MILVUS_URL="${MILVUS_URL:-http://localhost:19530}"
MILVUS_HEALTH_URL="${MILVUS_HEALTH_URL:-http://localhost:9091/healthz}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-zhangqiang}"

# ---- Test selection ----
MODE="${1:-all}"
TEST_CLASSES=""
case "$MODE" in
    all)
        TEST_CLASSES="MilvusCdcFullSyncE2E,MilvusCdcIncrementalE2E,MilvusCdcResilienceE2E"
        ;;
    full)
        TEST_CLASSES="MilvusCdcFullSyncE2E"
        ;;
    incremental)
        TEST_CLASSES="MilvusCdcIncrementalE2E"
        ;;
    resilience)
        TEST_CLASSES="MilvusCdcResilienceE2E"
        ;;
    compile)
        TEST_CLASSES=""
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: $0 [all|full|incremental|resilience|compile]"
        exit 1
        ;;
esac

# ---- Helpers ----
log()  { echo "[cdc-e2e] $*"; }
warn() { echo "[cdc-e2e] WARNING: $*" >&2; }
fail() { echo "[cdc-e2e] ERROR: $*" >&2; exit 1; }

check_prerequisites() {
    log "Checking prerequisites..."

    # 1. Milvus health
    if curl -sf "$MILVUS_HEALTH_URL" >/dev/null 2>&1; then
        log "  Milvus healthy at $MILVUS_URL"
    else
        warn "  Milvus health check failed at $MILVUS_HEALTH_URL"
        warn "  Start Milvus: cd /home/zhangqiang/project/vts && docker compose -f vts.yaml up -d"
        fail "Milvus not reachable"
    fi

    # 2. PostgreSQL
    if pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" >/dev/null 2>&1; then
        log "  PostgreSQL reachable at $PG_HOST:$PG_PORT"
    elif PGPASSWORD="" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d postgres -c "SELECT 1" >/dev/null 2>&1; then
        log "  PostgreSQL reachable at $PG_HOST:$PG_PORT"
    else
        warn "  PostgreSQL not reachable at $PG_HOST:$PG_PORT"
        warn "  Start PostgreSQL: pg_ctl -D /home/zhangqiang/pg/data start"
        fail "PostgreSQL not reachable"
    fi

    # 3. pgvector extension available
    if PGPASSWORD="" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d postgres -c "SELECT extversion FROM pg_extension WHERE extname='vector'" 2>/dev/null | grep -q '[0-9]'; then
        log "  pgvector extension available in postgres database"
    else
        warn "  pgvector extension not found in 'postgres' database (tests will CREATE EXTENSION in test DB)"
    fi

    log "Prerequisites OK."
}

run_compile() {
    log "Compiling test sources (no execution)..."
    cd "$PROJECT_ROOT"
    ./mvnw test-compile -pl "$MODULE_REL" \
        -Dmaven.test.skip=false -DskipUT=false \
        -D"checkstyle.skip"=true -D"spotless.check.skip"=true \
        -q
    log "Compilation OK."
}

run_tests() {
    [ -z "$TEST_CLASSES" ] && { log "No tests selected (compile-only mode)."; return 0; }
    log "Running CDC E2E tests: $TEST_CLASSES"
    log "  Milvus: $MILVUS_URL"
    log "  PostgreSQL: $PG_HOST:$PG_PORT (user=$PG_USER)"
    log "  Report dir: $REPORT_DIR"
    cd "$PROJECT_ROOT"
    ./mvnw test -pl "$MODULE_REL" \
        -Dtest="$TEST_CLASSES" \
        -Dmigration.cdc.e2e.enabled=true \
        -Dmaven.test.skip=false -DskipUT=false \
        -DfailIfNoTests=false \
        -D"checkstyle.skip"=true -D"spotless.check.skip"=true
}

show_report() {
    log "Test run complete."
    if [ -d "$REPORT_DIR" ]; then
        local latest
        latest=$(ls -t "$REPORT_DIR"/cdc-e2e-report-*.json 2>/dev/null | head -1)
        if [ -n "$latest" ]; then
            log "Latest report: $latest"
            if command -v jq >/dev/null 2>&1; then
                log "Scenario summary:"
                jq -r '.scenarios[] | "\(.name)\t\(.passed)"' "$latest" 2>/dev/null || true
                log "Problem count: $(jq '.problemAnalysis | length' "$latest" 2>/dev/null || echo '?')"
            else
                log "Install 'jq' for pretty report summary."
            fi
        fi
    fi
}

# ---- Main ----
if [ "$MODE" = "compile" ]; then
    run_compile
    exit 0
fi

check_prerequisites
run_compile
run_tests
show_report
