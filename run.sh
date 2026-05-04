#!/usr/bin/env bash
# =============================================================================
# run.sh — One-shot launcher for the HCMC Real-Time Bus Tracking demo
#
# Usage:
#   ./run.sh            — full setup: check prereqs, build JAR, start everything
#   ./run.sh --stop     — stop all containers (keep data volumes)
#   ./run.sh --clean    — stop all containers AND delete all data volumes
#   ./run.sh --rebuild  — rebuild JAR + Docker images, then (re)start
#   ./run.sh --logs     — tail logs from the key services
#   ./run.sh --status   — show which services are up
# =============================================================================

set -euo pipefail

# ── Resolve project root (directory containing this script) ──────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

info()    { echo -e "${CYAN}[•]${RESET} $*"; }
success() { echo -e "${GREEN}[✓]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[!]${RESET} $*"; }
error()   { echo -e "${RED}[✗]${RESET} $*" >&2; }
header()  { echo -e "\n${BOLD}$*${RESET}"; }

# ── Pick docker compose command (v2 plugin preferred) ────────────────────────
if docker compose version &>/dev/null; then
    DC="docker compose"
elif command -v docker-compose &>/dev/null; then
    DC="docker-compose"
else
    error "Neither 'docker compose' nor 'docker-compose' found."
    error "Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
    exit 1
fi

# ── Handle flags ──────────────────────────────────────────────────────────────
MODE="start"
case "${1:-}" in
    --stop)    MODE="stop"    ;;
    --clean)   MODE="clean"   ;;
    --rebuild) MODE="rebuild" ;;
    --logs)    MODE="logs"    ;;
    --status)  MODE="status"  ;;
    --help|-h)
        sed -n '/^# Usage:/,/^# ===/p' "$0" | grep -v '^# ===' | sed 's/^# //'
        exit 0
        ;;
    "")        MODE="start"   ;;
    *)
        error "Unknown option: ${1}. Run ./run.sh --help"
        exit 1
        ;;
esac

# =============================================================================
# Non-start modes — quick exits
# =============================================================================

if [[ "$MODE" == "stop" ]]; then
    header "Stopping all services..."
    $DC down
    success "All containers stopped. Data volumes preserved."
    exit 0
fi

if [[ "$MODE" == "clean" ]]; then
    header "Stopping all services and deleting data volumes..."
    $DC down -v
    success "All containers stopped and volumes deleted. Fresh state next run."
    exit 0
fi

if [[ "$MODE" == "logs" ]]; then
    exec $DC logs -f producer flink-taskmanager flink-job-submitter api
fi

if [[ "$MODE" == "status" ]]; then
    $DC ps
    exit 0
fi

# =============================================================================
# START / REBUILD — main flow
# =============================================================================

echo -e "\n${BOLD}================================================${RESET}"
echo -e "${BOLD}  HCMC Real-Time Bus Tracking — Demo Launcher  ${RESET}"
echo -e "${BOLD}================================================${RESET}\n"

# ── Step 1: Prerequisites check ───────────────────────────────────────────────
header "Step 1/4 — Checking prerequisites"

PREREQ_OK=true

# Java
if command -v java &>/dev/null; then
    JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/{print $2}' | cut -d'.' -f1)
    if [[ "$JAVA_VER" -ge 17 ]] 2>/dev/null; then
        success "Java $JAVA_VER found"
    else
        error "Java 17+ required, found version $JAVA_VER"
        PREREQ_OK=false
    fi
else
    error "Java not found. Install: https://adoptium.net/"
    PREREQ_OK=false
fi

# Maven
if command -v mvn &>/dev/null; then
    MVN_VER=$(mvn -version 2>&1 | head -1 | awk '{print $3}')
    success "Maven $MVN_VER found"
else
    error "Maven not found. Install: brew install maven  OR  https://maven.apache.org/"
    PREREQ_OK=false
fi

# Docker
if command -v docker &>/dev/null; then
    if docker info &>/dev/null; then
        success "Docker is running"
    else
        error "Docker is installed but not running. Please start Docker Desktop."
        PREREQ_OK=false
    fi
else
    error "Docker not found. Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
    PREREQ_OK=false
fi

# Dataset path
DATASET_PATH=$(grep '^DATASET_PATH=' .env | cut -d'=' -f2)
if [[ -d "$DATASET_PATH" ]]; then
    NFILES=$(find "$DATASET_PATH" -name "sub_raw_*.json" 2>/dev/null | wc -l | tr -d ' ')
    if [[ "$NFILES" -gt 0 ]]; then
        success "Dataset found at $DATASET_PATH ($NFILES JSON files)"
    else
        error "Dataset directory exists but contains no sub_raw_*.json files: $DATASET_PATH"
        PREREQ_OK=false
    fi
else
    error "Dataset not found at: $DATASET_PATH"
    error "Update DATASET_PATH in .env to the correct location."
    PREREQ_OK=false
fi

if [[ "$PREREQ_OK" != "true" ]]; then
    echo ""
    error "Fix the issues above, then re-run ./run.sh"
    exit 1
fi

# ── Step 2: Build Flink JAR ───────────────────────────────────────────────────
header "Step 2/4 — Building Flink JAR"

JAR="flink-job/target/bus-tracking-job-1.0.0.jar"

# Rebuild if: --rebuild flag, JAR missing, or any Java source is newer than JAR
NEED_BUILD=false
if [[ "$MODE" == "rebuild" ]]; then
    NEED_BUILD=true
elif [[ ! -f "$JAR" ]]; then
    NEED_BUILD=true
elif find flink-job/src -name "*.java" -newer "$JAR" 2>/dev/null | grep -q .; then
    NEED_BUILD=true
fi

if [[ "$NEED_BUILD" == "true" ]]; then
    info "Building fat JAR (this takes ~60 seconds on first run)..."
    if mvn clean package -q -f flink-job/pom.xml; then
        JAR_SIZE=$(du -sh "$JAR" | cut -f1)
        success "JAR built: $JAR ($JAR_SIZE)"
    else
        error "Maven build failed. Run: mvn package -f flink-job/pom.xml"
        exit 1
    fi
else
    JAR_SIZE=$(du -sh "$JAR" | cut -f1)
    success "JAR already up-to-date: $JAR ($JAR_SIZE)"
fi

# ── Step 3: Start Docker Compose ──────────────────────────────────────────────
header "Step 3/4 — Starting Docker services"

BUILD_FLAG=""
if [[ "$MODE" == "rebuild" ]]; then
    BUILD_FLAG="--build"
    info "Rebuilding Docker images (producer, api, dashboard)..."
fi

# Check if already running — if so, skip to step 4
RUNNING=$($DC ps --services --filter "status=running" 2>/dev/null | wc -l | tr -d ' ')
if [[ "$RUNNING" -gt 5 && "$MODE" != "rebuild" ]]; then
    warn "Services are already running ($RUNNING containers). Skipping start."
    warn "To restart from scratch: ./run.sh --clean && ./run.sh"
else
    $DC up -d $BUILD_FLAG
    success "Docker Compose started"
fi

# ── Step 4: Wait and verify ───────────────────────────────────────────────────
header "Step 4/4 — Waiting for pipeline to become ready"

# Helper: wait for a URL to respond
wait_for_url() {
    local label="$1" url="$2" timeout="${3:-120}"
    local elapsed=0
    printf "    Waiting for %-28s" "$label..."
    until curl -sf "$url" > /dev/null 2>&1; do
        sleep 3; elapsed=$((elapsed + 3))
        if [[ $elapsed -ge $timeout ]]; then
            echo -e " ${RED}TIMEOUT${RESET}"
            return 1
        fi
        printf "."
    done
    echo -e " ${GREEN}ready${RESET}"
}

wait_for_url "Kafka UI"       "http://localhost:8888"  90
wait_for_url "Flink UI"       "http://localhost:8081/overview" 120
wait_for_url "MinIO"          "http://localhost:9001"  60
wait_for_url "API"            "http://localhost:8000/stats" 90
wait_for_url "Dashboard"      "http://localhost:3002"  90
wait_for_url "Grafana"        "http://localhost:3001"  60

# Wait for Flink job to reach RUNNING state
printf "    Waiting for %-28s" "Flink job RUNNING..."
elapsed=0
until curl -sf "http://localhost:8081/jobs/overview" 2>/dev/null | grep -q '"status":"RUNNING"'; do
    sleep 3; elapsed=$((elapsed + 3))
    if [[ $elapsed -ge 180 ]]; then
        echo -e " ${YELLOW}not yet (check http://localhost:8081)${RESET}"
        break
    fi
    printf "."
done
if curl -sf "http://localhost:8081/jobs/overview" 2>/dev/null | grep -q '"status":"RUNNING"'; then
    echo -e " ${GREEN}RUNNING${RESET}"
fi

# ── Done — print access table ─────────────────────────────────────────────────
echo ""
echo -e "${BOLD}================================================${RESET}"
echo -e "${BOLD}  Pipeline is live. Open these URLs:           ${RESET}"
echo -e "${BOLD}================================================${RESET}"
echo ""
echo -e "  ${CYAN}Dashboard (live bus map)${RESET}   http://localhost:3002"
echo -e "  ${CYAN}Kafka UI${RESET}                   http://localhost:8888"
echo -e "  ${CYAN}Flink UI${RESET}                   http://localhost:8081"
echo -e "  ${CYAN}MinIO console${RESET}              http://localhost:9001  (minioadmin / minioadmin)"
echo -e "  ${CYAN}REST API${RESET}                   http://localhost:8000/buses"
echo -e "  ${CYAN}Grafana${RESET}                    http://localhost:3001  (admin / admin)"
echo -e "  ${CYAN}Prometheus${RESET}                 http://localhost:9090"
echo ""
echo -e "  ${BOLD}Replay speed:${RESET} REPLAY_SPEED=$(grep '^REPLAY_SPEED=' .env | cut -d'=' -f2)  (edit .env → restart producer: $DC restart producer)"
echo ""
echo -e "  ${BOLD}Useful commands:${RESET}"
echo -e "    $DC logs -f producer flink-taskmanager     # live logs"
echo -e "    docker exec -it \$($DC ps -q redis) redis-cli HGETALL bus:\$(docker exec -it \$($DC ps -q redis) redis-cli --no-auth-warning SCAN 0 MATCH 'bus:*' COUNT 1 2>/dev/null | tail -1 | tr -d '\r')"
echo -e "    ./run.sh --stop                            # stop everything"
echo -e "    ./run.sh --clean                           # stop + wipe volumes"
echo ""
echo -e "  ${BOLD}Offline notebooks${RESET} (run after ~10 min of data):"
echo -e "    pip install jupyter pandas numpy scikit-learn folium matplotlib seaborn boto3 redis"
echo -e "    jupyter notebook notebooks/"
echo ""
