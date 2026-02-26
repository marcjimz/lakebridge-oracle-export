#!/bin/bash
# Oracle Lakebridge Extractor - Docker Setup Script
#
# Usage: ./setup.sh <command>
#
# Commands:
#   start       - Start Oracle using slim profile (gvenzl, recommended)
#   start-xe    - Start Oracle XE 21c (requires Oracle SSO login)
#   start-23ai  - Start Oracle 23ai Free (requires Oracle SSO login)
#   stop        - Stop all Oracle containers
#   status      - Show container status
#   logs        - Follow container logs
#   connect     - Connect with sqlplus
#   init        - Initialize healthcare test schema
#   test        - Run extraction script against test DB
#   clean       - Remove containers and volumes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Default configuration
ORACLE_PWD="${ORACLE_PWD:-LakebridgeTest123!}"
HEALTHCARE_USER="${HEALTHCARE_USER:-healthcare}"
HEALTHCARE_PWD="${HEALTHCARE_PWD:-healthcare123}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Wait for Oracle to be ready
wait_for_oracle() {
    local profile="${1:-slim}"
    local max_attempts=30
    local attempt=1

    # Determine service name based on profile
    case "$profile" in
        slim)
            SERVICE_NAME="XEPDB1"
            ;;
        xe)
            SERVICE_NAME="XEPDB1"
            ;;
        23ai)
            SERVICE_NAME="FREEPDB1"
            ;;
        *)
            SERVICE_NAME="XEPDB1"
            ;;
    esac

    log_info "Waiting for Oracle to be ready (service: $SERVICE_NAME)..."

    while [ $attempt -le $max_attempts ]; do
        if docker exec oracle-lakebridge sqlplus -S -L "sys/${ORACLE_PWD}@//localhost:1521/${SERVICE_NAME} as sysdba" <<< "SELECT 1 FROM DUAL;" > /dev/null 2>&1; then
            log_info "Oracle is ready!"
            return 0
        fi

        echo -n "."
        sleep 10
        attempt=$((attempt + 1))
    done

    log_error "Oracle did not become ready in time"
    return 1
}

# Get the current profile/service name
get_service_name() {
    if docker ps --format '{{.Names}}' | grep -q "oracle-lakebridge"; then
        # Try to detect which profile is running
        local image=$(docker inspect --format '{{.Config.Image}}' oracle-lakebridge 2>/dev/null)
        case "$image" in
            *free*)
                echo "FREEPDB1"
                ;;
            *)
                echo "XEPDB1"
                ;;
        esac
    else
        echo "XEPDB1"
    fi
}

# Start Oracle container
start_oracle() {
    local profile="${1:-slim}"

    log_info "Starting Oracle container with profile: $profile"

    cd "$SCRIPT_DIR"

    # Check if container already exists
    if docker ps -a --format '{{.Names}}' | grep -q "oracle-lakebridge"; then
        log_warn "Container 'oracle-lakebridge' already exists"
        log_info "Run './setup.sh stop' first or './setup.sh clean' to reset"
        exit 1
    fi

    # Start with selected profile
    docker compose --profile "$profile" up -d

    # Wait for Oracle to be ready
    wait_for_oracle "$profile"

    log_info "Oracle container started successfully"

    # Show connection info
    local service_name=$(get_service_name)
    echo ""
    echo "Connection Information:"
    echo "  Host:     localhost"
    echo "  Port:     1521"
    echo "  Service:  $service_name"
    echo "  SYS Password: $ORACLE_PWD"
    echo ""
    echo "Connect with: sqlplus sys/${ORACLE_PWD}@//localhost:1521/${service_name} as sysdba"
}

# Stop Oracle containers
stop_oracle() {
    log_info "Stopping Oracle containers..."
    cd "$SCRIPT_DIR"
    docker compose --profile slim --profile xe --profile 23ai down
    log_info "Oracle containers stopped"
}

# Show status
show_status() {
    echo "Container Status:"
    docker ps -a --filter "name=oracle-lakebridge" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
}

# Show logs
show_logs() {
    docker logs -f oracle-lakebridge
}

# Connect with sqlplus
connect_sqlplus() {
    local user="${1:-sys}"
    local service_name=$(get_service_name)

    if [ "$user" = "sys" ]; then
        docker exec -it oracle-lakebridge sqlplus "sys/${ORACLE_PWD}@//localhost:1521/${service_name} as sysdba"
    elif [ "$user" = "healthcare" ]; then
        docker exec -it oracle-lakebridge sqlplus "${HEALTHCARE_USER}/${HEALTHCARE_PWD}@//localhost:1521/${service_name}"
    else
        docker exec -it oracle-lakebridge sqlplus "$user@//localhost:1521/${service_name}"
    fi
}

# Initialize healthcare schema
init_schema() {
    local service_name=$(get_service_name)

    log_info "Initializing healthcare schema..."

    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "oracle-lakebridge"; then
        log_error "Oracle container is not running. Start it first with './setup.sh start'"
        exit 1
    fi

    # Wait for Oracle to be ready
    wait_for_oracle

    # Run the init script
    log_info "Running init_healthcare_schema.sql..."
    docker exec -i oracle-lakebridge sqlplus "sys/${ORACLE_PWD}@//localhost:1521/${service_name} as sysdba" < "$SCRIPT_DIR/init-scripts/init_healthcare_schema.sql"

    log_info "Healthcare schema initialized successfully"
    echo ""
    echo "Schema Information:"
    echo "  User:     ${HEALTHCARE_USER}"
    echo "  Password: ${HEALTHCARE_PWD}"
    echo "  Service:  ${service_name}"
    echo ""
    echo "Connect with: sqlplus ${HEALTHCARE_USER}/${HEALTHCARE_PWD}@//localhost:1521/${service_name}"
}

# Run extraction test
run_test() {
    local service_name=$(get_service_name)
    local output_dir="$PROJECT_DIR/lakebridge_output"

    log_info "Running extraction test..."

    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "oracle-lakebridge"; then
        log_error "Oracle container is not running. Start it first with './setup.sh start'"
        exit 1
    fi

    # Create output directory
    mkdir -p "$output_dir"

    # Run the extraction
    log_info "Extracting DDL from healthcare schema..."
    cd "$PROJECT_DIR"
    python -m src.oracle_lakebridge_extractor \
        --host localhost \
        --port 1521 \
        --service "$service_name" \
        --user "$HEALTHCARE_USER" \
        --password "$HEALTHCARE_PWD" \
        --schemas healthcare \
        --output "$output_dir" \
        --verbose

    log_info "Extraction complete. Output in: $output_dir"

    # Show summary
    echo ""
    echo "Extracted files:"
    find "$output_dir" -name "*.sql" | head -20

    file_count=$(find "$output_dir" -name "*.sql" | wc -l)
    echo ""
    echo "Total SQL files: $file_count"
}

# Clean up containers and volumes
clean_all() {
    log_warn "This will remove all Oracle containers and data volumes!"
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Cleaning up..."
        cd "$SCRIPT_DIR"
        docker compose --profile slim --profile xe --profile 23ai down -v
        docker volume rm oracle-lakebridge-data-slim oracle-lakebridge-data-xe oracle-lakebridge-data-23ai 2>/dev/null || true
        log_info "Cleanup complete"
    else
        log_info "Cleanup cancelled"
    fi
}

# Show help
show_help() {
    echo "Oracle Lakebridge Extractor - Docker Setup Script"
    echo ""
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  start       Start Oracle using slim profile (gvenzl, recommended)"
    echo "  start-xe    Start Oracle XE 21c (requires Oracle SSO login)"
    echo "  start-23ai  Start Oracle 23ai Free (requires Oracle SSO login)"
    echo "  stop        Stop all Oracle containers"
    echo "  status      Show container status"
    echo "  logs        Follow container logs"
    echo "  connect     Connect with sqlplus as SYS"
    echo "  connect-hc  Connect with sqlplus as healthcare user"
    echo "  init        Initialize healthcare test schema"
    echo "  test        Run extraction script against test DB"
    echo "  clean       Remove containers and volumes"
    echo ""
    echo "Environment Variables:"
    echo "  ORACLE_PWD       Oracle SYS password (default: LakebridgeTest123!)"
    echo "  HEALTHCARE_USER  Healthcare schema user (default: healthcare)"
    echo "  HEALTHCARE_PWD   Healthcare schema password (default: healthcare123)"
}

# Main
case "${1:-help}" in
    start)
        start_oracle "slim"
        ;;
    start-xe)
        start_oracle "xe"
        ;;
    start-23ai)
        start_oracle "23ai"
        ;;
    stop)
        stop_oracle
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    connect)
        connect_sqlplus "sys"
        ;;
    connect-hc)
        connect_sqlplus "healthcare"
        ;;
    init)
        init_schema
        ;;
    test)
        run_test
        ;;
    clean)
        clean_all
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac
