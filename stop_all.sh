#!/bin/bash
# Stop web services (MQTT publisher, FastAPI backend, Dash frontend)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

PID_DIR="$SCRIPT_DIR/logs/pids"
LOG_DIR="$SCRIPT_DIR/logs"

BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
MQTT_PID_FILE="$PID_DIR/mqtt_publisher.pid"

stop_service() {
    local name="$1"
    local pid_file="$2"
    local port="$3"

    if [ -f "$pid_file" ]; then
        PID=$(cat "$pid_file" 2>/dev/null)
        if [ -n "$PID" ] && ps -p "$PID" > /dev/null 2>&1; then
            echo "Stopping $name (PID: $PID)..."
            kill "$PID" 2>/dev/null
            sleep 1
            if ps -p "$PID" > /dev/null 2>&1; then
                echo "Force killing $name (PID: $PID)..."
                kill -9 "$PID" 2>/dev/null
            fi
        else
            echo "$name PID file exists but process not running"
        fi
        rm -f "$pid_file"
fi

    # Fallback: stop by port if provided
    if [ -n "$port" ]; then
        PORT_PID=$(lsof -ti :$port 2>/dev/null | head -1)
        if [ -n "$PORT_PID" ]; then
            echo "Stopping $name by port $port (PID: $PORT_PID)..."
            kill "$PORT_PID" 2>/dev/null || true
        sleep 1
            ps -p "$PORT_PID" > /dev/null 2>&1 && kill -9 "$PORT_PID" 2>/dev/null || true
        fi
    fi
}

# Stop services (web stack only)
stop_service "Frontend" "$FRONTEND_PID_FILE" 8050
stop_service "Backend" "$BACKEND_PID_FILE" 8000
stop_service "MQTT publisher" "$MQTT_PID_FILE" ""

echo ""
echo "=========================================="
echo "Dashboard services stopped."
echo "Remaining processes using ports (if any):"
lsof -i :8000 -sTCP:LISTEN 2>/dev/null || true
lsof -i :8050 -sTCP:LISTEN 2>/dev/null || true
echo "=========================================="

