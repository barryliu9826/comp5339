#!/bin/bash
# Start all services (FastAPI backend and Dash frontend)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# PID and log directories
mkdir -p "$SCRIPT_DIR/logs/pids"
PID_DIR="$SCRIPT_DIR/logs/pids"
LOG_DIR="$SCRIPT_DIR/logs"

BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"
MQTT_PID_FILE="$PID_DIR/mqtt_publisher.pid"

# Mode parsing: only web stack now
MODE="web"
case "$1" in
    ""|"--web-only")
        MODE="web"
        ;;
    "-h"|"--help")
        echo "Usage: $0 [--web-only]"
        echo "  --web-only   Start MQTT publisher -> Backend -> Frontend (default)"
        exit 0
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use --help for usage."
        exit 1
        ;;
 esac

echo "Running mode: ${MODE}"

# Function: clean up processes occupying a port
cleanup_port() {
    local port=$1
    local service_name=$2
    
    # Check if port is in use
    local port_pid=$(lsof -ti :$port 2>/dev/null | head -1)
    if [ -n "$port_pid" ]; then
        echo "Warning: Port $port is already in use (PID: $port_pid)"
        echo "Attempting to terminate process $port_pid..."
        kill "$port_pid" 2>/dev/null
        
        # Wait for process to exit
        sleep 1
        
        # Force kill if still running
        if ps -p "$port_pid" > /dev/null 2>&1; then
            echo "Force killing process $port_pid..."
            kill -9 "$port_pid" 2>/dev/null
            sleep 1
        fi
        
        # Verify port is freed
        if lsof -ti :$port > /dev/null 2>&1; then
            echo "Error: Failed to free port $port. Please terminate the process manually."
            exit 1
        else
            echo "Port $port has been freed"
        fi
    fi
}

# Check if services are already running (via PID files)
if [ -f "$BACKEND_PID_FILE" ]; then
    BACKEND_PID=$(cat "$BACKEND_PID_FILE")
    if ps -p "$BACKEND_PID" > /dev/null 2>&1; then
        echo "Backend is already running (PID: $BACKEND_PID)"
        # Verify the process is using the expected port
        if ! lsof -ti :8000 | grep -q "$BACKEND_PID" 2>/dev/null; then
            echo "Warning: Backend PID file exists but process is not using port 8000"
        fi
    else
        rm -f "$BACKEND_PID_FILE"
    fi
fi

if [ -f "$FRONTEND_PID_FILE" ]; then
    FRONTEND_PID=$(cat "$FRONTEND_PID_FILE")
    if ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
        echo "Frontend is already running (PID: $FRONTEND_PID)"
    else
        rm -f "$FRONTEND_PID_FILE"
    fi
fi

if [ -f "$MQTT_PID_FILE" ]; then
    MQTT_PID=$(cat "$MQTT_PID_FILE")
    if ps -p "$MQTT_PID" > /dev/null 2>&1; then
        echo "MQTT publisher is already running (PID: $MQTT_PID)"
    else
        rm -f "$MQTT_PID_FILE"
    fi
fi

# Proactively free ports (even if PID files are missing)
cleanup_port 8000 "Backend"
cleanup_port 8050 "Frontend"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

# Check dependencies
if [ ! -f "requirements.txt" ]; then
    echo "Error: requirements.txt not found"
    exit 1
fi

# Install dependencies if needed
echo "Installing dependencies..."
pip3 install -q -r requirements.txt

# Start MQTT publisher (background)
if [ ! -f "$MQTT_PID_FILE" ] || ! ps -p "$(cat "$MQTT_PID_FILE" 2>/dev/null)" > /dev/null 2>&1; then
    echo "Starting MQTT publisher..."
    nohup python3 src/data/mqtt_publisher.py > "$LOG_DIR/mqtt_publisher.log" 2>&1 &
    MQTT_PID=$!
    echo $MQTT_PID > "$MQTT_PID_FILE"
    echo "MQTT publisher started (PID: $MQTT_PID)"
fi

# Start FastAPI backend (background)
echo "Starting FastAPI backend on port 8000..."
nohup python3 -m uvicorn src.dashboard.backend:app --host 0.0.0.0 --port 8000 > "$LOG_DIR/backend.log" 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID > "$BACKEND_PID_FILE"
echo "Backend started (PID: $BACKEND_PID)"

# Wait for backend
sleep 3

# Verify backend started
if ! ps -p "$BACKEND_PID" > /dev/null 2>&1; then
    echo "Error: Backend failed to start"
    rm -f "$BACKEND_PID_FILE"
    exit 1
fi

# Start Dash frontend (background)
echo "Starting Dash frontend on port 8050..."
nohup python3 src/dashboard/frontend.py > "$LOG_DIR/frontend.log" 2>&1 &
FRONTEND_PID=$!
echo $FRONTEND_PID > "$FRONTEND_PID_FILE"
echo "Frontend started (PID: $FRONTEND_PID)"

# Wait for frontend
sleep 2

# Verify frontend started
if ! ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
    echo "Error: Frontend failed to start"
    rm -f "$FRONTEND_PID_FILE"
    exit 1
fi

echo ""
echo "=========================================="
echo "Dashboard services started successfully!"
echo "=========================================="
echo "Mode: $MODE"
echo "Backend PID:  $BACKEND_PID"
echo "Frontend PID: $FRONTEND_PID"
[ -n "$MQTT_PID" ] && echo "MQTT publisher PID: $MQTT_PID"
echo ""
echo "Backend API:   http://localhost:8000"
echo "Backend Docs:  http://localhost:8000/docs"
echo "Frontend:      http://localhost:8050"
echo ""
echo "Logs:"
echo "  Backend:  $LOG_DIR/backend.log"
echo "  Frontend: $LOG_DIR/frontend.log"
echo "  MQTT publisher:   $LOG_DIR/mqtt_publisher.log"
echo ""
echo "To stop all services, run: ./stop_all.sh"
echo "=========================================="

