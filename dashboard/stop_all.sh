#!/bin/bash
# 停止所有服务（FastAPI 后端和 Dash 前端）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$SCRIPT_DIR/.pids"

BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"

# 停止后端
if [ -f "$BACKEND_PID_FILE" ]; then
    BACKEND_PID=$(cat "$BACKEND_PID_FILE")
    if ps -p "$BACKEND_PID" > /dev/null 2>&1; then
        echo "Stopping backend (PID: $BACKEND_PID)..."
        kill "$BACKEND_PID" 2>/dev/null
        
        # 等待进程退出
        for i in {1..10}; do
            if ! ps -p "$BACKEND_PID" > /dev/null 2>&1; then
                break
            fi
            sleep 0.5
        done
        
        # 如果进程仍在运行，强制杀死
        if ps -p "$BACKEND_PID" > /dev/null 2>&1; then
            echo "Force killing backend..."
            kill -9 "$BACKEND_PID" 2>/dev/null
        fi
        
        rm -f "$BACKEND_PID_FILE"
        echo "Backend stopped"
    else
        echo "Backend is not running"
        rm -f "$BACKEND_PID_FILE"
    fi
else
    echo "Backend PID file not found"
fi

# 停止前端
if [ -f "$FRONTEND_PID_FILE" ]; then
    FRONTEND_PID=$(cat "$FRONTEND_PID_FILE")
    if ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
        echo "Stopping frontend (PID: $FRONTEND_PID)..."
        kill "$FRONTEND_PID" 2>/dev/null
        
        # 等待进程退出
        for i in {1..10}; do
            if ! ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
                break
            fi
            sleep 0.5
        done
        
        # 如果进程仍在运行，强制杀死
        if ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
            echo "Force killing frontend..."
            kill -9 "$FRONTEND_PID" 2>/dev/null
        fi
        
        rm -f "$FRONTEND_PID_FILE"
        echo "Frontend stopped"
    else
        echo "Frontend is not running"
        rm -f "$FRONTEND_PID_FILE"
    fi
else
    echo "Frontend PID file not found"
fi

# 清理 PID 目录（如果为空）
if [ -d "$PID_DIR" ] && [ -z "$(ls -A "$PID_DIR" 2>/dev/null)" ]; then
    rmdir "$PID_DIR" 2>/dev/null
fi

# 清理可能遗留的端口占用进程（即使PID文件不存在）
echo ""
echo "Checking for orphaned processes on ports 8000 and 8050..."

for port in 8000 8050; do
    port_pid=$(lsof -ti :$port 2>/dev/null | head -1)
    if [ -n "$port_pid" ]; then
        echo "Found process $port_pid using port $port, terminating..."
        kill "$port_pid" 2>/dev/null
        sleep 1
        if ps -p "$port_pid" > /dev/null 2>&1; then
            kill -9 "$port_pid" 2>/dev/null
        fi
    fi
done

echo ""
echo "All services stopped"

