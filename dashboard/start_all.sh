#!/bin/bash
# 启动所有服务（FastAPI 后端和 Dash 前端）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# PID 文件目录
PID_DIR="$SCRIPT_DIR/.pids"
mkdir -p "$PID_DIR"

BACKEND_PID_FILE="$PID_DIR/backend.pid"
FRONTEND_PID_FILE="$PID_DIR/frontend.pid"

# 函数：检查并清理占用端口的进程
cleanup_port() {
    local port=$1
    local service_name=$2
    
    # 检查端口是否被占用
    local port_pid=$(lsof -ti :$port 2>/dev/null | head -1)
    if [ -n "$port_pid" ]; then
        echo "Warning: Port $port is already in use (PID: $port_pid)"
        echo "Attempting to terminate process $port_pid..."
        kill "$port_pid" 2>/dev/null
        
        # 等待进程退出
        sleep 1
        
        # 如果进程仍在运行，强制终止
        if ps -p "$port_pid" > /dev/null 2>&1; then
            echo "Force killing process $port_pid..."
            kill -9 "$port_pid" 2>/dev/null
            sleep 1
        fi
        
        # 再次检查端口是否已释放
        if lsof -ti :$port > /dev/null 2>&1; then
            echo "Error: Failed to free port $port. Please manually terminate the process."
            exit 1
        else
            echo "Port $port has been freed"
        fi
    fi
}

# 检查服务是否已经在运行（通过PID文件）
if [ -f "$BACKEND_PID_FILE" ]; then
    BACKEND_PID=$(cat "$BACKEND_PID_FILE")
    if ps -p "$BACKEND_PID" > /dev/null 2>&1; then
        echo "Backend is already running (PID: $BACKEND_PID)"
        # 检查端口是否被该进程占用
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

# 清理可能占用端口的进程（即使PID文件不存在）
cleanup_port 8000 "Backend"
cleanup_port 8050 "Frontend"

# 检查 Python 环境
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

# 检查依赖
if [ ! -f "../requirements.txt" ]; then
    echo "Error: requirements.txt not found"
    exit 1
fi

# 安装依赖（如果需要）
echo "Installing dependencies..."
pip3 install -q -r ../requirements.txt

# 启动 FastAPI 后端（后台）
echo "Starting FastAPI backend on port 8000..."
nohup python3 -m uvicorn backend:app --host 0.0.0.0 --port 8000 > "$PID_DIR/backend.log" 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID > "$BACKEND_PID_FILE"
echo "Backend started (PID: $BACKEND_PID)"

# 等待后端启动
sleep 3

# 检查后端是否启动成功
if ! ps -p "$BACKEND_PID" > /dev/null 2>&1; then
    echo "Error: Backend failed to start"
    rm -f "$BACKEND_PID_FILE"
    exit 1
fi

# 启动 Dash 前端（后台）
echo "Starting Dash frontend on port 8050..."
nohup python3 frontend.py > "$PID_DIR/frontend.log" 2>&1 &
FRONTEND_PID=$!
echo $FRONTEND_PID > "$FRONTEND_PID_FILE"
echo "Frontend started (PID: $FRONTEND_PID)"

# 等待前端启动
sleep 2

# 检查前端是否启动成功
if ! ps -p "$FRONTEND_PID" > /dev/null 2>&1; then
    echo "Error: Frontend failed to start"
    rm -f "$FRONTEND_PID_FILE"
    exit 1
fi

echo ""
echo "=========================================="
echo "Dashboard services started successfully!"
echo "=========================================="
echo "Backend PID:  $BACKEND_PID"
echo "Frontend PID: $FRONTEND_PID"
echo ""
echo "Backend API:   http://localhost:8000"
echo "Backend Docs:  http://localhost:8000/docs"
echo "Frontend:      http://localhost:8050"
echo ""
echo "Logs:"
echo "  Backend:  $PID_DIR/backend.log"
echo "  Frontend: $PID_DIR/frontend.log"
echo ""
echo "To stop all services, run: ./stop_all.sh"
echo "=========================================="

