@echo off
REM 启动所有服务（FastAPI 后端和 Dash 前端）

setlocal enabledelayedexpansion

REM 获取脚本目录
set "SCRIPT_DIR=%~dp0"
cd /d "%SCRIPT_DIR%" || exit /b 1

REM PID 文件目录
if not exist "%SCRIPT_DIR%/logs/pids" mkdir "%SCRIPT_DIR%/logs/pids"

set "BACKEND_PID_FILE=%SCRIPT_DIR%/logs/pids/backend.pid"
set "FRONTEND_PID_FILE=%SCRIPT_DIR%/logs/pids/frontend.pid"

REM 函数：清理占用端口的进程
call :cleanup_port 8000 "Backend"
call :cleanup_port 8050 "Frontend"

REM 检查服务是否已经在运行（通过PID文件）
if exist "%BACKEND_PID_FILE%" (
    set /p BACKEND_PID=<"%BACKEND_PID_FILE%"
    tasklist /FI "PID eq !BACKEND_PID!" 2>nul | find /I "!BACKEND_PID!" >nul
    if errorlevel 1 (
        echo Backend PID file exists but process is not running, cleaning up...
        del /f /q "%BACKEND_PID_FILE%" 2>nul
    ) else (
        echo Backend is already running (PID: !BACKEND_PID!)
        goto :check_frontend
    )
)

if exist "%FRONTEND_PID_FILE%" (
    set /p FRONTEND_PID=<"%FRONTEND_PID_FILE%"
    tasklist /FI "PID eq !FRONTEND_PID!" 2>nul | find /I "!FRONTEND_PID!" >nul
    if errorlevel 1 (
        echo Frontend PID file exists but process is not running, cleaning up...
        del /f /q "%FRONTEND_PID_FILE%" 2>nul
    ) else (
        echo Frontend is already running (PID: !FRONTEND_PID!)
        goto :start_backend
    )
)

:check_frontend
REM 检查前端是否也在运行
if exist "%FRONTEND_PID_FILE%" (
    set /p FRONTEND_PID=<"%FRONTEND_PID_FILE%"
    tasklist /FI "PID eq !FRONTEND_PID!" 2>nul | find /I "!FRONTEND_PID!" >nul
    if not errorlevel 1 (
        echo All services are already running!
        goto :show_info
    )
)

:start_backend
REM 检查 Python 环境
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python not found. Please install Python 3.8+
    exit /b 1
)

REM 检查依赖文件
if not exist "..\requirements.txt" (
    echo Error: requirements.txt not found
    exit /b 1
)

REM 安装依赖（如果需要）
echo Installing dependencies...
python -m pip install -q -r ..\requirements.txt
if errorlevel 1 (
    echo Error: Failed to install dependencies
    exit /b 1
)

REM 启动 FastAPI 后端（后台）
echo Starting FastAPI backend on port 8000...

REM 使用 start 命令在后台启动进程
start /b "" python -m uvicorn backend:app --host 0.0.0.0 --port 8000 > "%SCRIPT_DIR%/logs/backend.log" 2>&1

REM 等待进程启动，然后通过端口查找 PID
timeout /t 2 /nobreak >nul
set BACKEND_PID=
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
    set BACKEND_PID=%%a
    goto :save_backend_pid
)

REM 如果通过端口找不到，尝试通过进程命令行查找
for /f "tokens=2" %%a in ('wmic process where "commandline like '%%uvicorn backend:app%%'" get processid /format:value 2^>nul ^| findstr "ProcessId"') do (
    set BACKEND_PID=%%a
    goto :save_backend_pid
)

echo Error: Failed to start backend or find process
exit /b 1

:save_backend_pid
echo !BACKEND_PID! > "%BACKEND_PID_FILE%"
echo Backend started (PID: !BACKEND_PID!)

:wait_backend
REM 等待后端启动并检查
timeout /t 3 /nobreak >nul
tasklist /FI "PID eq !BACKEND_PID!" 2>nul | find /I "!BACKEND_PID!" >nul
if errorlevel 1 (
    echo Error: Backend failed to start
    del /f /q "%BACKEND_PID_FILE%" 2>nul
    exit /b 1
)

REM 启动 Dash 前端（后台）
echo Starting Dash frontend on port 8050...

REM 使用 start 命令在后台启动进程
start /b "" python frontend.py > "%SCRIPT_DIR%/logs/frontend.log" 2>&1

REM 等待进程启动，然后通过端口查找 PID
timeout /t 2 /nobreak >nul
set FRONTEND_PID=
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8050" ^| findstr "LISTENING"') do (
    set FRONTEND_PID=%%a
    goto :save_frontend_pid
)

REM 如果通过端口找不到，尝试通过进程命令行查找
for /f "tokens=2" %%a in ('wmic process where "commandline like '%%frontend.py%%'" get processid /format:value 2^>nul ^| findstr "ProcessId"') do (
    set FRONTEND_PID=%%a
    goto :save_frontend_pid
)

echo Error: Failed to start frontend or find process
del /f /q "%BACKEND_PID_FILE%" 2>nul
exit /b 1

:save_frontend_pid
echo !FRONTEND_PID! > "%FRONTEND_PID_FILE%"
echo Frontend started (PID: !FRONTEND_PID!)

:wait_frontend
REM 等待前端启动并检查
timeout /t 2 /nobreak >nul
tasklist /FI "PID eq !FRONTEND_PID!" 2>nul | find /I "!FRONTEND_PID!" >nul
if errorlevel 1 (
    echo Error: Frontend failed to start
    del /f /q "%FRONTEND_PID_FILE%" 2>nul
    exit /b 1
)

:show_info
echo.
echo ==========================================
echo Dashboard services started successfully!
echo ==========================================
if defined BACKEND_PID echo Backend PID:  !BACKEND_PID!
if defined FRONTEND_PID echo Frontend PID: !FRONTEND_PID!
echo.
echo Backend API:   http://localhost:8000
echo Backend Docs:  http://localhost:8000/docs
echo Frontend:      http://localhost:8050
echo.
echo Logs:
echo   Backend:  %SCRIPT_DIR%/logs/backend.log
echo   Frontend: %SCRIPT_DIR%/logs/frontend.log
echo.
echo To stop all services, run: stop_all.bat
echo ==========================================

endlocal
exit /b 0

REM 清理端口占用的子程序
:cleanup_port
setlocal enabledelayedexpansion
set "port=%~1"
set "service_name=%~2"

REM 查找占用端口的进程
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%port%" ^| findstr "LISTENING"') do (
    set "port_pid=%%a"
    if defined port_pid (
        echo Warning: Port %port% is already in use (PID: !port_pid!)
        echo Attempting to terminate process !port_pid!...
        taskkill /PID !port_pid! /F >nul 2>&1
        
        REM 等待进程退出
        timeout /t 1 /nobreak >nul
        
        REM 再次检查
        tasklist /FI "PID eq !port_pid!" 2>nul | find /I "!port_pid!" >nul
        if not errorlevel 1 (
            echo Error: Failed to free port %port%. Please manually terminate the process.
            exit /b 1
        ) else (
            echo Port %port% has been freed
        )
    )
)
endlocal
exit /b 0
