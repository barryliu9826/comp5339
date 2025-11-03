@echo off
REM 停止所有服务（FastAPI 后端和 Dash 前端）

setlocal enabledelayedexpansion

REM 获取脚本目录
set "SCRIPT_DIR=%~dp0"
set "PID_DIR=%SCRIPT_DIR%/logs/pids"

set "BACKEND_PID_FILE=%SCRIPT_DIR%/logs/pids/backend.pid"
set "FRONTEND_PID_FILE=%SCRIPT_DIR%/logs/pids/frontend.pid"

REM 停止后端
if exist "%SCRIPT_DIR%/logs/pids/backend.pid" (
    set /p BACKEND_PID=<"%SCRIPT_DIR%/logs/pids/backend.pid"
    tasklist /FI "PID eq !BACKEND_PID!" 2>nul | find /I "!BACKEND_PID!" >nul
    if not errorlevel 1 (
        echo Stopping backend (PID: !BACKEND_PID!)...
        taskkill /PID !BACKEND_PID! /F >nul 2>&1
        
        REM 等待进程退出
        for /l %%i in (1,1,10) do (
            tasklist /FI "PID eq !BACKEND_PID!" 2>nul | find /I "!BACKEND_PID!" >nul
            if errorlevel 1 goto :backend_stopped
            timeout /t 1 /nobreak >nul
        )
        
        :backend_stopped
        REM 如果进程仍在运行，再次强制杀死
        tasklist /FI "PID eq !BACKEND_PID!" 2>nul | find /I "!BACKEND_PID!" >nul
        if not errorlevel 1 (
            echo Force killing backend...
            taskkill /PID !BACKEND_PID! /F >nul 2>&1
        )
        
        del /f /q "%BACKEND_PID_FILE%" 2>nul
        echo Backend stopped
    ) else (
        echo Backend is not running
        del /f /q "%BACKEND_PID_FILE%" 2>nul
    )
) else (
    echo Backend PID file not found
)

REM 停止前端
if exist "%FRONTEND_PID_FILE%" (
    set /p FRONTEND_PID=<"%FRONTEND_PID_FILE%"
    tasklist /FI "PID eq !FRONTEND_PID!" 2>nul | find /I "!FRONTEND_PID!" >nul
    if not errorlevel 1 (
        echo Stopping frontend (PID: !FRONTEND_PID!)...
        taskkill /PID !FRONTEND_PID! /F >nul 2>&1
        
        REM 等待进程退出
        for /l %%i in (1,1,10) do (
            tasklist /FI "PID eq !FRONTEND_PID!" 2>nul | find /I "!FRONTEND_PID!" >nul
            if errorlevel 1 goto :frontend_stopped
            timeout /t 1 /nobreak >nul
        )
        
        :frontend_stopped
        REM 如果进程仍在运行，再次强制杀死
        tasklist /FI "PID eq !FRONTEND_PID!" 2>nul | find /I "!FRONTEND_PID!" >nul
        if not errorlevel 1 (
            echo Force killing frontend...
            taskkill /PID !FRONTEND_PID! /F >nul 2>&1
        )
        
        del /f /q "%FRONTEND_PID_FILE%" 2>nul
        echo Frontend stopped
    ) else (
        echo Frontend is not running
        del /f /q "%FRONTEND_PID_FILE%" 2>nul
    )
) else (
    echo Frontend PID file not found
)

REM 清理 PID 目录（如果为空）
if exist "%PID_DIR%" (
    dir /b "%PID_DIR%" 2>nul | findstr /r "." >nul
    if errorlevel 1 (
        rmdir "%PID_DIR%" 2>nul
    )
)

REM 清理可能遗留的端口占用进程（即使PID文件不存在）
echo.
echo Checking for orphaned processes on ports 8000 and 8050...

for %%p in (8000 8050) do (
    for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%%p" ^| findstr "LISTENING"') do (
        set "port_pid=%%a"
        if defined port_pid (
            echo Found process !port_pid! using port %%p, terminating...
            taskkill /PID !port_pid! /F >nul 2>&1
            timeout /t 1 /nobreak >nul
            tasklist /FI "PID eq !port_pid!" 2>nul | find /I "!port_pid!" >nul
            if not errorlevel 1 (
                taskkill /PID !port_pid! /F >nul 2>&1
            )
        )
    )
)

echo.
echo All services stopped

endlocal
exit /b 0
