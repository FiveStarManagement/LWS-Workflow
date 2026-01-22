@echo off
setlocal enabledelayedexpansion
title LWS_Admin: Port 9595

REM ==========================================
REM LWS Admin Runner (Rotating Log)
REM  - Writes to logs\admin.log
REM  - Rotates daily OR when > 5MB
REM  - Keeps 5 rotated logs (zipped)
REM ==========================================

REM ===== Config =====
set "WORKDIR=C:\Work\lws_workflow"
set "LOG_DIR=%WORKDIR%\logs"
set "HOST=0.0.0.0"
set "PORT=9595"

REM ✅ Force 64-bit Python
set "PY=C:\Users\rdevelopment\AppData\Local\Programs\Python\Python313\python.exe"

REM ----- Ensure log dir exists -----
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM ----- Base log file (single file) -----
set "BASELOG=%LOG_DIR%\admin.log"

cd /d "%WORKDIR%"

:START

REM ✅ Rotate log before starting (daily + size, keep 5, compress)
powershell -NoProfile -ExecutionPolicy Bypass -File "%WORKDIR%\scripts\rotate_logs.ps1" ^
  -LogFile "%BASELOG%" -MaxMB 5 -Keep 5 -RotateDaily -Compress

REM Locale-safe timestamp for header
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd HH:mm:ss"') do set "TS=%%i"

echo ====================================================== >> "%BASELOG%"
echo ===== ADMIN START: %TS%  Host=%HOST%  Port=%PORT%  User=%USERNAME% ===== >> "%BASELOG%"
echo ====================================================== >> "%BASELOG%"

REM ✅ Run waitress (stdout-only logging; BAT owns admin.log rotation)
set "LWS_STDOUT_ONLY=1"
"%PY%" -m waitress --host=%HOST% --port=%PORT% admin:app >> "%BASELOG%" 2>&1
set "LWS_STDOUT_ONLY="


set "RC=%ERRORLEVEL%"

REM Timestamp for exit header
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd HH:mm:ss"') do set "TS=%%i"

echo ====================================================== >> "%BASELOG%"
echo ===== ADMIN EXIT: %TS%  ExitCode=%RC% ===== >> "%BASELOG%"
echo ====================================================== >> "%BASELOG%"

echo Server stopped/crashed (ExitCode=%RC%). Restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto START
