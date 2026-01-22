@echo off
setlocal enabledelayedexpansion

set "WORKDIR=C:\Work\lws_workflow"
set "SCRIPT=%WORKDIR%\app.py"
set "LOG_DIR=%WORKDIR%\logs"
set "PY=C:\Users\rdevelopment\AppData\Local\Programs\Python\Python313\python.exe"

set "BASELOG=%LOG_DIR%\scheduler.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Rotate log (daily + size) and keep only 5
powershell -NoProfile -ExecutionPolicy Bypass -File "%WORKDIR%\scripts\rotate_logs.ps1" ^
  -LogFile "%BASELOG%" -MaxMB 5 -Keep 5 -RotateDaily -Compress

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd HH:mm:ss"') do set "TS=%%i"

echo ====================================================== >> "%BASELOG%"
echo ===== RUN START: %TS%  User: %USERNAME% ===== >> "%BASELOG%"
echo ====================================================== >> "%BASELOG%"

cd /d "%WORKDIR%"

"%PY%" "%SCRIPT%" >> "%BASELOG%" 2>&1
set "RC=%ERRORLEVEL%"

echo ====================================================== >> "%BASELOG%"
echo ===== RUN END: %TS%  ExitCode=%RC% ===== >> "%BASELOG%"
echo ====================================================== >> "%BASELOG%"

if not "%RC%"=="0" (
    echo [ERROR] Workflow failed. Sending alert email... >> "%BASELOG%"
    "%PY%" "%WORKDIR%\send_fail_email.py" "%BASELOG%" "%RC%" >> "%BASELOG%" 2>&1
)

exit /b %RC%
