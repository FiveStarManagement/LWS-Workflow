@echo off
setlocal enabledelayedexpansion

REM ---------------------------------------------------------
REM LWS Daily HOLD Summary Email Runner (Rotating Log)
REM  - Writes to logs\daily_hold_summary.log
REM  - Rotates daily OR when > 5MB
REM  - Keeps 5 rotated logs (zipped)
REM ---------------------------------------------------------

REM ✅ Project root
set "ROOT=C:\Work\lws_workflow"

REM ✅ Force 64-bit Python (DO NOT CHANGE)
set "PYTHON_EXE=C:\Users\rdevelopment\AppData\Local\Programs\Python\Python313\python.exe"

REM ✅ Log folder
set "LOG_DIR=%ROOT%\logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM ✅ Single base log file
set "BASELOG=%LOG_DIR%\daily_hold_summary.log"

cd /d "%ROOT%"

REM ✅ Rotate log before running (daily + size, keep 5, compress)
powershell -NoProfile -ExecutionPolicy Bypass -File "%ROOT%\scripts\rotate_logs.ps1" ^
  -LogFile "%BASELOG%" -MaxMB 5 -Keep 5 -RotateDaily -Compress

REM ✅ Timestamp for header
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd HH:mm:ss"') do set "TS=%%i"

echo ========================================================= >> "%BASELOG%"
echo ===== HOLD SUMMARY START: %TS%  User=%USERNAME% ===== >> "%BASELOG%"
echo ========================================================= >> "%BASELOG%"

REM ✅ Run the python summary email script
"%PYTHON_EXE%" scripts\daily_hold_summary_email.py >> "%BASELOG%" 2>&1
set "RC=%ERRORLEVEL%"

REM ✅ Timestamp for end header
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd HH:mm:ss"') do set "TS=%%i"

echo ========================================================= >> "%BASELOG%"
echo ===== HOLD SUMMARY END: %TS%  ExitCode=%RC% ===== >> "%BASELOG%"
echo ========================================================= >> "%BASELOG%"
echo. >> "%BASELOG%"

REM ✅ Optional: email alert if hold summary fails
if not "%RC%"=="0" (
    echo [ERROR] Daily HOLD Summary failed. Sending alert... >> "%BASELOG%"
    "%PYTHON_EXE%" "%ROOT%\send_fail_email.py" "%BASELOG%" "%RC%" >> "%BASELOG%" 2>&1
)

endlocal
exit /b %RC%
