@echo off
title LWS_ADmin: Port 5050
cd /d "D:\Work\LWS\lws_workflow"
:start
echo Starting the Waitress server...
waitress-serve --host=0.0.0.0 --port=5050 admin:app
echo Server crashed. Restarting in 5 seconds...
timeout /t 5
goto start