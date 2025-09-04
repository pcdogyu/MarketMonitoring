@echo off
REM Ensure UTF-8 output and disable colored logs on Windows consoles
set PYTHONUTF8=1
call mmenv\Scripts\activate
uvicorn server:app --reload --host 0.0.0.0 --port 8001 --no-use-colors --log-config log_config.json
