@echo off
REM Create venv if missing, install, and freeze exact versions to requirements.lock
set VENV=mmenv
if not exist %VENV% (
  python -m venv %VENV%
)
call %VENV%\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip freeze > requirements.lock
echo Locked packages to requirements.lock

