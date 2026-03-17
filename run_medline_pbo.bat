@echo off
setlocal

for %%I in ("%~dp0.") do set "PROJECT_DIR=%%~fI"
set "PYTHON_EXE=%PROJECT_DIR%\.venv\Scripts\python.exe"
set "MAIN_PY=%PROJECT_DIR%\main.py"
set "CONFIG_YAML=%PROJECT_DIR%\config.yaml"

if not exist "%PYTHON_EXE%" (
    echo ERROR: Python executable not found at "%PYTHON_EXE%".
    exit /b 1
)

if not exist "%MAIN_PY%" (
    echo ERROR: main.py not found at "%MAIN_PY%".
    exit /b 1
)

if not exist "%CONFIG_YAML%" (
    echo ERROR: config.yaml not found at "%CONFIG_YAML%".
    exit /b 1
)

"%PYTHON_EXE%" "%MAIN_PY%" --config "%CONFIG_YAML%"
set "EXIT_CODE=%ERRORLEVEL%"

endlocal & exit /b %EXIT_CODE%