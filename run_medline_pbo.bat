@echo off
setlocal EnableDelayedExpansion

for %%I in ("%~dp0.") do set "PROJECT_DIR=%%~fI"
set "PYTHON_EXE=%PROJECT_DIR%\venv\Scripts\python.exe"
set "MAIN_PY=%PROJECT_DIR%\main.py"
set "CONFIG_YAML=%PROJECT_DIR%\reports\medline_pbo\config.yaml"

if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"
set "LOGFILE=%PROJECT_DIR%\logs\python_run.log"

echo [%date% %time%] Starting >> "%LOGFILE%"


if not exist "%PYTHON_EXE%" (
    echo [%date% %time%] ERROR: python.exe missing >> "%LOGFILE%"
    set "PY_EXIT=1"
    goto :Finish
)

if not exist "%MAIN_PY%" (
    echo [%date% %time%] ERROR: main.py missing >> "%LOGFILE%"
    set "PY_EXIT=1"
    goto :Finish
)

if not exist "%CONFIG_YAML%" (
    echo [%date% %time%] ERROR: config.yaml missing >> "%LOGFILE%"
    set "PY_EXIT=1"
    goto :Finish
)

"%PYTHON_EXE%" "%MAIN_PY%" --report medline_pbo --config "%CONFIG_YAML%"
set "EXIT_CODE=%ERRORLEVEL%"
echo [%date% %time%] Finished - exit code !PY_EXIT! >> "%LOGFILE%"

:Finish
:: ── Pause only when explicitly asked (debug mode) ──
if /I "%~1"=="debug" (
    echo.
    echo Exit code: !PY_EXIT!
    echo Log: %LOGFILE%
    echo.
    pause
)

endlocal
exit /b %PY_EXIT%