@echo off
setlocal EnableDelayedExpansion

for %%I in ("%~dp0.") do set "PROJECT_DIR=%%~fI"
set "PYTHON_EXE=%PROJECT_DIR%\venv\Scripts\python.exe"
set "MAIN_PY=%PROJECT_DIR%\main.py"
set "CONFIG_YAML=%PROJECT_DIR%\reports\medline_pbo\config.yaml"

:: ---- Parse arguments: [--test or --prd] [debug], any order ----
:: Default is test so a blank or forgotten argument can never email
:: the full distribution list. Production requires an explicit --prd.
set "MODE=test"
set "DEBUG="
set "BAD_ARG="
for %%A in (%*) do (
    if /I "%%~A"=="--test" ( set "MODE=test"
    ) else if /I "%%~A"=="test" ( set "MODE=test"
    ) else if /I "%%~A"=="--prd" ( set "MODE=prd"
    ) else if /I "%%~A"=="prd" ( set "MODE=prd"
    ) else if /I "%%~A"=="debug" ( set "DEBUG=1"
    ) else ( set "BAD_ARG=%%~A" )
)

if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"
set "LOGFILE=%PROJECT_DIR%\logs\python_run.log"

echo [%date% %time%] Starting - mode: %MODE% >> "%LOGFILE%"

if defined BAD_ARG (
    echo [%date% %time%] ERROR: unknown argument "%BAD_ARG%" - use --test or --prd >> "%LOGFILE%"
    echo ERROR: unknown argument "%BAD_ARG%". Use --test or --prd, optionally debug.
    set "PY_EXIT=1"
    goto :Finish
)

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

"%PYTHON_EXE%" "%MAIN_PY%" --report medline_pbo --config "%CONFIG_YAML%" --mode %MODE%
set "PY_EXIT=%ERRORLEVEL%"
echo [%date% %time%] Finished - exit code !PY_EXIT! >> "%LOGFILE%"

:Finish
:: ---- Pause only when explicitly asked (debug mode) ----
if defined DEBUG (
    echo.
    echo Mode: %MODE%
    echo Exit code: !PY_EXIT!
    echo Log: %LOGFILE%
    echo.
    pause
)

endlocal & exit /b %PY_EXIT%
