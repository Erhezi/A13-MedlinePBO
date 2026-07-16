@echo off
setlocal

for %%I in ("%~dp0.") do set "PROJECT_DIR=%%~fI"
set "PYTHON_EXE=%PROJECT_DIR%\venv\Scripts\python.exe"
set "MAIN_PY=%PROJECT_DIR%\main.py"
set "CONFIG_YAML=%PROJECT_DIR%\reports\medline_allocation\config.yaml"

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

if defined BAD_ARG (
    echo ERROR: unknown argument "%BAD_ARG%". Use --test or --prd, optionally debug.
    exit /b 1
)

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

"%PYTHON_EXE%" "%MAIN_PY%" --report medline_allocation --config "%CONFIG_YAML%" --mode %MODE%
set "EXIT_CODE=%ERRORLEVEL%"

:: ---- Pause only when explicitly asked (debug mode) ----
if defined DEBUG (
    echo.
    echo Mode: %MODE%
    echo Exit code: %EXIT_CODE%
    echo.
    pause
)

endlocal & exit /b %EXIT_CODE%
