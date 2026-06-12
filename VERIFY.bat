@echo off
REM ============================================
REM Crypto Analyzer - Pre-Build Verification
REM ============================================

setlocal enabledelayedexpansion

echo.
echo ============================================
echo Pre-Build Verification Checklist
echo ============================================
echo.

set errors=0

REM Check files
echo Checking files...

if not exist "app_main.py" (
    echo Missing: app_main.py
    set /a errors+=1
) else (
    echo app_main.py found
)

if not exist "solana_alchemy.py" (
    echo Missing: solana_alchemy.py
    set /a errors+=1
) else (
    echo solana_alchemy.py found
)

if not exist "eth_alchemy.py" (
    echo Missing: eth_alchemy.py
    set /a errors+=1
) else (
    echo eth_alchemy.py found
)

if not exist "BUILD.bat" (
    echo Missing: BUILD.bat
    set /a errors+=1
) else (
    echo BUILD.bat found
)

if not exist "README_FOR_ACCOUNTANTS.txt" (
    echo Missing: README_FOR_ACCOUNTANTS.txt (optional)
) else (
    echo README_FOR_ACCOUNTANTS.txt found
)

echo.
echo Checking Python...

python --version >nul 2>&1
if errorlevel 1 (
    echo Python not found in PATH
    set /a errors+=1
) else (
    for /f "tokens=*" %%i in ('python --version') do echo ✓ %%i found
)

echo.
echo Checking Python packages...

python -c "import pandas" 2>nul
if errorlevel 1 (
    echo pandas not installed (will be installed during build)
) else (
    echo pandas available
)

python -c "import openpyxl" 2>nul
if errorlevel 1 (
    echo openpyxl not installed (will be installed during build)
) else (
    echo openpyxl available
)

python -c "import requests" 2>nul
if errorlevel 1 (
    echo requests not installed (will be installed during build)
) else (
    echo requests available
)

echo.
echo ============================================
if !errors! equ 0 (
    echo All checks passed!
    echo.
    echo You're ready to build. Run: BUILD.bat
) else (
    echo %errors% errors found. Fix them before building.
    echo.
    echo Make sure all .py files are in this folder:
    echo   - app_main.py
    echo   - solana_alchemy.py
    echo   - eth_alchemy.py
    echo   - BUILD.bat
)
echo ============================================
echo.
pause
