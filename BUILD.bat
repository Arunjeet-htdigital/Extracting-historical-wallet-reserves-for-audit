@echo off
REM ============================================
REM Build Crypto Analyzer .EXE for Distribution
REM ============================================
REM Run this ONCE to create CryptoAnalyzer.exe
REM Then share the .exe with accountants

echo.
echo ============================================
echo Crypto Analyzer - EXE Builder
echo ============================================
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found!
    echo Please install Python from https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [1/3] Installing build tools...
echo.

REM Use python -m pip (works with Miniconda/Anaconda)
python -m pip install --upgrade pip
python -m pip install pyinstaller pandas openpyxl requests

REM Verify PyInstaller installed
pyinstaller --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: PyInstaller failed to install
    echo Try manually: python -m pip install pyinstaller
    pause
    exit /b 1
)

echo.
echo [2/3] Building executable (this takes ~2 minutes)...
echo.

REM Build with PyInstaller directly
pyinstaller ^
    --onefile ^
    --windowed ^
    --name "CryptoAnalyzer" ^
    --hidden-import=solana_alchemy ^
    --hidden-import=eth_alchemy ^
    --hidden-import=pandas ^
    --hidden-import=openpyxl ^
    --hidden-import=requests ^
    --add-data "solana_alchemy.py:." ^
    --add-data "eth_alchemy.py:." ^
    app_main.py

echo.
echo [3/3] Verifying build...

if exist "dist\CryptoAnalyzer.exe" (
    echo.
    echo ============================================
    echo SUCCESS! ✓
    echo ============================================
    echo.
    echo Your executable is ready:
    echo   Location: dist\CryptoAnalyzer.exe
    echo.
    echo Next steps:
    echo   1. Copy "dist\CryptoAnalyzer.exe" to a folder
    echo   2. Add a README.txt file (see template below)
    echo   3. Zip the folder
    echo   4. Share with accountants
    echo.
    echo NO MORE STEPS NEEDED - They just double-click!
    echo.
    pause
) else (
    echo.
    echo ERROR: Build failed!
    echo Check the output above for errors.
    pause
    exit /b 1
)