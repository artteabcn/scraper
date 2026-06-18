@echo off
setlocal
title Universal Lead Scraper — Setup

echo =============================================
echo   Universal Lead Scraper — Local Setup
echo =============================================
echo.

:: Check Python
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found.
    echo Please install Python 3.11+ from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)
for /f "tokens=*" %%i in ('python --version') do echo [OK] %%i found

:: Create virtual environment
if not exist venv (
    echo.
    echo [1/4] Creating virtual environment...
    python -m venv venv
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo [OK] Virtual environment created.
) else (
    echo [OK] Virtual environment already exists.
)

:: Activate
call venv\Scripts\activate.bat

:: Install Python dependencies
echo.
echo [2/4] Installing Python dependencies...
pip install --prefer-binary -r requirements.txt --quiet
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
)
echo [OK] Dependencies installed.

:: Install Playwright browser
echo.
echo [3/4] Installing Playwright Chromium browser (first time only, ~150MB)...
playwright install chromium
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to install Playwright browser.
    pause
    exit /b 1
)
echo [OK] Chromium browser ready.

:: Launch server
echo.
echo [4/4] Starting server...
echo.
echo =============================================
echo   Dashboard: http://localhost:8000
echo   Press Ctrl+C to stop the server.
echo =============================================
echo.
python universal_scraper.py --server

endlocal
pause
