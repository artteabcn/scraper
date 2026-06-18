#!/bin/bash
set -e

echo "============================================="
echo "  Universal Lead Scraper — Local Setup"
echo "============================================="
echo ""

# Check Python
if ! command -v python3 &>/dev/null; then
    echo "[ERROR] Python 3 not found."
    echo "Install it:"
    echo "  macOS:  brew install python3"
    echo "  Ubuntu: sudo apt install python3 python3-venv python3-pip"
    exit 1
fi
echo "[OK] $(python3 --version) found"

# Create virtual environment
if [ ! -d "venv" ]; then
    echo ""
    echo "[1/4] Creating virtual environment..."
    python3 -m venv venv
    echo "[OK] Virtual environment created."
else
    echo "[OK] Virtual environment already exists."
fi

source venv/bin/activate

# Install Python dependencies
echo ""
echo "[2/4] Installing Python dependencies..."
pip install --prefer-binary -r requirements.txt --quiet
echo "[OK] Dependencies installed."

# Install Playwright browser
echo ""
echo "[3/4] Installing Playwright Chromium browser (first time only, ~150MB)..."
playwright install chromium
echo "[OK] Chromium browser ready."

# Launch server
echo ""
echo "[4/4] Starting server..."
echo ""
echo "============================================="
echo "  Dashboard: http://localhost:8000"
echo "  Press Ctrl+C to stop the server."
echo "============================================="
echo ""
python universal_scraper.py --server
