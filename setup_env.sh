#!/bin/bash

# --- Configuration ---
BASE_DIR=$(pwd)
DRIVER_VENV="$BASE_DIR/driver_env"
EXECUTOR_ZIP="$BASE_DIR/executor_env.zip"
PYTHON_EXE="/usr/bin/python3.8"

echo "🚀 Starting environment synchronization processin $BASE_DIR..."

# 1. Cleanup old environments
echo "🧹 Cleaning up existing venv and zip files..."
rm -rf $DRIVER_VENV
rm -f $EXECUTOR_ZIP

# 2. Create Local Driver Environment
echo "📦 Creating local Driver venv at $DRIVER_VENV..."
$PYTHON_EXE -m venv $DRIVER_VENV
source $DRIVER_VENV/bin/activate

# 3. Upgrade basic pip tools
echo "📥 Upgrading pip and essential build tools..."
pip install --upgrade pip setuptools wheel -q

# 4. Install Dependencies
# Note: numpy<2.0.0 is explicitly required for version compatibility
echo "📥 Installing required libraries (numpy, pandas, spacy, presidio)..."
pip install "numpy<2.0.0" -q
pip install pandas pyarrow requests spacy==3.7.5 presidio-analyzer requests-kerberos -q

# 5. Download NLP Model
echo "🧠 Downloading Spacy model: en_core_web_sm..."
python -m spacy download en_core_web_sm

# 6. Package for Spark Executors
# Store the current path
CURR_DIR=$(pwd)
# Navigate to site-packages to zip contents from within
cd $DRIVER_VENV/lib/python3.8/site-packages
zip -r $EXECUTOR_ZIP . > /dev/null
# Return to original directory
cd $CURR_DIR

echo "------------------------------------------------"
echo "✅ SUCCESS: Environments are ready!"
echo "📍 Local Driver Venv: $DRIVER_VENV"
echo "📍 Spark Executor Zip: $EXECUTOR_ZIP"
echo "------------------------------------------------"

# Keep the driver environment activated for the user
# deactivate # Uncomment if you want to exit the venv automatically