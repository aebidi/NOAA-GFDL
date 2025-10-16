#!/bin/bash

# this script is a robust wrapper for running the GFDL Python pipeline.
# it handles directory changes, virtual environment activation and a lock file
# to ensure that only one instance of the pipeline runs at a time.

# --- Configuration ---
# set the absolute path to the project directory (where main.py is)
PROJECT_DIR="/mnt/datalake/abdullah/GFDL/gfdl_pipeline"

# set the absolute path to the virtual environment's 'activate' script
VENV_ACTIVATE_SCRIPT="/mnt/datalake/abdullah/GFDL/venv/bin/activate"

# set the path for the lock file
LOCKFILE="$PROJECT_DIR/pipeline.lock"

# --- 1. Change to the Project Directory ---
# this is critical so that 'main.py' can find 'config.yaml' and the 'modules' folder
cd "$PROJECT_DIR" || { echo "Failed to change directory to $PROJECT_DIR. Aborting."; exit 1; }

# --- 2. Implement Locking Mechanism ---
# check if the lock file already exists
if [ -e "$LOCKFILE" ]; then
    echo "Pipeline is already running. Lock file exists: $LOCKFILE. Aborting."
    exit 1
else
    # create the lock file
    touch "$LOCKFILE"
    # set a trap: this command ensures that the lock file is automatically
    # removed when the script exits, for any reason (success or failure)
    trap 'rm -f "$LOCKFILE"' EXIT
fi

# --- 3. Activate the Python Virtual Environment ---
# this ensures we use the correct Python interpreter and have all our libraries.
# the script will now use the explicit path you provided
source "$VENV_ACTIVATE_SCRIPT" || { echo "Failed to activate virtual environment at $VENV_ACTIVATE_SCRIPT. Aborting."; exit 1; }

# --- 4. Run the Main Python Pipeline Script ---
echo "Starting GFDL pipeline run at $(date)"
python main.py
echo "GFDL pipeline run finished at $(date)"

# trap command will automatically remove the lock file now.
