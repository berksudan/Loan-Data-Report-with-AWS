#!/bin/bash

# Change current directory to the project directory.
CURRENT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"

# Paths of Python 3 and main module.
PYTHON_BIN=$CURRENT_PATH/venv/bin/python3
PYTHON_MAIN=$CURRENT_PATH/firehose_client.py

# Change directory to where bash script resides.
cd $CURRENT_PATH

# Run the program.
$PYTHON_BIN $PYTHON_MAIN