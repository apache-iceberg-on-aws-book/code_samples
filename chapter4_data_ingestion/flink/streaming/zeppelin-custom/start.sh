#!/bin/bash

# Wait and check if Flink is running
echo "Waiting for Flink to start..."

# Start Zeppelin
echo "Starting Zeppelin..."
if ! $ZEPPELIN_HOME/bin/zeppelin.sh; then
  echo "Failed to start Zeppelin. Exiting with error."
  exit 1
fi
echo "Zeppelin started successfully"