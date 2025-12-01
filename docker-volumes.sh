#!/bin/bash

# Define volume names
VOLUME_NAMES=("telegraf-input" "staros-config" "collector-input")

# Loop through the volume names and create each one
for VOLUME in "${VOLUME_NAMES[@]}"; do
    echo "Creating volume: $VOLUME"
    docker volume create "$VOLUME"
    
    if [ $? -eq 0 ]; then
        echo "Volume $VOLUME created successfully."
    else
        echo "Failed to create volume $VOLUME."
    fi
done
