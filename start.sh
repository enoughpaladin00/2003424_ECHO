#!/bin/bash

set -e

if [ ! -f "./source/seismic-signal-simulator-oci.tar" ]; then
    echo "❌ CRITICAL ERROR: Simulator image not found!"
    echo "Please place the provided 'seismic-signal-simulator-oci.tar' file into the './source/' directory."
    echo "Once the file is in place, run this script again."
    exit 1
fi

echo "Step 1: Loading the Seismic Simulator image from tarball..."
docker load -i ./source/seismic-signal-simulator-oci.tar

echo "Step 2: Building and starting E.C.H.O. platform with Docker Compose..."
docker compose up -d --build

echo "System is up and running in detached mode!"
echo "Gatway is accessible at http://localhost"