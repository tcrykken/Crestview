#!/bin/bash
set -e # exit on error

echo "running ..."

echo "go process ..."
./go/bin/processor \
    -input-file = "data\input\reservations_02NOV25.json"