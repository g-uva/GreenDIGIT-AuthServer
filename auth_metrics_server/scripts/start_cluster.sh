#!/bin/bash

# This script is necessary for the docker-compose as it generates the JWT token.
# Please write your `USER_EMAIL` and `USER_PASSWORD` into the .env file.

# Script for the .env
set -e
python3 -m venv . 2>/dev/null || true
. bin/activate
pwd
cd auth_metrics_server
pip install -r requirements.txt
cd ..
python3 auth_metrics_server/get_bearer_token/get_bearer_token.py
python3 get_wattprint_token/get_wattprint_token.py
docker compose up -d --force-recreate --no-deps