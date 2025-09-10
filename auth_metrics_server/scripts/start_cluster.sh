#!/bin/bash

# This script is necessary for the docker-compose as it generates the JWT token.
# Please write your `USER_EMAIL` and `USER_PASSWORD` into the .env file.

# Script for the .env
set -e
python3 -m venv . 2>/dev/null || true
. bin/activate
pip install -r requirements.txt
python3 get_bearer_token/get_bearer_token.py
docker compose up -d --force-recreate --no-deps