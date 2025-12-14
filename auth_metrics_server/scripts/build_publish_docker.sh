#!/bin/bash
set -e
# This is supposed to be ran in the root of the repository.
docker build -t goncaloferreirauva/cim-fastapi -f ./auth_metrics_server/Dockerfile ./auth_metrics_server
docker push goncaloferreirauva/cim-fastapi