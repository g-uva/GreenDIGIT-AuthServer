#!/bin/bash
set -e
docker build -t goncaloferreirauva/cim-fastapi .
docker push goncaloferreirauva/cim-fastapi