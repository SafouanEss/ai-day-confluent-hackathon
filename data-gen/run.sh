#!/usr/bin/env bash

set -e

# Required env vars (typically sourced from credentials.env):
#   KAFKA_BOOTSTRAP_SERVERS, KAFKA_API_KEY, KAFKA_API_SECRET
#   SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_API_KEY, SCHEMA_REGISTRY_API_SECRET
# ShadowTraffic license must be set up in colima config (~/.colima/default/colima.yaml)

docker run \
    --rm \
    -e KAFKA_BOOTSTRAP_SERVERS \
    -e KAFKA_API_KEY \
    -e KAFKA_API_SECRET \
    -e SCHEMA_REGISTRY_URL \
    -e SCHEMA_REGISTRY_API_KEY \
    -e SCHEMA_REGISTRY_API_SECRET \
    --net=host \
    -v "$(pwd)/root.json:/home/root.json" \
    -v "$(pwd)/generators:/home/generators" \
    -v "$(pwd)/connections:/home/connections" \
    -v "$(pwd)/functions:/home/functions" \
    -v "$(pwd)/trucks:/home/trucks" \
    shadowtraffic/shadowtraffic:1.14.1 \
    --config /home/root.json
