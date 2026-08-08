#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin 2>/dev/null || true

superset init

exec superset run -p 8088 -h 0.0.0.0
