#!/bin/bash
host_code="$1"

# Make cURL request
curl -X POST \
  -H "Content-Type: application/json" \
  -d "{\"host_code\": \"$host_code\"}" \
  https://msd-dev.atarev.dev/api/msdv2/rules/evaluate-events