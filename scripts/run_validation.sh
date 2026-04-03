#!/bin/bash
set -e

python -m contracts.runner \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/latest_validation.json

if grep -q '"status": "FAIL"' validation_reports/latest_validation.json; then
  echo "Validation failed"
  exit 1
fi

if grep -q '"status": "ERROR"' validation_reports/latest_validation.json; then
  echo "Validation errored"
  exit 1
fi

echo "Validation passed"