$ErrorActionPreference = "Stop"

uv run python -m contracts.runner `
  --contract generated_contracts\week3_extractions.yaml `
  --data outputs\week3\extractions.jsonl `
  --output validation_reports\latest_validation.json

$content = Get-Content validation_reports\latest_validation.json -Raw

if ($content -match '"status": "FAIL"') {
    Write-Host "Validation failed"
    exit 1
}

if ($content -match '"status": "ERROR"') {
    Write-Host "Validation errored"
    exit 1
}

Write-Host "Validation passed"
exit 0