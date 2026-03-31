# Data Contract Enforcer

Starter repo for the TRP1 Week 7 project.

## Repo layout
- `contracts/` entry-point scripts
- `generated_contracts/` generated YAML contracts
- `validation_reports/` validation outputs
- `violation_log/` attributed violations
- `schema_snapshots/` versioned contract snapshots and baselines
- `enforcer_report/` machine-generated report artifacts
- `outputs/` input JSONL files from prior weeks

## Quick start
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run order
```bash
python contracts/generator.py \
  --source outputs/week3/extractions.jsonl \
  --contract-id week3-document-refinery-extractions \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --output generated_contracts/

python contracts/runner.py \
  --contract generated_contracts/week3_extractions.yaml \
  --data outputs/week3/extractions.jsonl \
  --output validation_reports/clean_run.json

python contracts/attributor.py \
  --violation validation_reports/clean_run.json \
  --lineage outputs/week4/lineage_snapshots.jsonl \
  --contract generated_contracts/week3_extractions.yaml \
  --output violation_log/violations.jsonl

python contracts/schema_analyzer.py \
  --contract-id week3-document-refinery-extractions \
  --output validation_reports/schema_evolution.json

python contracts/ai_extensions.py \
  --mode all \
  --extractions outputs/week3/extractions.jsonl \
  --verdicts outputs/week2/verdicts.jsonl \
  --output validation_reports/ai_extensions.json

python contracts/report_generator.py \
  --reports validation_reports \
  --violations violation_log/violations.jsonl \
  --ai validation_reports/ai_extensions.json \
  --output enforcer_report/report_data.json
```
