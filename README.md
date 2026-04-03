# Data Contract Enforcer

## Overview
The Data Contract Enforcer is a system for validating, monitoring, and diagnosing data quality issues across distributed pipelines.

It enforces schema contracts, detects drift, attributes failures to upstream sources, and evaluates AI-specific risks.

---

## Pipeline Components

1. **Generator**
   - Infers schema from JSONL data
   - Produces YAML data contracts

2. **Runner**
   - Validates data against contracts
   - Detects schema violations and statistical drift

3. **Attributor**
   - Links failures to upstream systems using lineage + git history

4. **Schema Analyzer**
   - Tracks schema evolution
   - Identifies breaking vs compatible changes

5. **AI Extensions**
   - Validates AI behavior (confidence drift, structure)

6. **Report Generator**
   - Produces final health report with recommendations

---

## Project Structure


contracts/
generator.py
runner.py
attributor.py
schema_analyzer.py
ai_extensions.py
report_generator.py

outputs/
validation_reports/
violation_log/
schema_snapshots/


---

## Execution Steps

### 1. Generate contract

uv run python -m contracts.generator
--source outputs/week3/extractions.jsonl
--contract-id week3-document-refinery-extractions
--lineage outputs/week4/lineage_snapshots.jsonl
--output generated_contracts


---

### 2. Validate clean data

uv run python -m contracts.runner
--contract generated_contracts/week3_extractions.yaml
--data outputs/week3/extractions.jsonl
--output validation_reports/week3_validation.json


---

### 3. Inject violation

uv run python create_violation.py


---

### 4. Validate violated data

uv run python -m contracts.runner
--contract generated_contracts/week3_extractions.yaml
--data outputs/week3/extractions_violated.jsonl
--output validation_reports/week3_violated_validation.json


---

### 5. Attribute failure

uv run python -m contracts.attributor
--violation validation_reports/week3_violated_validation.json
--lineage outputs/week4/lineage_snapshots.jsonl
--contract generated_contracts/week3_extractions.yaml
--output violation_log/violations.jsonl


---

### 6. AI validation

uv run python -m contracts.ai_extensions
--extractions outputs/week3/extractions_violated.jsonl
--output validation_reports/ai_validation.json


---

### 7. Final report

uv run python -m contracts.report_generator
--reports "validation_reports/*.json"
--violations violation_log/violations.jsonl
--ai validation_reports/ai_validation.json
--output enforcer_report/report_data.json


---

## Key Result

The system successfully detects:

- Confidence scale bug (0–1 → 0–100)
- Statistical drift
- AI behavior anomalies
- Downstream impact (blast radius)

---
## Final Capabilities

- End-to-end data contract enforcement across pipeline stages
- Schema validation and statistical drift detection
- AI-specific validation (confidence drift, schema, entity consistency)
- Root cause attribution using lineage and git history
- Schema evolution analysis (BREAKING vs COMPATIBLE)
- CI/CD-ready validation scripts
## Outcome

Final report:
- Data Health Score: **50**
- AI Risk: **HIGH**
- Root cause identified and actionable recommendations generated