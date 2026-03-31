from __future__ import annotations

import argparse
import glob
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from contracts.common import utc_now_iso, write_json

SEVERITY_DEDUCTIONS = {"CRITICAL": 20, "HIGH": 10, "MEDIUM": 5, "LOW": 1, "WARNING": 1}


def compute_health_score(reports: list[dict[str, Any]]) -> int:
    score = 100
    for report in reports:
        for result in report.get("results", []):
            if result.get("status") in {"FAIL", "ERROR"}:
                score -= SEVERITY_DEDUCTIONS.get(result.get("severity", "LOW"), 1)
    return max(0, min(100, score))


def plain_language_violation(result: dict[str, Any]) -> str:
    return (
        f"The {result.get('column_name')} field failed its {result.get('check_type')} check. "
        f"Expected {result.get('expected')} but found {result.get('actual_value')}. "
        f"Affected records: {result.get('records_failing', 'unknown')}."
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reports", default="validation_reports")
    parser.add_argument("--violations", default="violation_log/violations.jsonl")
    parser.add_argument("--ai", default="validation_reports/ai_extensions.json")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    reports = []
    for path in glob.glob(str(Path(args.reports) / "*.json")):
        with open(path, "r", encoding="utf-8") as f:
            reports.append(json.load(f))

    failures = [r for report in reports for r in report.get("results", []) if r.get("status") in {"FAIL", "ERROR"}]
    severity_counts = {sev: sum(1 for item in failures if item.get("severity") == sev) for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "WARNING"]}
    top = sorted(failures, key=lambda x: SEVERITY_DEDUCTIONS.get(x.get("severity", "LOW"), 1), reverse=True)[:3]

    ai_metrics = {}
    if Path(args.ai).exists():
        with open(args.ai, "r", encoding="utf-8") as f:
            ai_metrics = json.load(f)

    violation_count = 0
    if Path(args.violations).exists():
        with open(args.violations, "r", encoding="utf-8") as f:
            violation_count = sum(1 for _ in f if _.strip())

    score = compute_health_score(reports)
    payload = {
        "generated_at": utc_now_iso(),
        "period": f"{(datetime.utcnow() - timedelta(days=7)).date()} to {datetime.utcnow().date()}",
        "data_health_score": score,
        "health_narrative": f"Score of {score}/100. {'No critical issues.' if severity_counts['CRITICAL'] == 0 else 'Critical issues require immediate action.'}",
        "violations_this_week": severity_counts,
        "top_violations": [plain_language_violation(item) for item in top],
        "ai_system_risk_assessment": ai_metrics,
        "violation_log_entries": violation_count,
        "recommended_actions": [
            "Review and lock the extracted_facts.confidence contract clause.",
            "Add ValidationRunner to CI before downstream pipelines execute.",
            "Run schema evolution diff before each release that changes JSON output.",
        ],
    }
    write_json(args.output, payload)
    print(f"Wrote enforcer report: {args.output}")


if __name__ == "__main__":
    main()
