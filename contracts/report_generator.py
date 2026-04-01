from __future__ import annotations

import argparse
import json
import glob
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def severity_weight(severity: str) -> int:
    weights = {
        "CRITICAL": 20,
        "HIGH": 10,
        "MEDIUM": 5,
        "LOW": 1,
        "WARNING": 3,
    }
    return weights.get(severity, 1)


def compute_health_score(validation_reports: list[dict[str, Any]], ai_report: dict[str, Any] | None) -> int:
    score = 100

    for report in validation_reports:
        for result in report.get("results", []):
            if result.get("status") in {"FAIL", "ERROR"}:
                score -= severity_weight(result.get("severity", "LOW"))

    if ai_report:
        for check in ai_report.get("checks", []):
            if check.get("status") == "FAIL":
                score -= severity_weight(check.get("severity", "LOW"))
            elif check.get("status") == "WARN":
                score -= 3

    return max(0, min(100, score))


def summarize_top_violations(validation_reports: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []

    for report in validation_reports:
        for result in report.get("results", []):
            if result.get("status") in {"FAIL", "ERROR"}:
                failures.append(result)

    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "WARNING": 4}
    failures.sort(key=lambda x: severity_order.get(x.get("severity", "LOW"), 99))

    summaries = []
    for item in failures[:limit]:
        summaries.append(
            {
                "check_id": item.get("check_id"),
                "severity": item.get("severity"),
                "summary": (
                    f"The field '{item.get('column_name')}' failed a {item.get('check_type')} check. "
                    f"Expected {item.get('expected')} but observed {item.get('actual_value')}. "
                    f"Affected records: {item.get('records_failing', 'unknown')}."
                ),
            }
        )
    return summaries


def count_violations_by_severity(validation_reports: list[dict[str, Any]], ai_report: dict[str, Any] | None) -> dict[str, int]:
    counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "WARNING": 0}

    for report in validation_reports:
        for result in report.get("results", []):
            if result.get("status") in {"FAIL", "ERROR", "WARN"}:
                severity = result.get("severity", "LOW")
                counts[severity] = counts.get(severity, 0) + 1

    if ai_report:
        for check in ai_report.get("checks", []):
            if check.get("status") in {"FAIL", "WARN"}:
                severity = check.get("severity", "LOW")
                counts[severity] = counts.get(severity, 0) + 1

    return counts


def build_recommendations(
    validation_reports: list[dict[str, Any]],
    ai_report: dict[str, Any] | None,
) -> list[str]:
    recommendations: list[str] = []

    all_results = [
        result
        for report in validation_reports
        for result in report.get("results", [])
        if result.get("status") in {"FAIL", "ERROR"}
    ]

    if any("confidence" in result.get("column_name", "") for result in all_results):
        recommendations.append(
            "Update the Week 3 extraction producer so extracted_facts.confidence remains a float in the 0.0–1.0 range."
        )

    if ai_report:
        for check in ai_report.get("checks", []):
            if check.get("check") == "confidence_drift" and check.get("status") == "FAIL":
                recommendations.append(
                    "Add a pre-deployment contract validation step to block releases when confidence scale drift is detected."
                )

    if any(result.get("status") == "ERROR" for result in all_results):
        recommendations.append(
            "Align flattening logic and contract field naming so all expected nested fields are validated consistently."
        )

    recommendations.append(
        "Run contract validation in CI before downstream systems consume new snapshots."
    )

    deduped = []
    for rec in recommendations:
        if rec not in deduped:
            deduped.append(rec)

    return deduped[:3]


def build_ai_risk_summary(ai_report: dict[str, Any] | None) -> dict[str, Any]:
    if not ai_report:
        return {
            "status": "UNKNOWN",
            "summary": "No AI validation report was provided.",
        }

    checks = ai_report.get("checks", [])
    failing = [c for c in checks if c.get("status") == "FAIL"]
    warning = [c for c in checks if c.get("status") == "WARN"]

    if failing:
        return {
            "status": "HIGH_RISK",
            "summary": f"{len(failing)} AI-specific checks failed.",
            "details": checks,
        }

    if warning:
        return {
            "status": "MEDIUM_RISK",
            "summary": f"{len(warning)} AI-specific checks are in warning state.",
            "details": checks,
        }

    return {
        "status": "LOW_RISK",
        "summary": "All AI-specific checks passed.",
        "details": checks,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate final Enforcer report data.")
    parser.add_argument("--reports", default="validation_reports/*.json", help="Glob for validation reports")
    parser.add_argument("--violations", default="violation_log/violations.jsonl", help="Violation log JSONL")
    parser.add_argument("--ai", default="validation_reports/ai_validation.json", help="AI validation report JSON")
    parser.add_argument("--output", required=True, help="Output JSON path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report_paths = sorted(glob.glob(args.reports))
    validation_reports = [load_json(path) for path in report_paths if Path(path).name != Path(args.ai).name]

    violation_log = load_jsonl(args.violations)

    ai_report = None
    ai_path = Path(args.ai)
    if ai_path.exists():
        ai_report = load_json(ai_path)

    health_score = compute_health_score(validation_reports, ai_report)
    violation_counts = count_violations_by_severity(validation_reports, ai_report)
    top_violations = summarize_top_violations(validation_reports, limit=3)
    ai_risk = build_ai_risk_summary(ai_report)
    recommendations = build_recommendations(validation_reports, ai_report)

    final_report = {
        "generated_at": utc_now_iso(),
        "data_health_score": health_score,
        "health_narrative": (
            "Critical data quality issues require immediate action."
            if health_score < 80
            else "Data quality is stable with manageable issues."
        ),
        "violations_this_week": violation_counts,
        "top_violations": top_violations,
        "schema_changes_detected": "See schema evolution report for detailed compatibility analysis.",
        "ai_system_risk_assessment": ai_risk,
        "violation_log_entries": len(violation_log),
        "recommended_actions": recommendations,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(final_report, f, indent=2)

    print(f"Wrote final report data: {output_path}")


if __name__ == "__main__":
    main()