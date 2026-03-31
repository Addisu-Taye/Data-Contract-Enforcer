from __future__ import annotations

import argparse
import json
import uuid
from pathlib import Path
from typing import Any

import pandas as pd

from contracts.common import flatten_records, load_jsonl, load_yaml, parse_dt, sha256_file, utc_now_iso, write_json

BASELINE_PATH = Path("schema_snapshots/baselines.json")


def load_baselines() -> dict[str, Any]:
    if BASELINE_PATH.exists():
        with open(BASELINE_PATH, "r", encoding="utf-8") as f:
            return json.load(f).get("columns", {})
    return {}


def save_baselines(df: pd.DataFrame) -> None:
    BASELINE_PATH.parent.mkdir(parents=True, exist_ok=True)
    baselines = {}
    for col in df.select_dtypes(include="number").columns:
        baselines[col] = {"mean": float(df[col].mean()), "stddev": float(df[col].std() or 0.0)}
    with open(BASELINE_PATH, "w", encoding="utf-8") as f:
        json.dump({"written_at": utc_now_iso(), "columns": baselines}, f, indent=2)


def check_statistical_drift(column: str, current_mean: float, baselines: dict[str, Any]) -> dict[str, Any] | None:
    if column not in baselines:
        return None
    baseline = baselines[column]
    stddev = max(float(baseline.get("stddev", 0.0)), 1e-9)
    z = abs(current_mean - float(baseline.get("mean", 0.0))) / stddev
    if z > 3:
        return {"status": "FAIL", "severity": "HIGH", "message": f"{column} mean drifted {z:.1f} stddev from baseline", "actual_value": f"z={z:.2f}"}
    if z > 2:
        return {"status": "WARN", "severity": "WARNING", "message": f"{column} mean within warning range ({z:.1f} stddev)", "actual_value": f"z={z:.2f}"}
    return {"status": "PASS", "severity": "LOW", "message": f"{column} stable", "actual_value": f"z={z:.2f}"}


def result_row(check_id: str, column_name: str, check_type: str, status: str, expected: str, actual_value: str, severity: str, message: str, records_failing: int = 0, sample_failing: list[Any] | None = None) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "column_name": column_name,
        "check_type": check_type,
        "status": status,
        "actual_value": actual_value,
        "expected": expected,
        "severity": severity,
        "records_failing": records_failing,
        "sample_failing": sample_failing or [],
        "message": message,
    }


def run_checks(contract: dict[str, Any], df: pd.DataFrame) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    baselines = load_baselines()
    contract_id = contract["id"]

    for field, clause in contract.get("schema", {}).items():
        if field not in df.columns:
            results.append(result_row(f"{contract_id}.{field}.exists", field, "presence", "ERROR", "column exists", "missing", "CRITICAL", f"Column {field} does not exist"))
            continue

        series = df[field]

        if clause.get("required"):
            nulls = int(series.isna().sum())
            results.append(result_row(f"{contract_id}.{field}.required", field, "required", "FAIL" if nulls else "PASS", "null_count=0", f"null_count={nulls}", "CRITICAL" if nulls else "LOW", f"{field} required check", nulls))

        expected_type = clause.get("type")
        if expected_type in {"number", "integer"}:
            numeric = pd.api.types.is_numeric_dtype(series)
            results.append(result_row(f"{contract_id}.{field}.type", field, "type", "PASS" if numeric else "FAIL", expected_type, str(series.dtype), "CRITICAL" if not numeric else "LOW", f"{field} type check"))

        if clause.get("enum"):
            bad = series.dropna()[~series.dropna().isin(clause["enum"])]
            results.append(result_row(f"{contract_id}.{field}.enum", field, "enum", "FAIL" if not bad.empty else "PASS", f"in {clause['enum']}", f"invalid={bad.nunique()}", "CRITICAL" if not bad.empty else "LOW", f"{field} enum check", int(bad.shape[0]), bad.astype(str).head(5).tolist()))

        if clause.get("format") == "date-time":
            parsed = series.dropna().apply(lambda x: parse_dt(x) is not None)
            bad_count = int((~parsed).sum()) if not parsed.empty else 0
            results.append(result_row(f"{contract_id}.{field}.datetime", field, "date-time", "FAIL" if bad_count else "PASS", "ISO 8601", f"bad={bad_count}", "CRITICAL" if bad_count else "LOW", f"{field} datetime check", bad_count))

        if clause.get("minimum") is not None or clause.get("maximum") is not None:
            if pd.api.types.is_numeric_dtype(series):
                min_value = float(series.min())
                max_value = float(series.max())
                failures = int(((series < clause.get("minimum", float("-inf"))) | (series > clause.get("maximum", float("inf")))).sum())
                results.append(result_row(
                    f"{contract_id}.{field}.range",
                    field,
                    "range",
                    "FAIL" if failures else "PASS",
                    f"min>={clause.get('minimum')}, max<={clause.get('maximum')}",
                    f"min={min_value}, max={max_value}, mean={float(series.mean()):.4f}",
                    "CRITICAL" if failures else "LOW",
                    f"{field} range check",
                    failures,
                ))
                drift = check_statistical_drift(field, float(series.mean()), baselines)
                if drift:
                    results.append(result_row(f"{contract_id}.{field}.drift", field, "drift", drift["status"], "within 2 stddev of baseline", drift["actual_value"], drift["severity"], drift["message"]))
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    contract = load_yaml(args.contract)
    records = load_jsonl(args.data)
    df = flatten_records(records)
    results = run_checks(contract, df)

    payload = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract["id"],
        "snapshot_id": sha256_file(args.data),
        "run_timestamp": utc_now_iso(),
        "total_checks": len(results),
        "passed": sum(1 for r in results if r["status"] == "PASS"),
        "failed": sum(1 for r in results if r["status"] == "FAIL"),
        "warned": sum(1 for r in results if r["status"] == "WARN"),
        "errored": sum(1 for r in results if r["status"] == "ERROR"),
        "results": results,
    }
    write_json(args.output, payload)
    if not BASELINE_PATH.exists():
        save_baselines(df)
    print(f"Wrote validation report: {args.output}")


if __name__ == "__main__":
    main()
