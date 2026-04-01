from __future__ import annotations

import argparse
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {line_no} in {path}: {e}") from e
            if not isinstance(obj, dict):
                raise ValueError(f"Line {line_no} in {path} is not a JSON object")
            records.append(obj)
    if not records:
        raise ValueError(f"No records found in {path}")
    return records


def make_hashable(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return value


def flatten_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    flat_rows: list[dict[str, Any]] = []

    for record in records:
        row: dict[str, Any] = {}

        for key, value in record.items():
            if isinstance(value, dict):
                for sub_k, sub_v in value.items():
                    row[f"{key}.{sub_k}"] = make_hashable(sub_v)

            elif isinstance(value, list):
                row[f"{key}.__len__"] = len(value)

                if value and all(isinstance(item, dict) for item in value):
                    first = value[0]
                    for sub_k in first.keys():
                        extracted = [item.get(sub_k) for item in value if isinstance(item, dict)]
                        sample_non_null = [x for x in extracted if x is not None]
                        if not sample_non_null:
                            continue

                        col_name = f"{key}[].{sub_k}"
                        if all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in sample_non_null):
                            row[col_name] = float(pd.Series(sample_non_null).mean())
                        else:
                            row[col_name] = make_hashable(sample_non_null[0])
                else:
                    row[key] = make_hashable(value)

            else:
                row[key] = value

        flat_rows.append(row)

    return pd.DataFrame(flat_rows)


def load_contract(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def severity_for_status(status: str, default: str = "LOW") -> str:
    if status == "ERROR":
        return "CRITICAL"
    if status == "FAIL":
        return default
    if status == "WARN":
        return "WARNING"
    return "LOW"


def add_result(
    results: list[dict[str, Any]],
    check_id: str,
    column_name: str,
    check_type: str,
    status: str,
    actual_value: str,
    expected: str,
    severity: str,
    records_failing: int = 0,
    sample_failing: list[str] | None = None,
    message: str = "",
) -> None:
    results.append(
        {
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
    )


def read_baselines(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("columns", {})


def write_baselines(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    baselines: dict[str, Any] = {}
    for col in df.columns:
        numeric = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(numeric) == 0:
            continue
        baselines[col] = {
            "mean": float(numeric.mean()),
            "stddev": float(numeric.std()) if len(numeric) > 1 else 0.0,
        }
    with path.open("w", encoding="utf-8") as f:
        json.dump({"written_at": utc_now_iso(), "columns": baselines}, f, indent=2)


def check_statistical_drift(column: str, current_mean: float, baselines: dict[str, Any]) -> dict[str, Any] | None:
    if column not in baselines:
        return None
    baseline = baselines[column]
    baseline_std = max(float(baseline.get("stddev", 0.0)), 1e-9)
    z_score = abs(current_mean - float(baseline.get("mean", 0.0))) / baseline_std

    if z_score > 3:
        return {
            "status": "FAIL",
            "severity": "HIGH",
            "z_score": round(z_score, 2),
            "message": f"{column} mean drifted {z_score:.1f} stddev from baseline",
        }
    if z_score > 2:
        return {
            "status": "WARN",
            "severity": "MEDIUM",
            "z_score": round(z_score, 2),
            "message": f"{column} mean within warning range ({z_score:.1f} stddev)",
        }
    return {
        "status": "PASS",
        "severity": "LOW",
        "z_score": round(z_score, 2),
        "message": f"{column} stable vs baseline",
    }


def run_checks(contract: dict[str, Any], df: pd.DataFrame) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    schema = contract.get("schema", {})
    baselines_path = Path("schema_snapshots") / "baselines.json"
    baselines = read_baselines(baselines_path)

    for column_name, clause in schema.items():
        check_prefix = f"{contract['id']}.{column_name}"

        if column_name not in df.columns:
            add_result(
                results,
                f"{check_prefix}.exists",
                column_name,
                "exists",
                "ERROR",
                "missing column",
                "column present",
                "CRITICAL",
                message=f"Column '{column_name}' does not exist in flattened dataset.",
            )
            continue

        series = df[column_name]
        safe_series = series.map(make_hashable)
        non_null = safe_series.dropna()

        if clause.get("required", False):
            missing = int(series.isna().sum())
            status = "PASS" if missing == 0 else "FAIL"
            add_result(
                results,
                f"{check_prefix}.required",
                column_name,
                "required",
                status,
                f"missing={missing}",
                "missing=0",
                "CRITICAL" if missing else "LOW",
                records_failing=missing,
                message=f"Required-field check for {column_name}",
            )

        expected_type = clause.get("type")
        if expected_type in {"number", "integer"}:
            numeric = pd.to_numeric(series, errors="coerce")
            invalid = int(series.notna().sum() - numeric.notna().sum())
            status = "PASS" if invalid == 0 else "FAIL"
            add_result(
                results,
                f"{check_prefix}.type",
                column_name,
                "type",
                status,
                f"invalid_numeric={invalid}",
                f"type={expected_type}",
                "CRITICAL" if invalid else "LOW",
                records_failing=invalid,
                message=f"Type check for {column_name}",
            )
        elif expected_type == "string":
            add_result(
                results,
                f"{check_prefix}.type",
                column_name,
                "type",
                "PASS",
                f"dtype={series.dtype}",
                "type=string",
                "LOW",
                message=f"Type check for {column_name}",
            )

        if "enum" in clause:
            allowed = set(clause["enum"])
            bad_values = [str(v) for v in non_null if str(v) not in allowed]
            status = "PASS" if not bad_values else "FAIL"
            add_result(
                results,
                f"{check_prefix}.enum",
                column_name,
                "enum",
                status,
                f"invalid_count={len(bad_values)}",
                f"allowed={sorted(allowed)}",
                "CRITICAL" if bad_values else "LOW",
                records_failing=len(bad_values),
                sample_failing=bad_values[:5],
                message=f"Enum conformance check for {column_name}",
            )

        if clause.get("format") == "uuid":
            invalid = [str(v) for v in non_null if not UUID_RE.fullmatch(str(v))]
            status = "PASS" if not invalid else "FAIL"
            add_result(
                results,
                f"{check_prefix}.uuid",
                column_name,
                "format",
                status,
                f"invalid_uuid_count={len(invalid)}",
                "uuid format",
                "CRITICAL" if invalid else "LOW",
                records_failing=len(invalid),
                sample_failing=invalid[:5],
                message=f"UUID format check for {column_name}",
            )

        if clause.get("format") == "date-time":
            invalid = [str(v) for v in non_null if parse_iso(str(v)) is None]
            status = "PASS" if not invalid else "FAIL"
            add_result(
                results,
                f"{check_prefix}.datetime",
                column_name,
                "format",
                status,
                f"invalid_datetime_count={len(invalid)}",
                "ISO 8601 datetime",
                "CRITICAL" if invalid else "LOW",
                records_failing=len(invalid),
                sample_failing=invalid[:5],
                message=f"Datetime format check for {column_name}",
            )

        if "minimum" in clause or "maximum" in clause:
            numeric = pd.to_numeric(series, errors="coerce").dropna()
            if len(numeric) == 0:
                add_result(
                    results,
                    f"{check_prefix}.range",
                    column_name,
                    "range",
                    "ERROR",
                    "non-numeric or empty column",
                    f"min>={clause.get('minimum')} max<={clause.get('maximum')}",
                    "CRITICAL",
                    message=f"Range check could not run for {column_name}",
                )
            else:
                min_ok = True if "minimum" not in clause else float(numeric.min()) >= float(clause["minimum"])
                max_ok = True if "maximum" not in clause else float(numeric.max()) <= float(clause["maximum"])
                status = "PASS" if (min_ok and max_ok) else "FAIL"

                failing_mask = pd.Series([False] * len(series))
                coerced = pd.to_numeric(series, errors="coerce")
                if "minimum" in clause:
                    failing_mask = failing_mask | (coerced < float(clause["minimum"]))
                if "maximum" in clause:
                    failing_mask = failing_mask | (coerced > float(clause["maximum"]))

                add_result(
                    results,
                    f"{check_prefix}.range",
                    column_name,
                    "range",
                    status,
                    f"min={float(numeric.min()):.4f}, max={float(numeric.max()):.4f}, mean={float(numeric.mean()):.4f}",
                    f"min>={clause.get('minimum')} max<={clause.get('maximum')}",
                    "CRITICAL" if status == "FAIL" else "LOW",
                    records_failing=int(failing_mask.fillna(False).sum()),
                    sample_failing=[str(v) for v in series[failing_mask.fillna(False)].head(5).tolist()],
                    message=f"Range check for {column_name}",
                )

                drift = check_statistical_drift(column_name, float(numeric.mean()), baselines)
                if drift:
                    add_result(
                        results,
                        f"{check_prefix}.drift",
                        column_name,
                        "statistical_drift",
                        drift["status"],
                        f"mean={float(numeric.mean()):.4f}, z={drift['z_score']}",
                        "z<=2 WARN threshold, z<=3 FAIL threshold",
                        drift["severity"],
                        message=drift["message"],
                    )

    if not baselines:
        write_baselines(Path("schema_snapshots") / "baselines.json", df)

    return results


def build_report(contract: dict[str, Any], data_path: Path, results: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = [r["status"] for r in results]
    return {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract["id"],
        "snapshot_id": sha256_file(data_path),
        "run_timestamp": utc_now_iso(),
        "total_checks": len(results),
        "passed": sum(1 for s in statuses if s == "PASS"),
        "failed": sum(1 for s in statuses if s == "FAIL"),
        "warned": sum(1 for s in statuses if s == "WARN"),
        "errored": sum(1 for s in statuses if s == "ERROR"),
        "results": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a dataset against a generated contract.")
    parser.add_argument("--contract", required=True, help="Path to contract YAML")
    parser.add_argument("--data", required=True, help="Path to JSONL data file")
    parser.add_argument("--output", required=True, help="Path to validation report JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    contract = load_contract(args.contract)
    records = load_jsonl(args.data)
    df = flatten_records(records)

    results = run_checks(contract, df)
    report = build_report(contract, Path(args.data), results)

    ensure_dir(args.output)
    with Path(args.output).open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Wrote validation report: {args.output}")


if __name__ == "__main__":
    main()