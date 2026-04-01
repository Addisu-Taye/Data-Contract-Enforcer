from __future__ import annotations

import argparse
import json
import re
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"JSONL file not found: {path}")

    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON on line {line_no} of {path}: {e}") from e
            if not isinstance(obj, dict):
                raise ValueError(f"Line {line_no} in {path} is not a JSON object")
            records.append(obj)

    if not records:
        raise ValueError(f"No records found in {path}")

    return records


def make_hashable(value: Any) -> Any:
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    if isinstance(value, list):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, set):
        return json.dumps(sorted(list(value)), ensure_ascii=False)
    return value


def dominant_string_pattern(values: list[str]) -> str | None:
    if not values:
        return None

    def classify(text: str) -> str:
        if re.fullmatch(r"[0-9a-fA-F-]{36}", text):
            return "uuid-like"
        if re.fullmatch(r"[a-f0-9]{64}", text):
            return "sha256-like"
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}T.*Z?", text):
            return "iso8601-like"
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
            return "identifier-like"
        if re.fullmatch(r"[A-Z][A-Za-z0-9]+", text):
            return "pascalcase-like"
        if re.fullmatch(r"[A-Z_]+", text):
            return "enum-like"
        if re.fullmatch(r"https?://\S+", text):
            return "url-like"
        if re.fullmatch(r"/\S+", text):
            return "path-like"
        return "free-text"

    patterns = [classify(v) for v in values[:100]]
    return Counter(patterns).most_common(1)[0][0]


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
                    all_sub_keys: set[str] = set()
                    for item in value:
                        all_sub_keys.update(item.keys())

                    for sub_k in sorted(all_sub_keys):
                        extracted = [
                            item.get(sub_k) for item in value if isinstance(item, dict)
                        ]
                        sample_non_null = [x for x in extracted if x is not None]

                        if not sample_non_null:
                            continue

                        col_name = f"{key}[].{sub_k}"

                        if all(
                            isinstance(x, (int, float)) and not isinstance(x, bool)
                            for x in sample_non_null
                        ):
                            row[col_name] = float(pd.Series(sample_non_null).mean())
                        elif all(isinstance(x, str) for x in sample_non_null):
                            row[col_name] = sample_non_null[0]
                        else:
                            row[col_name] = make_hashable(sample_non_null[0])

                elif value and all(not isinstance(item, (dict, list)) for item in value):
                    row[key] = make_hashable(value)
                else:
                    row[key] = make_hashable(value)

            else:
                row[key] = value

        flat_rows.append(row)

    return pd.DataFrame(flat_rows)


def infer_type(series: pd.Series) -> str:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() == len(series.dropna()) and len(series.dropna()) > 0:
        if pd.api.types.is_float_dtype(numeric):
            if all(float(v).is_integer() for v in numeric.dropna()):
                return "integer"
            return "number"

    if pd.api.types.is_bool_dtype(series):
        return "boolean"

    return "string"


def profile_column(series: pd.Series, name: str) -> dict[str, Any]:
    safe_series = series.map(make_hashable)
    non_null = safe_series.dropna()

    profile: dict[str, Any] = {
        "name": name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()) if len(series) else 0.0,
        "cardinality_estimate": int(non_null.nunique(dropna=True)) if len(non_null) else 0,
        "sample_values": [str(v) for v in non_null.astype(str).unique()[:5]],
    }

    numeric_series = pd.to_numeric(series, errors="coerce").dropna()
    if len(numeric_series) > 0:
        profile["stats"] = {
            "min": float(numeric_series.min()),
            "max": float(numeric_series.max()),
            "mean": float(numeric_series.mean()),
            "p25": float(numeric_series.quantile(0.25)),
            "p50": float(numeric_series.quantile(0.50)),
            "p75": float(numeric_series.quantile(0.75)),
            "p95": float(numeric_series.quantile(0.95)),
            "p99": float(numeric_series.quantile(0.99)),
            "stddev": float(numeric_series.std()) if len(numeric_series) > 1 else 0.0,
        }

    if infer_type(series) == "string":
        string_values = [str(v) for v in non_null.astype(str).tolist()[:100]]
        profile["dominant_pattern"] = dominant_string_pattern(string_values)

    return profile


def looks_like_uuid_column(name: str, samples: list[str]) -> bool:
    if name.endswith("_id") or name.endswith("[].fact_id") or name.endswith("[].entity_id"):
        return True
    if not samples:
        return False
    return all(re.fullmatch(r"[0-9a-fA-F-]{36}", s) for s in samples[:5])


def looks_like_datetime_column(name: str, samples: list[str]) -> bool:
    if name.endswith("_at"):
        return True
    if not samples:
        return False
    return all("T" in s and ("Z" in s or "+" in s) for s in samples[:5])


def looks_like_sha256_column(name: str, samples: list[str]) -> bool:
    if "hash" not in name:
        return False
    if not samples:
        return False
    return all(re.fullmatch(r"[a-f0-9]{64}", s) for s in samples[:5])


def maybe_enum(profile: dict[str, Any], field_name: str) -> list[str] | None:
    dtype = profile["dtype"]
    cardinality = profile["cardinality_estimate"]
    samples = profile["sample_values"]

    never_enum = {
        "doc_id",
        "source_path",
        "description",
        "text",
        "notes",
        "source_excerpt",
        "source_hash",
        "extracted_at",
        "created_at",
        "evaluated_at",
        "recorded_at",
        "occurred_at",
    }

    if field_name in never_enum:
        return None

    if "object" not in dtype and "string" not in dtype:
        return None

    if 0 < cardinality <= 10 and len(samples) == cardinality:
        if all(len(v) <= 40 for v in samples):
            return samples

    return None


def profile_type_to_contract_type(series: pd.Series) -> str:
    inferred = infer_type(series)
    if inferred in {"number", "integer", "boolean"}:
        return inferred
    return "string"


def clause_from_profile(profile: dict[str, Any], series: pd.Series) -> dict[str, Any]:
    clause: dict[str, Any] = {
        "type": profile_type_to_contract_type(series),
        "required": profile["null_fraction"] == 0.0,
    }

    samples = profile.get("sample_values", [])
    name = profile["name"]

    if looks_like_uuid_column(name, samples):
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-fA-F-]{36}$"

    if looks_like_datetime_column(name, samples):
        clause["format"] = "date-time"

    if looks_like_sha256_column(name, samples):
        clause["pattern"] = "^[a-f0-9]{64}$"

    enum_values = maybe_enum(profile, name)
    if enum_values:
        clause["enum"] = enum_values

    stats = profile.get("stats")
    if stats:
        if clause["type"] in {"integer", "number"}:
            clause["minimum_observed"] = round(stats["min"], 6)
            clause["maximum_observed"] = round(stats["max"], 6)

        if "confidence" in name.lower():
            clause["type"] = "number"
            clause["minimum"] = 0.0
            clause["maximum"] = 1.0
            clause["description"] = (
                "Confidence score. Must remain in 0.0-1.0 float range. "
                "Breaking change if converted to 0-100 scale."
            )

        if name.endswith("processing_time_ms"):
            clause["type"] = "integer"
            clause["minimum"] = 1

        if "sequence_number" in name:
            clause["type"] = "integer"
            clause["minimum"] = 1

    if name == "overall_verdict":
        clause["type"] = "string"
        clause["enum"] = ["PASS", "FAIL", "WARN"]

    if name.endswith("event_type") or name.endswith("aggregate_type"):
        clause.setdefault("pattern", "^[A-Z][A-Za-z0-9]+$")

    if name.endswith("git_commit"):
        clause["pattern"] = "^[a-f0-9]{40}$"

    if name.endswith("[].type") and "entities" in name:
        clause["type"] = "string"
        clause["enum"] = ["PERSON", "ORG", "LOCATION", "DATE", "AMOUNT", "OTHER"]

    return clause


def derive_title(contract_id: str) -> str:
    return contract_id.replace("-", " ").title()


def infer_owner(contract_id: str) -> str:
    if "week3" in contract_id:
        return "week3-team"
    if "week5" in contract_id:
        return "week5-team"
    return "data-platform"


def infer_dataset_name(source_path: str | Path) -> str:
    return Path(source_path).stem


def build_quality_checks(schema: dict[str, Any]) -> list[str]:
    checks: list[str] = []

    for field_name, clause in schema.items():
        if clause.get("required"):
            checks.append(f"missing_count({field_name}) = 0")

        if "enum" in clause:
            checks.append(f"accepted_values({field_name}) in {clause['enum']}")

        if "minimum" in clause:
            checks.append(f"min({field_name}) >= {clause['minimum']}")

        if "maximum" in clause:
            checks.append(f"max({field_name}) <= {clause['maximum']}")

    checks.append("row_count >= 1")
    return checks


def load_latest_lineage_snapshot(lineage_path: str | Path) -> dict[str, Any] | None:
    path = Path(lineage_path)
    if not path.exists():
        return None

    with path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    if not lines:
        return None

    return json.loads(lines[-1])


def inject_lineage(contract: dict[str, Any], lineage_path: str | Path, source_path: str | Path) -> dict[str, Any]:
    snapshot = load_latest_lineage_snapshot(lineage_path)
    if not snapshot:
        contract["lineage"] = {"upstream": [], "downstream": []}
        return contract

    source_name = Path(source_path).stem.lower()
    downstream: list[dict[str, Any]] = []

    for edge in snapshot.get("edges", []):
        source = str(edge.get("source", "")).lower()
        target = str(edge.get("target", "")).lower()

        if source_name in source or source_name.replace("_", "") in source.replace("_", ""):
            downstream.append(
                {
                    "id": edge.get("target"),
                    "relationship": edge.get("relationship"),
                    "fields_consumed": [],
                }
            )
        elif "week3" in source_name and "week4" in target:
            downstream.append(
                {
                    "id": edge.get("target"),
                    "relationship": edge.get("relationship"),
                    "fields_consumed": ["doc_id", "extracted_facts[].confidence"],
                }
            )

    contract["lineage"] = {
        "upstream": [],
        "downstream": downstream,
    }
    return contract


def build_contract(
    source_path: str | Path,
    contract_id: str,
    column_profiles: dict[str, dict[str, Any]],
    df: pd.DataFrame,
) -> dict[str, Any]:
    schema = {
        name: clause_from_profile(profile, df[name])
        for name, profile in column_profiles.items()
    }

    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": derive_title(contract_id),
            "version": "1.0.0",
            "owner": infer_owner(contract_id),
            "description": f"Auto-generated contract for {infer_dataset_name(source_path)}.",
        },
        "servers": {
            "local": {
                "type": "local",
                "path": str(source_path),
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract.",
            "limitations": "Generated from observed data and should be reviewed.",
        },
        "schema": schema,
        "quality": {
            "type": "SodaChecks",
            "specification": {
                f"checks for {infer_dataset_name(source_path)}": build_quality_checks(schema)
            },
        },
    }
    return contract


def build_dbt_yaml(model_name: str, schema: dict[str, Any]) -> dict[str, Any]:
    columns: list[dict[str, Any]] = []

    for field_name, clause in schema.items():
        tests: list[Any] = []

        if clause.get("required"):
            tests.append("not_null")

        if "enum" in clause:
            tests.append({"accepted_values": {"values": clause["enum"]}})

        col_entry = {
            "name": field_name,
            "description": clause.get("description", ""),
        }
        if tests:
            col_entry["tests"] = tests
        columns.append(col_entry)

    return {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": f"Auto-generated dbt tests for {model_name}",
                "columns": columns,
            }
        ],
    }


def save_yaml(data: dict[str, Any], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)


def write_schema_snapshot(contract_output_path: Path, contract_id: str) -> None:
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot_path = snapshot_dir / f"{ts}.yaml"
    shutil.copy(contract_output_path, snapshot_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a data contract from JSONL data.")
    parser.add_argument("--source", required=True, help="Path to source JSONL file")
    parser.add_argument("--contract-id", required=True, help="Contract identifier")
    parser.add_argument("--lineage", required=False, help="Path to lineage snapshot JSONL")
    parser.add_argument("--output", required=True, help="Output directory for generated contracts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_path = Path(args.source)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    records = load_jsonl(source_path)
    df = flatten_records(records)

    if df.empty:
        raise ValueError("Flattened DataFrame is empty; cannot generate contract.")

    column_profiles = {col: profile_column(df[col], col) for col in df.columns}

    contract = build_contract(
        source_path=source_path,
        contract_id=args.contract_id,
        column_profiles=column_profiles,
        df=df,
    )

    if args.lineage:
        contract = inject_lineage(contract, args.lineage, source_path)

    contract_filename = f"{source_path.stem.replace('-', '_')}.yaml"
    dbt_filename = f"{source_path.stem.replace('-', '_')}_dbt.yml"

    if "week3" in args.contract_id:
        contract_filename = "week3_extractions.yaml"
        dbt_filename = "week3_extractions_dbt.yml"
    elif "week5" in args.contract_id:
        contract_filename = "week5_events.yaml"
        dbt_filename = "week5_events_dbt.yml"

    contract_output_path = output_dir / contract_filename
    dbt_output_path = output_dir / dbt_filename

    save_yaml(contract, contract_output_path)
    save_yaml(build_dbt_yaml(source_path.stem, contract["schema"]), dbt_output_path)
    write_schema_snapshot(contract_output_path, args.contract_id)

    print(f"Wrote contract: {contract_output_path}")


if __name__ == "__main__":
    main()