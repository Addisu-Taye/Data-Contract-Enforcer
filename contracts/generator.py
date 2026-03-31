from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

from contracts.common import dump_yaml, flatten_records, load_jsonl, load_jsonl as read_jsonl
from contracts.common import utc_now_iso

ENUM_THRESHOLD = 10


def infer_type(dtype: str) -> str:
    mapping = {
        "int64": "integer",
        "float64": "number",
        "bool": "boolean",
        "object": "string",
    }
    return mapping.get(dtype, "string")


def profile_column(series: pd.Series, name: str) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "name": name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()) if len(series) else 1.0,
        "cardinality_estimate": int(series.nunique(dropna=True)) if len(series) else 0,
        "sample_values": [str(v) for v in series.dropna().astype(str).unique()[:5]],
    }
    if pd.api.types.is_numeric_dtype(series):
        clean = series.dropna()
        if not clean.empty:
            profile["stats"] = {
                "min": float(clean.min()),
                "max": float(clean.max()),
                "mean": float(clean.mean()),
                "p25": float(clean.quantile(0.25)),
                "p50": float(clean.quantile(0.50)),
                "p75": float(clean.quantile(0.75)),
                "p95": float(clean.quantile(0.95)),
                "p99": float(clean.quantile(0.99)),
                "stddev": float(clean.std() or 0.0),
            }
    return profile


def column_to_clause(profile: dict[str, Any]) -> dict[str, Any]:
    clause: dict[str, Any] = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0,
        "description": f"Auto-generated clause for {profile['name']}",
    }
    name = profile["name"]
    if name.endswith("_id") or name.split(".")[-1].endswith("_id"):
        clause["format"] = "uuid"
        clause["pattern"] = "^[0-9a-fA-F-]{36}$"
    if name.endswith("_at") or name.split(".")[-1].endswith("_at"):
        clause["format"] = "date-time"
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = "Confidence score. Must remain a float in the 0.0-1.0 range."
    if profile["cardinality_estimate"] and profile["cardinality_estimate"] <= ENUM_THRESHOLD and clause["type"] == "string":
        values = profile["sample_values"]
        if len(values) == profile["cardinality_estimate"]:
            clause["enum"] = values
    return clause


def build_contract(contract_id: str, source: str, column_profiles: dict[str, dict[str, Any]]) -> dict[str, Any]:
    contract = {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": contract_id.replace("-", " ").title(),
            "version": "1.0.0",
            "owner": "week7-team",
            "description": "Auto-generated baseline contract.",
        },
        "servers": {
            "local": {
                "type": "local",
                "path": source,
                "format": "jsonl",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract.",
            "limitations": "Generated baseline. Review high-risk clauses before production use.",
        },
        "schema": {},
        "quality": {
            "type": "CustomChecks",
            "specification": {
                "checks": []
            }
        },
        "lineage": {
            "upstream": [],
            "downstream": []
        },
        "llm_annotations": [],
        "generated_at": utc_now_iso(),
    }
    for name, profile in column_profiles.items():
        contract["schema"][name] = column_to_clause(profile)
        if "stats" in profile:
            stats = profile["stats"]
            if "confidence" in name:
                contract["quality"]["specification"]["checks"].append(
                    f"{name}: min>={stats['min']:.4f}, max<={stats['max']:.4f}, mean={stats['mean']:.4f}"
                )
    return contract


def inject_lineage(contract: dict[str, Any], lineage_path: str | None) -> dict[str, Any]:
    if not lineage_path or not Path(lineage_path).exists():
        return contract
    snapshots = read_jsonl(lineage_path)
    if not snapshots:
        return contract
    latest = snapshots[-1]
    edges = latest.get("edges", [])
    consumers = []
    for edge in edges:
        source = edge.get("source", "")
        if "week3" in source or "week5" in source or "extraction" in source or "event" in source:
            consumers.append({
                "id": edge.get("target"),
                "fields_consumed": ["doc_id", "extracted_facts", "payload"],
                "breaking_if_changed": ["extracted_facts.confidence", "payload", "sequence_number"],
            })
    contract["lineage"]["downstream"] = consumers[:10]
    return contract


def write_dbt_schema(output_dir: Path, stem: str, contract: dict[str, Any]) -> None:
    dbt = {
        "version": 2,
        "models": [
            {
                "name": stem,
                "columns": [],
            }
        ]
    }
    for name, clause in contract["schema"].items():
        tests = []
        if clause.get("required"):
            tests.append("not_null")
        if clause.get("enum"):
            tests.append({"accepted_values": {"values": clause["enum"]}})
        dbt["models"][0]["columns"].append({
            "name": name,
            "description": clause.get("description", ""),
            "tests": tests,
        })
    dump_yaml(output_dir / f"{stem}_dbt.yml", dbt)


def snapshot_contract(contract_id: str, contract_path: Path) -> None:
    snapshot_dir = Path("schema_snapshots") / contract_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")
    shutil.copy(contract_path, snapshot_dir / f"{timestamp}.yaml")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--lineage")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    records = load_jsonl(args.source)
    df = flatten_records(records)
    column_profiles = {col: profile_column(df[col], col) for col in df.columns}
    contract = build_contract(args.contract_id, args.source, column_profiles)
    contract = inject_lineage(contract, args.lineage)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = "week3_extractions" if "week3" in args.contract_id else "week5_events"
    contract_path = output_dir / f"{stem}.yaml"
    dump_yaml(contract_path, contract)
    write_dbt_schema(output_dir, stem, contract)
    snapshot_contract(args.contract_id, contract_path)

    print(f"Wrote contract: {contract_path}")


if __name__ == "__main__":
    main()
