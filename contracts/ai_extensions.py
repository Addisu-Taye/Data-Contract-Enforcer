from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


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
        for line in f:
            if not line.strip():
                continue
            records.append(json.loads(line))

    return records


# --------------------------------------------------
# 1. Embedding / confidence drift (proxy)
# --------------------------------------------------

def check_confidence_drift(df: pd.DataFrame) -> dict[str, Any]:
    if "extracted_facts[].confidence" not in df.columns:
        return {
            "check": "confidence_drift",
            "status": "SKIP",
            "message": "Column not found",
        }

    series = pd.to_numeric(df["extracted_facts[].confidence"], errors="coerce").dropna()

    if len(series) == 0:
        return {
            "check": "confidence_drift",
            "status": "SKIP",
            "message": "No numeric data",
        }

    mean = float(series.mean())

    if mean > 1.0:
        return {
            "check": "confidence_drift",
            "status": "FAIL",
            "severity": "CRITICAL",
            "mean": round(mean, 4),
            "message": "Confidence scale likely shifted (0–100 instead of 0–1)",
        }

    if mean < 0.2:
        return {
            "check": "confidence_drift",
            "status": "WARN",
            "severity": "MEDIUM",
            "mean": round(mean, 4),
            "message": "Confidence unusually low",
        }

    return {
        "check": "confidence_drift",
        "status": "PASS",
        "severity": "LOW",
        "mean": round(mean, 4),
        "message": "Confidence distribution normal",
    }


# --------------------------------------------------
# 2. Prompt / input structure validation
# --------------------------------------------------

def check_prompt_structure(df: pd.DataFrame) -> dict[str, Any]:
    required_fields = [
        "doc_id",
        "extraction_model",
        "source_path",
    ]

    missing = [col for col in required_fields if col not in df.columns]

    if missing:
        return {
            "check": "prompt_structure",
            "status": "FAIL",
            "severity": "HIGH",
            "missing_fields": missing,
            "message": "Missing required input fields for AI processing",
        }

    return {
        "check": "prompt_structure",
        "status": "PASS",
        "severity": "LOW",
        "message": "Input structure valid",
    }


# --------------------------------------------------
# 3. Output schema validation
# --------------------------------------------------

def check_output_schema(df: pd.DataFrame) -> dict[str, Any]:
    required_outputs = [
        "extracted_facts.__len__",
        "extracted_facts[].confidence",
    ]

    missing = [col for col in required_outputs if col not in df.columns]

    if missing:
        return {
            "check": "output_schema",
            "status": "FAIL",
            "severity": "CRITICAL",
            "missing_fields": missing,
            "message": "AI output structure invalid",
        }

    return {
        "check": "output_schema",
        "status": "PASS",
        "severity": "LOW",
        "message": "AI output structure valid",
    }


# --------------------------------------------------
# Flattening (reuse logic)
# --------------------------------------------------

def make_hashable(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return value


def flatten_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []

    for record in records:
        row = {}

        for key, value in record.items():
            if isinstance(value, dict):
                for k, v in value.items():
                    row[f"{key}.{k}"] = make_hashable(v)

            elif isinstance(value, list):
                row[f"{key}.__len__"] = len(value)

                if value and all(isinstance(i, dict) for i in value):
                    keys = set()
                    for item in value:
                        keys.update(item.keys())

                    for sub_k in keys:
                        vals = [i.get(sub_k) for i in value if isinstance(i, dict)]
                        vals = [v for v in vals if v is not None]

                        if not vals:
                            continue

                        col = f"{key}[].{sub_k}"

                        if all(isinstance(v, (int, float)) for v in vals):
                            row[col] = sum(vals) / len(vals)
                        else:
                            row[col] = str(vals[0])

            else:
                row[key] = value

        rows.append(row)

    return pd.DataFrame(rows)


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="AI-specific contract validation")
    parser.add_argument("--extractions", help="Path to extraction JSONL")
    parser.add_argument("--output", required=True, help="Output report JSON")
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.extractions:
        raise ValueError("Provide --extractions path")

    records = load_jsonl(args.extractions)
    df = flatten_records(records)

    results = [
        check_confidence_drift(df),
        check_prompt_structure(df),
        check_output_schema(df),
    ]

    report = {
        "report_id": str(uuid.uuid4()),
        "generated_at": utc_now_iso(),
        "checks": results,
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)

    with open(args.output, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Wrote AI validation report: {args.output}")


if __name__ == "__main__":
    main()