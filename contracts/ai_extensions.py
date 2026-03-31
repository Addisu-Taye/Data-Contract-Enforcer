from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
from jsonschema import ValidationError, validate
from sklearn.feature_extraction.text import TfidfVectorizer

from contracts.common import load_jsonl, utc_now_iso, write_json

EMBEDDING_BASELINE = Path("schema_snapshots/embedding_baselines.npz")
PROMPT_INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 36, "maxLength": 36},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def text_centroid(texts: list[str]) -> np.ndarray:
    if not texts:
        return np.zeros(8)
    vec = TfidfVectorizer(max_features=256)
    matrix = vec.fit_transform(texts)
    return np.asarray(matrix.mean(axis=0)).ravel()


def check_embedding_drift(texts: list[str], threshold: float = 0.15) -> dict[str, Any]:
    centroid = text_centroid(texts[:200])
    EMBEDDING_BASELINE.parent.mkdir(parents=True, exist_ok=True)
    if not EMBEDDING_BASELINE.exists():
        np.savez(EMBEDDING_BASELINE, centroid=centroid)
        return {"status": "BASELINE_SET", "drift_score": 0.0, "threshold": threshold}
    baseline = np.load(EMBEDDING_BASELINE)["centroid"]
    denom = (np.linalg.norm(centroid) * np.linalg.norm(baseline)) + 1e-9
    cosine_sim = float(np.dot(centroid, baseline) / denom) if denom else 1.0
    drift = 1 - cosine_sim
    return {
        "status": "FAIL" if drift > threshold else "PASS",
        "drift_score": round(float(drift), 4),
        "threshold": threshold,
    }


def validate_prompt_inputs(records: list[dict[str, Any]]) -> dict[str, Any]:
    quarantined = []
    valid_count = 0
    qpath = Path("outputs/quarantine/quarantine.jsonl")
    qpath.parent.mkdir(parents=True, exist_ok=True)
    for record in records:
        prompt_input = {
            "doc_id": record.get("doc_id", ""),
            "source_path": record.get("source_path", ""),
            "content_preview": " ".join(f.get("text", "") for f in record.get("extracted_facts", [])[:3]),
        }
        try:
            validate(instance=prompt_input, schema=PROMPT_INPUT_SCHEMA)
            valid_count += 1
        except ValidationError as exc:
            quarantined.append({"record": prompt_input, "error": exc.message})
    if quarantined:
        with open(qpath, "a", encoding="utf-8") as f:
            for row in quarantined:
                f.write(json.dumps(row) + "\n")
    return {
        "valid_records": valid_count,
        "quarantined_records": len(quarantined),
        "status": "FAIL" if quarantined else "PASS",
    }


def check_output_schema_violation_rate(verdict_records: list[dict[str, Any]], baseline_rate: float | None = None, warn_threshold: float = 0.02) -> dict[str, Any]:
    total = len(verdict_records)
    violations = sum(1 for v in verdict_records if v.get("overall_verdict") not in {"PASS", "FAIL", "WARN"})
    rate = violations / max(total, 1)
    trend = "unknown"
    if baseline_rate is not None:
        trend = "rising" if rate > baseline_rate * 1.5 else "stable"
    return {
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "trend": trend,
        "status": "WARN" if rate > warn_threshold else "PASS",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default="all", choices=["all", "embedding", "prompt", "output"])
    parser.add_argument("--extractions")
    parser.add_argument("--verdicts")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    payload: dict[str, Any] = {"generated_at": utc_now_iso(), "mode": args.mode}

    if args.mode in {"all", "embedding", "prompt"} and args.extractions:
        extraction_records = load_jsonl(args.extractions)
        texts = [fact.get("text", "") for record in extraction_records for fact in record.get("extracted_facts", []) if fact.get("text")]
        payload["embedding_drift"] = check_embedding_drift(texts)
        payload["prompt_input_validation"] = validate_prompt_inputs(extraction_records)

    if args.mode in {"all", "output"} and args.verdicts:
        verdict_records = load_jsonl(args.verdicts)
        payload["llm_output_schema"] = check_output_schema_violation_rate(verdict_records)

    write_json(args.output, payload)
    print(f"Wrote AI extensions report: {args.output}")


if __name__ == "__main__":
    main()
