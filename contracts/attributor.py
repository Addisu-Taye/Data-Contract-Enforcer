from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


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


def load_latest_lineage_snapshot(path: str | Path) -> dict[str, Any]:
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Lineage snapshot file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    if not lines:
        raise ValueError(f"Lineage snapshot file is empty: {path}")

    raw = lines[-1]

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON in lineage snapshot file {path}. "
            f"Last non-empty line was: {raw[:200]}"
        ) from e


def load_contract(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def choose_primary_failure(report: dict[str, Any]) -> dict[str, Any]:
    failures = [r for r in report.get("results", []) if r.get("status") == "FAIL"]
    if not failures:
        raise ValueError("No FAIL results found in validation report.")

    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "WARNING": 4}
    failures.sort(key=lambda r: severity_order.get(r.get("severity", "LOW"), 99))
    return failures[0]


def infer_system_from_check(check_id: str) -> str:
    if "week1" in check_id:
        return "week1"
    if "week2" in check_id:
        return "week2"
    if "week3" in check_id:
        return "week3"
    if "week4" in check_id:
        return "week4"
    if "week5" in check_id:
        return "week5"
    return "unknown"


def find_upstream_candidates(failing_result: dict[str, Any], lineage_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    system = infer_system_from_check(failing_result["check_id"])
    column_name = failing_result["column_name"]

    candidates: list[dict[str, Any]] = []

    for node in lineage_snapshot.get("nodes", []):
        node_id = str(node.get("node_id", "")).lower()
        node_type = node.get("type", "")
        metadata = node.get("metadata", {})

        if node_type == "FILE" and system in node_id:
            candidates.append(
                {
                    "node_id": node.get("node_id"),
                    "file_path": metadata.get("path", ""),
                    "lineage_distance": 1,
                    "column_name": column_name,
                }
            )

    if not candidates:
        for node in lineage_snapshot.get("nodes", []):
            if node.get("type") == "FILE":
                metadata = node.get("metadata", {})
                candidates.append(
                    {
                        "node_id": node.get("node_id"),
                        "file_path": metadata.get("path", ""),
                        "lineage_distance": 2,
                        "column_name": column_name,
                    }
                )

    return candidates[:5]


def run_git_log(repo_root: Path, file_path: str, days: int = 14) -> list[dict[str, Any]]:
    cmd = [
        "git",
        "log",
        "--follow",
        f"--since={days} days ago",
        "--format=%H|%an|%ae|%ai|%s",
        "--",
        file_path,
    ]
    result = subprocess.run(
        cmd,
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    commits: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        if "|" not in line:
            continue
        parts = line.split("|", 4)
        if len(parts) != 5:
            continue
        commit_hash, author_name, author_email, commit_ts, commit_msg = parts
        commits.append(
            {
                "commit_hash": commit_hash.strip(),
                "author_name": author_name.strip(),
                "author": author_email.strip(),
                "commit_timestamp": commit_ts.strip(),
                "commit_message": commit_msg.strip(),
            }
        )
    return commits


def score_commit(commit: dict[str, Any], detected_at: str, lineage_distance: int) -> float:
    try:
        detected_dt = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
        commit_dt = datetime.fromisoformat(commit["commit_timestamp"].replace("Z", "+00:00"))
        days_since = abs((detected_dt - commit_dt).days)
    except Exception:
        days_since = 7

    score = 1.0 - (days_since * 0.1) - (lineage_distance * 0.2)
    return round(max(0.0, score), 3)


def build_blame_chain(
    candidates: list[dict[str, Any]],
    detected_at: str,
    repo_root: Path,
) -> list[dict[str, Any]]:
    blame_chain: list[dict[str, Any]] = []

    for candidate in candidates:
        file_path = candidate["file_path"]
        if not file_path:
            continue

        commits = run_git_log(repo_root, file_path)
        if not commits:
            blame_chain.append(
                {
                    "rank": len(blame_chain) + 1,
                    "file_path": file_path,
                    "commit_hash": "unknown",
                    "author": "unknown",
                    "commit_timestamp": detected_at,
                    "commit_message": "No recent git history found",
                    "confidence_score": 0.2,
                }
            )
            continue

        best = commits[0]
        blame_chain.append(
            {
                "rank": len(blame_chain) + 1,
                "file_path": file_path,
                "commit_hash": best["commit_hash"],
                "author": best["author"],
                "commit_timestamp": best["commit_timestamp"],
                "commit_message": best["commit_message"],
                "confidence_score": score_commit(
                    best,
                    detected_at=detected_at,
                    lineage_distance=candidate["lineage_distance"],
                ),
            }
        )

    blame_chain.sort(key=lambda x: x["confidence_score"], reverse=True)

    for i, item in enumerate(blame_chain, start=1):
        item["rank"] = i

    return blame_chain[:5]


def compute_blast_radius(contract: dict[str, Any], failing_result: dict[str, Any]) -> dict[str, Any]:
    downstream = contract.get("lineage", {}).get("downstream", [])

    affected_nodes = [d.get("id") for d in downstream if d.get("id")]
    affected_pipelines = [
        d.get("id") for d in downstream
        if d.get("id") and "pipeline" in str(d.get("id")).lower()
    ]

    return {
        "affected_nodes": affected_nodes,
        "affected_pipelines": affected_pipelines,
        "estimated_records": failing_result.get("records_failing"),
    }


def append_jsonl(path: str | Path, record: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Attribute contract violations to likely upstream commits.")
    parser.add_argument("--violation", required=True, help="Validation report JSON path")
    parser.add_argument("--lineage", required=True, help="Lineage snapshot JSONL path")
    parser.add_argument("--contract", required=True, help="Contract YAML path")
    parser.add_argument("--output", required=True, help="Violation log JSONL output path")
    parser.add_argument("--repo-root", default=".", help="Git repository root")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    report = load_json(args.violation)
    lineage_snapshot = load_latest_lineage_snapshot(args.lineage)
    contract = load_contract(args.contract)
    repo_root = Path(args.repo_root).resolve()

    failing_result = choose_primary_failure(report)
    detected_at = report.get("run_timestamp", utc_now_iso())

    candidates = find_upstream_candidates(failing_result, lineage_snapshot)
    blame_chain = build_blame_chain(candidates, detected_at, repo_root)
    blast_radius = compute_blast_radius(contract, failing_result)

    violation_record = {
        "violation_id": str(uuid.uuid4()),
        "check_id": failing_result["check_id"],
        "detected_at": detected_at,
        "blame_chain": blame_chain if blame_chain else [
            {
                "rank": 1,
                "file_path": "unknown",
                "commit_hash": "unknown",
                "author": "unknown",
                "commit_timestamp": detected_at,
                "commit_message": "No attribution candidates found",
                "confidence_score": 0.1,
            }
        ],
        "blast_radius": blast_radius,
    }

    append_jsonl(args.output, violation_record)
    print(f"Wrote violation log entry: {args.output}")


if __name__ == "__main__":
    main()