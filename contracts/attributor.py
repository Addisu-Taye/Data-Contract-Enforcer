from __future__ import annotations

import argparse
import json
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from contracts.common import append_jsonl, bfs_upstream, load_jsonl, load_yaml, utc_now_iso


def find_failed_results(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [r for r in report.get("results", []) if r.get("status") == "FAIL"]


def get_recent_commits(file_path: str, repo_root: str = ".", days: int = 14) -> list[dict[str, Any]]:
    cmd = [
        "git", "log", "--follow",
        f"--since={days} days ago",
        "--format=%H|%an|%ae|%ai|%s",
        "--", file_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=repo_root)
    commits = []
    for line in proc.stdout.splitlines():
        if "|" not in line:
            continue
        commit_hash, author, email, timestamp, message = line.split("|", 4)
        commits.append({
            "commit_hash": commit_hash,
            "author": f"{author} <{email}>",
            "commit_timestamp": timestamp,
            "commit_message": message,
        })
    return commits


def score_candidates(commits: list[dict[str, Any]], lineage_distance: int = 0) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    scored = []
    for rank, commit in enumerate(commits[:5], start=1):
        commit_time = datetime.fromisoformat(commit["commit_timestamp"].replace(" +0000", "+00:00"))
        days_since = abs((now - commit_time).days)
        score = max(0.0, 1.0 - (days_since * 0.1) - (lineage_distance * 0.2))
        scored.append({**commit, "rank": rank, "confidence_score": round(score, 3)})
    return scored or [{
        "rank": 1,
        "file_path": "unknown",
        "commit_hash": "unknown",
        "author": "unknown",
        "commit_timestamp": utc_now_iso(),
        "commit_message": "No git history found",
        "confidence_score": 0.1,
    }]


def map_column_to_node(column_name: str, lineage: dict[str, Any]) -> str | None:
    nodes = lineage.get("nodes", [])
    if not nodes:
        return None
    needle = column_name.split(".")[0]
    for node in nodes:
        node_id = node.get("node_id", "")
        if needle in node_id or "week3" in node_id:
            return node_id
    return nodes[0].get("node_id")


def build_blast_radius(contract: dict[str, Any], records_failing: int) -> dict[str, Any]:
    downstream = contract.get("lineage", {}).get("downstream", [])
    return {
        "affected_nodes": [d.get("id") for d in downstream],
        "affected_pipelines": [d.get("id") for d in downstream if "pipeline" in str(d.get("id", "")).lower()],
        "estimated_records": records_failing,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--violation", required=True, help="Validation report JSON")
    parser.add_argument("--lineage", required=True)
    parser.add_argument("--contract", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()

    with open(args.violation, "r", encoding="utf-8") as f:
        report = json.load(f)
    lineage_records = load_jsonl(args.lineage)
    lineage = lineage_records[-1] if lineage_records else {"nodes": [], "edges": []}
    contract = load_yaml(args.contract)

    rows = []
    for failed in find_failed_results(report):
        node_id = map_column_to_node(failed["column_name"], lineage)
        path_candidates = []
        if node_id:
            traversal = bfs_upstream(node_id, lineage.get("edges", []))
            node_map = {n["node_id"]: n for n in lineage.get("nodes", [])}
            for node in traversal:
                meta = node_map.get(node, {}).get("metadata", {})
                if meta.get("path"):
                    path_candidates.append(meta["path"])
        if not path_candidates:
            path_candidates = ["src/week3/extractor.py"]

        blame_chain = []
        for candidate in path_candidates[:2]:
            commits = get_recent_commits(candidate, repo_root=args.repo_root)
            for item in score_candidates(commits or [], 0):
                item["file_path"] = candidate
                blame_chain.append(item)
        blame_chain = sorted(blame_chain, key=lambda x: x["confidence_score"], reverse=True)[:5]
        if not blame_chain:
            blame_chain = score_candidates([])

        rows.append({
            "violation_id": str(uuid.uuid4()),
            "check_id": failed["check_id"],
            "detected_at": utc_now_iso(),
            "blame_chain": blame_chain,
            "blast_radius": build_blast_radius(contract, failed.get("records_failing", 0)),
        })

    append_jsonl(args.output, rows)
    print(f"Wrote {len(rows)} attributed violations to {args.output}")


if __name__ == "__main__":
    main()
