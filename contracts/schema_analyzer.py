from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from contracts.common import load_yaml, utc_now_iso, write_json


def classify_change(field_name: str, old_clause: dict[str, Any] | None, new_clause: dict[str, Any] | None) -> tuple[str, str]:
    if old_clause is None:
        if new_clause and new_clause.get("required"):
            return "BREAKING", f"{field_name}: added non-nullable field"
        return "COMPATIBLE", f"{field_name}: added nullable field"
    if new_clause is None:
        return "BREAKING", f"{field_name}: removed field"
    if old_clause.get("type") != new_clause.get("type"):
        return "BREAKING", f"{field_name}: type changed {old_clause.get('type')} -> {new_clause.get('type')}"
    if old_clause.get("maximum") != new_clause.get("maximum"):
        return "BREAKING", f"{field_name}: maximum changed {old_clause.get('maximum')} -> {new_clause.get('maximum')}"
    old_enum = set(old_clause.get("enum", []))
    new_enum = set(new_clause.get("enum", []))
    if old_enum != new_enum:
        removed = old_enum - new_enum
        if removed:
            return "BREAKING", f"{field_name}: enum values removed {sorted(removed)}"
        return "COMPATIBLE", f"{field_name}: enum values added {sorted(new_enum - old_enum)}"
    return "COMPATIBLE", f"{field_name}: no material breaking change"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    snapshot_dir = Path("schema_snapshots") / args.contract_id
    snapshots = sorted(snapshot_dir.glob("*.yaml"))
    if len(snapshots) < 2:
        raise SystemExit(f"Need at least 2 snapshots in {snapshot_dir}")

    old = load_yaml(snapshots[-2])
    new = load_yaml(snapshots[-1])
    old_schema = old.get("schema", {})
    new_schema = new.get("schema", {})
    fields = sorted(set(old_schema) | set(new_schema))

    changes = []
    for field in fields:
        verdict, rationale = classify_change(field, old_schema.get(field), new_schema.get(field))
        changes.append({
            "field": field,
            "compatibility_verdict": verdict,
            "detail": rationale,
        })

    payload = {
        "contract_id": args.contract_id,
        "generated_at": utc_now_iso(),
        "old_snapshot": snapshots[-2].name,
        "new_snapshot": snapshots[-1].name,
        "changes": changes,
        "breaking_changes": [c for c in changes if c["compatibility_verdict"] == "BREAKING"],
        "rollback_plan": [
            "Restore previous contract snapshot",
            "Re-run ValidationRunner on rollback dataset",
            "Notify downstream consumers before re-introducing change",
        ],
    }
    write_json(args.output, payload)
    print(f"Wrote schema evolution report: {args.output}")


if __name__ == "__main__":
    main()
