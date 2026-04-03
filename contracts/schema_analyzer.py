from __future__ import annotations

import argparse
import json
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


def load_yaml(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def classify_change(field_name: str, old_clause: dict[str, Any] | None, new_clause: dict[str, Any] | None) -> tuple[str, str]:
    if old_clause is None:
        if new_clause and new_clause.get("required", False):
            return "BREAKING", "Add non-nullable column — coordinate all producers before deployment"
        return "COMPATIBLE", "Add nullable column — downstream consumers can ignore it"

    if new_clause is None:
        return "BREAKING", "Remove column — deprecation period required"

    if old_clause.get("type") != new_clause.get("type"):
        return "BREAKING", f"Type change {old_clause.get('type')} -> {new_clause.get('type')}"

    if old_clause.get("minimum") != new_clause.get("minimum"):
        return "BREAKING", f"Minimum changed {old_clause.get('minimum')} -> {new_clause.get('minimum')}"

    if old_clause.get("maximum") != new_clause.get("maximum"):
        return "BREAKING", f"Maximum changed {old_clause.get('maximum')} -> {new_clause.get('maximum')}"

    old_enum = set(old_clause.get("enum", []))
    new_enum = set(new_clause.get("enum", []))
    if old_enum != new_enum:
        removed = sorted(old_enum - new_enum)
        added = sorted(new_enum - old_enum)
        if removed:
            return "BREAKING", f"Enum values removed: {removed}"
        return "COMPATIBLE", f"Enum values added: {added}"

    if old_clause.get("format") != new_clause.get("format"):
        return "BREAKING", f"Format changed {old_clause.get('format')} -> {new_clause.get('format')}"

    if old_clause.get("pattern") != new_clause.get("pattern"):
        return "COMPATIBLE", "Pattern changed — review for downstream compatibility"

    if old_clause.get("required") != new_clause.get("required"):
        if new_clause.get("required"):
            return "BREAKING", "Field became required"
        return "COMPATIBLE", "Field became optional"

    return "COMPATIBLE", "No material breaking change"


def diff_schema(old_schema: dict[str, Any], new_schema: dict[str, Any]) -> list[dict[str, Any]]:
    all_fields = sorted(set(old_schema.keys()) | set(new_schema.keys()))
    changes: list[dict[str, Any]] = []

    for field_name in all_fields:
        old_clause = old_schema.get(field_name)
        new_clause = new_schema.get(field_name)

        if old_clause == new_clause:
            continue

        verdict, rationale = classify_change(field_name, old_clause, new_clause)
        changes.append(
            {
                "field_name": field_name,
                "old_clause": old_clause,
                "new_clause": new_clause,
                "compatibility": verdict,
                "rationale": rationale,
            }
        )

    return changes


def build_migration_checklist(changes: list[dict[str, Any]]) -> list[str]:
    checklist: list[str] = []

    for change in changes:
        field = change["field_name"]
        compatibility = change["compatibility"]
        rationale = change["rationale"]

        if compatibility == "BREAKING":
            checklist.append(f"Review all consumers of '{field}' because change is BREAKING: {rationale}")
            checklist.append(f"Prepare migration or alias plan for '{field}'")
            checklist.append(f"Block deployment until '{field}' downstream compatibility is confirmed")
        else:
            checklist.append(f"Document compatible change for '{field}': {rationale}")

    if not checklist:
        checklist.append("No schema changes detected")

    return checklist


def build_rollback_plan(changes: list[dict[str, Any]]) -> list[str]:
    if not changes:
        return ["No rollback needed"]

    rollback: list[str] = [
        "Restore previous schema snapshot",
        "Re-run ValidationRunner on the restored data shape",
        "Notify downstream consumers of rollback status",
    ]

    if any(change["compatibility"] == "BREAKING" for change in changes):
        rollback.append("Revert breaking schema-producing code change before redeploying")

    return rollback


def find_snapshots(contract_id: str) -> list[Path]:
    snapshot_dir = Path("schema_snapshots") / contract_id
    if not snapshot_dir.exists():
        return []
    return sorted(snapshot_dir.glob("*.yaml"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze schema evolution across snapshots.")
    parser.add_argument("--contract-id", required=True, help="Contract id whose snapshots should be diffed")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    snapshots = find_snapshots(args.contract_id)
    if len(snapshots) < 2:
        raise ValueError(
            f"Need at least 2 schema snapshots for contract '{args.contract_id}', found {len(snapshots)}"
        )

    old_path = snapshots[-2]
    new_path = snapshots[-1]

    old_contract = load_yaml(old_path)
    new_contract = load_yaml(new_path)

    old_schema = old_contract.get("schema", {})
    new_schema = new_contract.get("schema", {})

    changes = diff_schema(old_schema, new_schema)

    compatibility_verdict = "COMPATIBLE"
    if any(change["compatibility"] == "BREAKING" for change in changes):
        compatibility_verdict = "BREAKING"

    report = {
        "generated_at": utc_now_iso(),
        "contract_id": args.contract_id,
        "old_snapshot": str(old_path),
        "new_snapshot": str(new_path),
        "compatibility_verdict": compatibility_verdict,
        "change_count": len(changes),
        "changes": changes,
        "migration_checklist": build_migration_checklist(changes),
        "rollback_plan": build_rollback_plan(changes),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Wrote schema evolution report: {output_path}")


if __name__ == "__main__":
    main()