from __future__ import annotations

import hashlib
import json
import re
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import yaml

UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")
ISO_REPLACEMENT = "+00:00"


def ensure_parent(path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    output = ensure_parent(path)
    with open(output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def append_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
    output = ensure_parent(path)
    with open(output, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def dump_yaml(path: str | Path, payload: dict[str, Any]) -> None:
    output = ensure_parent(path)
    with open(output, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, sort_keys=False, allow_unicode=True)


def flatten_records(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in records:
        base = {k: v for k, v in record.items() if not isinstance(v, (list, dict))}
        extracted_facts = record.get("extracted_facts") or [None]
        entities = record.get("entities") or [None]

        if extracted_facts != [None]:
            for fact in extracted_facts:
                row = dict(base)
                for key, value in fact.items():
                    row[f"extracted_facts.{key}"] = value
                rows.append(row)
            continue

        if entities != [None]:
            for entity in entities:
                row = dict(base)
                for key, value in entity.items():
                    row[f"entities.{key}"] = value
                rows.append(row)
            continue

        rows.append(base)
    return pd.DataFrame(rows)


def parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + ISO_REPLACEMENT
    return datetime.fromisoformat(text)


def bfs_upstream(start_node: str, edges: list[dict[str, Any]]) -> list[str]:
    reverse = {}
    for edge in edges:
        reverse.setdefault(edge["target"], []).append(edge["source"])

    visited = set()
    order = []
    queue = deque([start_node])
    while queue:
        node = queue.popleft()
        if node in visited:
            continue
        visited.add(node)
        order.append(node)
        for parent in reverse.get(node, []):
            if parent not in visited:
                queue.append(parent)
    return order
