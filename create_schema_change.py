import json
from pathlib import Path

source = Path("outputs/week3/extractions.jsonl")
target = Path("outputs/week3/extractions_schema_changed.jsonl")

with source.open("r", encoding="utf-8") as f_in, target.open("w", encoding="utf-8") as f_out:
    for line in f_in:
        if not line.strip():
            continue
        record = json.loads(line)

        for fact in record.get("extracted_facts", []):
            confidence = fact.get("confidence")
            if confidence is not None:
                fact["confidence"] = str(confidence)

        f_out.write(json.dumps(record) + "\n")

print(f"Wrote schema-changed dataset: {target}")