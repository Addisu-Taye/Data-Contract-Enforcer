import json
from pathlib import Path

source = Path("outputs/week3/extractions.jsonl")
target = Path("outputs/week3/extractions_violated.jsonl")

with source.open("r", encoding="utf-8") as f_in, target.open("w", encoding="utf-8") as f_out:
    for line in f_in:
        if not line.strip():
            continue
        record = json.loads(line)

        for fact in record.get("extracted_facts", []):
            confidence = fact.get("confidence")
            if isinstance(confidence, (int, float)):
                fact["confidence"] = round(confidence * 100, 1)

        f_out.write(json.dumps(record) + "\n")

print(f"Wrote violated dataset: {target}")