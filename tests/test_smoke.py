from pathlib import Path


def test_repo_structure():
    root = Path(__file__).resolve().parents[1]
    for rel in [
        "contracts/generator.py",
        "contracts/runner.py",
        "contracts/attributor.py",
        "contracts/schema_analyzer.py",
        "contracts/ai_extensions.py",
        "contracts/report_generator.py",
    ]:
        assert (root / rel).exists(), rel
