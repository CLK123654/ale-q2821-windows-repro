import hashlib
import json
from pathlib import Path


root = Path(__file__).resolve().parents[1]
expected = json.loads((root / "qa" / "expected_hashes.json").read_text(encoding="utf-8"))
for name, digest in expected.items():
    actual = hashlib.sha256((root / "task" / name).read_bytes()).hexdigest()
    if actual != digest:
        raise SystemExit(f"附件哈希不一致：{name}")
