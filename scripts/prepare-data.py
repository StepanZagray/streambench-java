#!/usr/bin/env python3
"""Regenerate a small deterministic fixture; run with uv run --with pyarrow."""
import hashlib
import json
from pathlib import Path
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/source/wikitext-2-raw-v1/test-00000-of-00001.parquet"
REVISION = "f776294184f13b8ff2337b3841cf9269a6216d1e"
if not SOURCE.is_file() or SOURCE.stat().st_size > 2_000_000:
    raise SystemExit("Expected the small WikiText-2 test parquet (less than 2 MB). See data/README.md.")
rows = pq.read_table(SOURCE, columns=["text"]).column("text").to_pylist()
# Preserve dataset row order, Unicode and whitespace. A newline separates rows.
text = "\n".join(row for row in rows if row is not None)
fragments = [text[i:i + 16] for i in range(0, min(len(text), 16 * 8192), 16)]
fixture = "".join(json.dumps({"seq": i, "text": fragment}, ensure_ascii=False,
                             separators=(",", ":")) + "\n"
                  for i, fragment in enumerate(fragments)).encode("utf-8")
(ROOT / "data/events.jsonl").write_bytes(fixture)
metadata = {
    "dataset": "Salesforce/wikitext", "configuration": "wikitext-2-raw-v1",
    "split": "test", "revision": REVISION,
    "source_url": f"https://huggingface.co/datasets/Salesforce/wikitext/tree/{REVISION}",
    "source_file": str(SOURCE.relative_to(ROOT)),
    "source_bytes": SOURCE.stat().st_size,
    "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
    "source_rows": len(rows), "fragment_codepoints": 16,
    "fixture_events": len(fragments), "fixture_bytes": len(fixture),
    "fixture_sha256": hashlib.sha256(fixture).hexdigest(),
    "license": "CC-BY-SA-3.0 (dataset card also lists GFDL)",
    "transformation": "Join non-null rows with newline; take first 131072 Unicode codepoints; split every 16 codepoints; compact JSONL, zero-based seq. Not model tokens.",
}
(ROOT / "data/manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
print(json.dumps(metadata, indent=2))
