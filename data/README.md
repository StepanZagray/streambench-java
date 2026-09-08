# Text fixture and attribution

Source: [Salesforce/WikiText](https://huggingface.co/datasets/Salesforce/wikitext),
WikiText-2 **raw test split only**, pinned to
`f776294184f13b8ff2337b3841cf9269a6216d1e`.
The parquet download is 731,216 bytes (about 714 KiB); the dataset card is about 10 KiB.
No training split, WikiText-103, tokenizer, or model is downloaded.

WikiText was assembled by Stephen Merity and collaborators from Wikipedia articles
written by Wikipedia contributors. See the original dataset card in
[source/README.md](source/README.md), including the citation and source information.
The card lists CC BY-SA 3.0 and GFDL. This derived text fixture is distributed under
[CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/).
Article headings are preserved; Wikipedia article histories identify contributors.
The data license applies to the text, independently of the benchmark source code.

`manifest.json` records exact source/fixture sizes, hashes, revision and transformation.
`events.jsonl` contains the first 8,192 fragments of 16 Unicode codepoints each,
including original whitespace and article headings. These are **fragments, not LLM
tokens**. The prose is English-centric and is not a multilingual workload.
The full small source split is retained for reproducibility; servers load only the
derived fixture. Frames are pre-encoded at startup, excluding JSON serialization
from the timed request path.

## Reproduce

From the repository root (requires the installed `hf` CLI and `uv`):

```sh
hf download Salesforce/wikitext \
  wikitext-2-raw-v1/test-00000-of-00001.parquet README.md \
  --repo-type dataset --revision f776294184f13b8ff2337b3841cf9269a6216d1e \
  --local-dir data/source --dry-run
# The dry-run should report approximately 742 KB total.
hf download Salesforce/wikitext \
  wikitext-2-raw-v1/test-00000-of-00001.parquet README.md \
  --repo-type dataset --revision f776294184f13b8ff2337b3841cf9269a6216d1e \
  --local-dir data/source
uv run --with pyarrow==23.0.1 python scripts/prepare-data.py
```

The fixture is checked in, so running or deploying the apps needs no Hugging Face
account, token, Python packages, or download.
