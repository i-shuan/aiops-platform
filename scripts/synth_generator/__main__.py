"""python -m scripts.synth_generator --scenario scripts/synth_generator/scenarios/nov03_cascade.yaml --out synth-out/nov03_cascade

只產生本機檔案（bulk NDJSON + ground_truth.json + manifest.json），不連網路。載入 ES 用 es_writer.py（使用者自己跑）。
"""
from __future__ import annotations

import argparse
import json
import sys

from .faults import load_scenario
from .generate import generate


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m scripts.synth_generator", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", required=True, help="劇本 YAML")
    ap.add_argument("--out", required=True, help="輸出資料夾（建議放 synth-out/ 底下，已被 .gitignore 擋住）")
    args = ap.parse_args(argv)
    manifest = generate(load_scenario(args.scenario), args.out)
    getattr(sys.stdout, "reconfigure", lambda **_: None)(encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
