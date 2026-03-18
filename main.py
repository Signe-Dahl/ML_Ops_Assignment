from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    print("Starting weather pipeline...")

    # ── Fetch weather ──────────────────────────────
    fetch_result = subprocess.run(
        [sys.executable, "fetch.py"],
        capture_output=True,
        text=True,
        check=True,
    )
    print(fetch_result.stdout)

    # ── Generate poem ──────────────────────────────
    poem_result = subprocess.run(
        [sys.executable, "poem.py"],
        capture_output=True,
        text=True,
        check=True,
    )
    print(poem_result.stdout)

    # ── Summary ────────────────────────────────────
    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database_path": "data/weather.db",
        "poem_path": "data/weather_poem.txt",
        "status": "success",
    }

    output_dir = Path("outputs")
    output_dir.mkdir(exist_ok=True)

    summary_path = output_dir / "run_summary.json"

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4)

    print("Run summary written to:", summary_path)
    print(summary)


if __name__ == "__main__":
    main()