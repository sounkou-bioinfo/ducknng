#!/usr/bin/env python3
"""Render generated function catalog artifacts from functions.yaml.

The manifest is stored in JSON-formatted YAML so this script can use only the
Python standard library, following the duckhts pattern.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path


def die(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def escape_md(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def load_manifest(path: Path) -> list[dict[str, object]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        die(f"Failed to parse {path}: {exc}")

    functions = payload.get("functions")
    if not isinstance(functions, list):
        die(f"{path} is missing a top-level 'functions' array")

    required = {
        "name",
        "kind",
        "category",
        "signature",
        "returns",
        "since",
        "implemented",
        "description",
    }
    seen: set[str] = set()
    for index, entry in enumerate(functions):
        if not isinstance(entry, dict):
            die(f"functions[{index}] must be an object")
        missing = sorted(required - set(entry))
        if missing:
            die(f"functions[{index}] is missing required fields: {', '.join(missing)}")
        name = entry["name"]
        if not isinstance(name, str) or not name:
            die(f"functions[{index}].name must be a non-empty string")
        if name in seen:
            die(f"Duplicate function entry for {name}")
        seen.add(name)
    return functions


def render_markdown(functions: list[dict[str, object]]) -> str:
    lines: list[str] = []
    lines.append("# Function Catalog")
    lines.append("")
    lines.append("This file is generated from `function_catalog/functions.yaml`.")
    lines.append("")
    lines.append("| name | kind | returns | implemented | description |")
    lines.append("|---|---|---|---|---|")
    for entry in functions:
        lines.append(
            "| `{name}` | {kind} | `{returns}` | {implemented} | {description} |".format(
                name=escape_md(str(entry["name"])),
                kind=escape_md(str(entry["kind"])),
                returns=escape_md(str(entry["returns"])),
                implemented="yes" if entry["implemented"] else "no",
                description=escape_md(str(entry["description"])),
            )
        )
    lines.append("")
    return "\n".join(lines)


def write_tsv(path: Path, functions: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(
            ["name", "kind", "category", "signature", "returns", "since", "implemented", "description"]
        )
        for entry in functions:
            writer.writerow(
                [
                    entry["name"],
                    entry["kind"],
                    entry["category"],
                    entry["signature"],
                    entry["returns"],
                    entry["since"],
                    entry["implemented"],
                    entry["description"],
                ]
            )


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else repo_root / "function_catalog" / "functions.yaml"
    outdir = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else repo_root / "function_catalog"
    outdir.mkdir(parents=True, exist_ok=True)

    functions = load_manifest(manifest_path)
    (outdir / "functions.md").write_text(render_markdown(functions), encoding="utf-8")
    write_tsv(outdir / "functions.tsv", functions)
    print(f"wrote {outdir / 'functions.md'}")
    print(f"wrote {outdir / 'functions.tsv'}")


if __name__ == "__main__":
    main()
