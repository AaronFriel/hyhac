#!/usr/bin/env python3

import json
import re
from pathlib import Path


config_path = Path(".pre-commit-config.yaml")
hook_id_re = re.compile(r"^\s*-\s+id:\s+(.+?)\s*$")
stages_re = re.compile(r"^\s*stages:\s*\[(.*?)\]\s*$")


def parse_hooks() -> list[dict[str, object]]:
    hooks: list[dict[str, object]] = []
    current: dict[str, object] | None = None

    for raw_line in config_path.read_text().splitlines():
        hook_match = hook_id_re.match(raw_line)
        if hook_match:
            if current is not None:
                hooks.append(current)
            current = {"id": hook_match.group(1), "stages": None}
            continue

        if current is None:
            continue

        stages_match = stages_re.match(raw_line)
        if stages_match:
            stages = [
                stage.strip().strip("'\"")
                for stage in stages_match.group(1).split(",")
                if stage.strip()
            ]
            current["stages"] = stages

    if current is not None:
        hooks.append(current)

    return hooks


def main() -> None:
    hooks = []
    for hook in parse_hooks():
        stages = hook["stages"]
        if stages is not None and "pre-commit" not in stages:
            continue
        hooks.append({"id": hook["id"]})

    print(json.dumps({"include": hooks}))


if __name__ == "__main__":
    main()
