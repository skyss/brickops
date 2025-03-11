import json
from pathlib import Path
from typing import Any

import yaml


def read_config_yaml(cfgfile: str | Path) -> dict[str, Any]:
    with Path(cfgfile).open() as stream:
        return yaml.safe_load(stream)  # type: ignore [no-any-return]


def read_config_json(cfgfile: str | Path) -> dict[str, Any]:
    with Path(cfgfile).open() as file:
        return json.load(file)  # type: ignore [no-any-return]
