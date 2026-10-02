"""Pipeline settings loaded from YAML with environment overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

from .exceptions import ConfigError

# Relative paths resolve against the project directory (CLAIMS_HOME or the CWD).
REPO_ROOT = Path(os.environ.get("CLAIMS_HOME", Path.cwd())).resolve()
DEFAULT_CONFIG = REPO_ROOT / "config" / "config.example.yaml"
DEFAULT_RULES = REPO_ROOT / "config" / "dq_rules.yaml"


@dataclass(frozen=True)
class Settings:
    """Resolved filesystem layout and runtime options for one pipeline run."""

    raw_root: Path
    lakehouse_root: Path
    rules_path: Path
    spark_master: str = "local[2]"
    shuffle_partitions: int = 4
    seed: int = 42

    @property
    def bronze(self) -> Path:
        return self.lakehouse_root / "bronze"

    @property
    def silver(self) -> Path:
        return self.lakehouse_root / "silver"

    @property
    def gold(self) -> Path:
        return self.lakehouse_root / "gold"

    @property
    def rejects(self) -> Path:
        return self.lakehouse_root / "rejects"

    @property
    def reports(self) -> Path:
        return self.lakehouse_root / "_reports"

    @property
    def state_file(self) -> Path:
        return self.lakehouse_root / "_state" / "pipeline_state.json"


def load_settings(config_path: Path | None = None, **overrides: object) -> Settings:
    """Load settings from ``config/config.yaml`` (falls back to the example file).

    ``CLAIMS_LAKEHOUSE_ROOT`` and ``CLAIMS_RAW_ROOT`` environment variables, then
    keyword ``overrides``, take precedence over the file.
    """
    path = config_path or (REPO_ROOT / "config" / "config.yaml")
    if not path.exists():
        path = DEFAULT_CONFIG
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc

    def _resolve(value: str | os.PathLike) -> Path:
        p = Path(value)
        return p if p.is_absolute() else REPO_ROOT / p

    values = {
        "raw_root": os.environ.get("CLAIMS_RAW_ROOT", raw.get("raw_root", "data/raw")),
        "lakehouse_root": os.environ.get(
            "CLAIMS_LAKEHOUSE_ROOT", raw.get("lakehouse_root", "data/lakehouse")
        ),
        "rules_path": raw.get("rules_path", str(DEFAULT_RULES)),
        "spark_master": raw.get("spark", {}).get("master", "local[2]"),
        "shuffle_partitions": int(raw.get("spark", {}).get("shuffle_partitions", 4)),
        "seed": int(raw.get("seed", 42)),
    }
    values.update({k: v for k, v in overrides.items() if v is not None})
    for key in ("raw_root", "lakehouse_root", "rules_path"):
        values[key] = _resolve(values[key])  # type: ignore[arg-type]
    return Settings(**values)  # type: ignore[arg-type]
