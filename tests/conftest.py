"""Shared fixtures: one SparkSession and one generated raw dataset per test session."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from claims_lakehouse.config import Settings
from claims_lakehouse.generate_data import generate
from claims_lakehouse.spark import build_spark

# Spark session TZ is UTC; make Python's collect() conversions UTC too.
os.environ["TZ"] = "UTC"
time.tzset()

REPO = Path(__file__).resolve().parents[1]
RULES = REPO / "config" / "dq_rules.yaml"


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = build_spark("local[2]", shuffle_partitions=2, app_name="claims-lakehouse-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def raw_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("raw")
    generate(root, seed=42)
    return root


@pytest.fixture()
def settings(tmp_path: Path, raw_root: Path) -> Settings:
    """Fresh, isolated lakehouse per test on top of the shared raw feeds."""
    return Settings(raw_root=raw_root, lakehouse_root=tmp_path / "lakehouse", rules_path=RULES)
