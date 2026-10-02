"""Structured key=value logging on top of the stdlib ``logging`` module."""

from __future__ import annotations

import logging
import sys
import time
from typing import Any


class KeyValueFormatter(logging.Formatter):
    """Render records as ``ts=... level=... logger=... msg="..." k=v`` (UTC timestamps)."""

    converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        parts = [
            f"ts={self.formatTime(record, '%Y-%m-%dT%H:%M:%SZ')}",
            f"level={record.levelname}",
            f"logger={record.name}",
            f'msg="{record.getMessage()}"',
        ]
        for key, value in getattr(record, "kv", {}).items():
            parts.append(f"{key}={value}")
        if record.exc_info:
            parts.append(f'exc="{self.formatException(record.exc_info)!r}"')
        return " ".join(parts)


def configure_logging(level: str = "INFO") -> None:
    """Install the key=value formatter on the root logger (idempotent)."""
    root = logging.getLogger()
    root.setLevel(level)
    if not any(isinstance(h.formatter, KeyValueFormatter) for h in root.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(KeyValueFormatter())
        root.addHandler(handler)
    for noisy in ("py4j", "pyspark"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_kv(logger: logging.Logger, msg: str, level: int = logging.INFO, **kv: Any) -> None:
    """Log ``msg`` with structured key/value context."""
    logger.log(level, msg, extra={"kv": kv})
