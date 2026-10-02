"""Watermark / batch state persisted as a small JSON file."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .exceptions import BatchOrderError


@dataclass
class PipelineState:
    """Tracks which batches were published and the policy change watermark."""

    published_batches: list[int] = field(default_factory=list)
    policy_watermark: str | None = None  # max policies.updated_at already applied

    @classmethod
    def load(cls, path: Path) -> PipelineState:
        if not path.exists():
            return cls()
        return cls(**json.loads(path.read_text()))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2) + "\n")

    def check_can_run(self, batch_id: int) -> None:
        """Batches must be applied in order; re-running the latest is allowed."""
        last = max(self.published_batches, default=0)
        if batch_id > last + 1:
            raise BatchOrderError(f"batch {batch_id} requested but last published is {last}")
        if batch_id < last:
            raise BatchOrderError(
                f"batch {batch_id} is older than last published batch {last}; reset to replay"
            )
