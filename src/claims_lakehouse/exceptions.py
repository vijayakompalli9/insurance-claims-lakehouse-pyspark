"""Custom exceptions raised by the pipeline."""


class LakehouseError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(LakehouseError):
    """Configuration or rules file is missing or invalid."""


class SourceFileMissingError(LakehouseError):
    """An expected raw feed file for a batch is not present."""


class BatchOrderError(LakehouseError):
    """A batch was requested out of order relative to the watermark state."""


class DataQualityCircuitBreakerError(LakehouseError):
    """A critical data-quality rule failed; gold publishing was blocked."""

    def __init__(self, message: str, alert_path: str) -> None:
        super().__init__(message)
        self.alert_path = alert_path


class ReconciliationError(LakehouseError):
    """Post-load reconciliation found an unexplained difference."""
