"""Manage pipeline checkpoint state for incremental data generation."""

import json
from datetime import UTC, datetime
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger(__name__)


class CheckpointManager:
    """Read and write checkpoint state used by incremental pipeline runs."""

    def __init__(self, path):
        """Initialise the checkpoint manager with a checkpoint file path."""
        self.path = Path(path)
        logger.info(f"Checkpoint location: {self.path.resolve()}")

    def exists(self):
        """Return whether the checkpoint file already exists."""
        return self.path.exists()

    def load(self):
        """Load and return the persisted checkpoint state."""
        with open(self.path) as f:
            return json.load(f)

    def save(self, state):
        """Persist the current pipeline state to the checkpoint file."""
        # Create the checkpoint directory when it does not already exist.
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Record when the checkpoint was last updated.
        state["last_run"] = datetime.now(UTC).isoformat()

        with open(self.path, "w") as f:
            json.dump(state, f, indent=4)

        logger.info(f"Checkpoint saved successfully: {self.path.resolve()}")
