import json
from datetime import UTC, datetime
from pathlib import Path

from src.utils.logger import get_logger

logger = get_logger(__name__)


class CheckpointManager:

    def __init__(self, path):

        self.path = Path(path)
        logger.info(f"Checkpoint location: {self.path.resolve()}")

    def exists(self):
        return self.path.exists()

    def load(self):
        with open(self.path) as f:
            return json.load(f)

    def save(self, state):

        self.path.parent.mkdir(parents=True, exist_ok=True)
        state["last_run"] = datetime.now(UTC).isoformat()

        with open(self.path, "w") as f:
            json.dump(state, f, indent=4)
        logger.info(f"Checkpoint saved successfully: {self.path.resolve()}")
