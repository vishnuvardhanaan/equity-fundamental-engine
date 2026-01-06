import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from equity_pipeline.db import get_connection


@dataclass
class StepSummary:
    name: str
    attempts: int = 0
    duration: float = 0.0
    status: str = "PENDING"
    error: Optional[str] = None


@dataclass
class RunSummary:
    started_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    status: str = "RUNNING"
    steps: Dict[str, StepSummary] = field(default_factory=dict)
    run_id: Optional[int] = None

    def finish(self, status: str):
        self.finished_at = time.time()
        self.status = status
        self._persist()

    def _persist(self):
        with get_connection() as conn:
            cur = conn.cursor()

            cur.execute(
                "INSERT INTO pipeline_runs (started_at, finished_at, status) VALUES (?, ?, ?)",
                (self.started_at, self.finished_at, self.status),
            )
            self.run_id = cur.lastrowid

            for step in self.steps.values():
                cur.execute(
                    """
                    INSERT INTO pipeline_steps
                    (run_id, name, attempts, duration, status, error)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        self.run_id,
                        step.name,
                        step.attempts,
                        step.duration,
                        step.status,
                        step.error,
                    ),
                )

    @property
    def total_duration(self):
        if not self.finished_at:
            return None
        return self.finished_at - self.started_at
