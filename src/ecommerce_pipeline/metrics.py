from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


class MetricsCollector:
    def __init__(self, path: str = "logs/metrics.jsonl") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, name: str, value: float, **tags: str) -> None:
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "name": name,
            "value": value,
            "tags": tags,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")

    @contextmanager
    def timer(self, name: str, **tags: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
            status = "success"
        except Exception:
            status = "failed"
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self.emit(name, elapsed_ms, status=status, **tags)
