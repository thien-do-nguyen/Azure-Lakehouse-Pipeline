from __future__ import annotations

from datetime import datetime

from ecommerce_pipeline.watermark import get_watermark, load_watermarks, update_watermark


def test_watermark_round_trip(local_config) -> None:
    assert load_watermarks(local_config) == {}

    update_watermark(local_config, "orders", datetime(2026, 1, 1, 12, 30, 0))

    assert get_watermark(local_config, "orders") == "2026-01-01 12:30:00"
