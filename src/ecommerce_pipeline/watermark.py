from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from ecommerce_pipeline.config import AppConfig
from ecommerce_pipeline.filesystem import ensure_parent, exists, open_text


def load_watermarks(config: AppConfig) -> dict[str, str]:
    if not exists(config.batch.watermark_path):
        return {}
    with open_text(config.batch.watermark_path, "r") as handle:
        return json.load(handle)


def save_watermarks(config: AppConfig, watermarks: dict[str, str]) -> None:
    ensure_parent(config.batch.watermark_path)
    with open_text(config.batch.watermark_path, "w") as handle:
        json.dump(watermarks, handle, indent=2, sort_keys=True)
        handle.write("\n")


def get_watermark(config: AppConfig, table_name: str) -> str | None:
    return load_watermarks(config).get(table_name)


def update_watermark(config: AppConfig, table_name: str, value: Any) -> None:
    if value is None:
        return
    watermarks = load_watermarks(config)
    if isinstance(value, datetime):
        watermarks[table_name] = value.isoformat(sep=" ")
    else:
        watermarks[table_name] = str(value)
    save_watermarks(config, watermarks)
