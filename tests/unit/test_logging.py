from __future__ import annotations

import json
import logging

from ecommerce_pipeline.logging import JsonFormatter, configure_logging, get_logger


def test_json_formatter_outputs_structured_record() -> None:
    record = logging.LogRecord("unit", logging.INFO, __file__, 1, "hello", (), None)
    payload = json.loads(JsonFormatter().format(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "unit"
    assert payload["message"] == "hello"


def test_configure_logging_sets_root_handler() -> None:
    configure_logging("WARNING")
    logger = get_logger("unit")

    assert logger.getEffectiveLevel() == logging.WARNING
