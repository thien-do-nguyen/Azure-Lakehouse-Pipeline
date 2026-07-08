from __future__ import annotations

from typing import IO

import fsspec


def exists(path: str) -> bool:
    fs, resolved_path = fsspec.core.url_to_fs(path)
    return fs.exists(resolved_path)


def open_text(path: str, mode: str) -> IO[str]:
    return fsspec.open(path, mode=mode, encoding="utf-8").open()


def ensure_parent(path: str) -> None:
    fs, resolved_path = fsspec.core.url_to_fs(path)
    parent = resolved_path.rsplit("/", 1)[0] if "/" in resolved_path else ""
    if parent:
        fs.mkdirs(parent, exist_ok=True)
