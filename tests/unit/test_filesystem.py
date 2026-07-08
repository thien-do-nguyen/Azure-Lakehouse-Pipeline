from __future__ import annotations

from ecommerce_pipeline.filesystem import ensure_parent, exists, open_text


def test_filesystem_helpers(tmp_path) -> None:
    path = str(tmp_path / "nested" / "file.txt")

    ensure_parent(path)
    with open_text(path, "w") as handle:
        handle.write("ok")

    assert exists(path)
    with open_text(path, "r") as handle:
        assert handle.read() == "ok"
