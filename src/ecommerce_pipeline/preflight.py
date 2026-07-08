from __future__ import annotations

import argparse
import shutil
import subprocess
from dataclasses import dataclass

from ecommerce_pipeline.config import load_config


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


def _command_exists(command: str) -> bool:
    return shutil.which(command) is not None


def _run(command: list[str]) -> tuple[bool, str]:
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=10)
    except Exception as exc:  # pragma: no cover - defensive preflight path
        return False, str(exc)
    output = (result.stdout or result.stderr).strip().splitlines()
    return result.returncode == 0, output[0] if output else f"exit_code={result.returncode}"


def run_preflight(config_path: str) -> list[Check]:
    config = load_config(config_path)
    checks = [
        Check("config", True, f"{config.environment}: {config.lakehouse.base_path}"),
        Check("python", _command_exists("python3"), shutil.which("python3") or "missing"),
    ]

    java_ok, java_detail = _run(["java", "-version"]) if _command_exists("java") else (False, "missing java")
    checks.append(Check("java", java_ok, java_detail))

    docker_ok, docker_detail = (
        _run(["docker", "version", "--format", "{{.Server.Version}}"])
        if _command_exists("docker")
        else (False, "missing docker")
    )
    checks.append(Check("docker", docker_ok, docker_detail))
    return checks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check local prerequisites.")
    parser.add_argument("--config", default="configs/local.yaml")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    checks = run_preflight(args.config)
    failed = False
    for check in checks:
        status = "OK" if check.ok else "FAIL"
        print(f"{status:4} {check.name}: {check.detail}")
        failed = failed or not check.ok
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
