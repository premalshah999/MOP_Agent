from __future__ import annotations

import argparse
import importlib
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
REQUIRED_MODULES = ("duckdb", "fastapi", "uvicorn", "openai", "pandas")


def _run(cmd: list[str], *, cwd: Path) -> None:
    completed = subprocess.run(cmd, cwd=cwd)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


def _validate_runtime() -> None:
    for module_name in REQUIRED_MODULES:
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            raise SystemExit(
                f"Missing dependency '{exc.name}' for interpreter {sys.executable}. "
                f"Install requirements with `{sys.executable} -m pip install -r requirements.txt`."
            ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Build and run the MOP agent locally in production mode.")
    parser.add_argument("--host", default=os.getenv("HOST", "127.0.0.1"))
    parser.add_argument("--port", default=os.getenv("PORT", "8000"))
    parser.add_argument("--workers", default=os.getenv("UVICORN_WORKERS", "1"))
    parser.add_argument("--skip-build", action="store_true", help="Skip the frontend production build.")
    args = parser.parse_args()

    os.chdir(ROOT)
    _validate_runtime()

    if not args.skip_build:
        _run(["npm", "run", "build"], cwd=FRONTEND)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        str(args.host),
        "--port",
        str(args.port),
        "--workers",
        str(args.workers),
    ]
    os.execvpe(sys.executable, cmd, env)


if __name__ == "__main__":
    main()
