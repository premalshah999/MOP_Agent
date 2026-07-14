from __future__ import annotations

from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
MANIFEST_PATH = DATA_DIR / "schema" / "manifest.json"
METADATA_PATH = DATA_DIR / "schema" / "metadata.json"
RUNTIME_DIR = DATA_DIR / "runtime"
FRONTEND_DIST = ROOT_DIR / "frontend" / "dist"
