"""Runtime configuration, read from environment variables (and .env when present)."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DIR = Path(os.getenv("RAW_DIR", PROJECT_ROOT / "data" / "raw"))
LAKE_ROOT = Path(os.getenv("LAKE_ROOT", PROJECT_ROOT / "data" / "lake"))


def pg_conninfo() -> str:
    return (
        f"host={os.getenv('PG_HOST', 'localhost')} "
        f"port={os.getenv('PG_PORT', '5432')} "
        f"dbname={os.getenv('PG_DB', 'olist')} "
        f"user={os.getenv('PG_USER', 'olist')} "
        f"password={os.getenv('PG_PASSWORD', 'olist')}"
    )
