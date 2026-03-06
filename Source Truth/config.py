"""
Loads all configuration from .env so no secrets are hardcoded anywhere.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(_env_path)


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default).strip()


def _int(key: str, default: int = 0) -> int:
    try:
        return int(_get(key) or str(default))
    except ValueError:
        return default


def _bool(key: str, default: bool = False) -> bool:
    return (_get(key) or str(default)).lower() in ("true", "1", "yes")


BASE_URL = _get("BASE_URL")
AUTH_TOKEN = _get("AUTH_TOKEN")
USER_ID = _get("USER_ID")
TIMEOUT = _int("TIMEOUT", 120)

SPREADSHEET_ID = _get("SPREADSHEET_ID")
SHEET_NAME = _get("SHEET_NAME", "Sheet1")
GOOGLE_CREDENTIALS_PATH = _get("GOOGLE_CREDENTIALS_PATH", "credentials.json")

OPENAI_API_KEY = _get("OPENAI_API_KEY")

MAX_WORKERS = _int("MAX_WORKERS", 5)
SEQUENTIAL_EXECUTION = _bool("SEQUENTIAL_EXECUTION", False)


def resolve_bv_id(bv_name: str) -> str | None:
    """Resolve a Business View name to its BV ID from env vars."""
    return os.getenv(bv_name, "").strip() or None
