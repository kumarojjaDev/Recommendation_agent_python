import os
import json
from pathlib import Path
from typing import Any, Dict

# Default configuration values
_defaults: Dict[str, Any] = {
    "GEMINI_MODEL": "models/gemini-2.5-flash",
    "GEMINI_FALLBACK_MODEL": "models/gemini-2.0-flash",
    "USE_MONGO": False,
    "MONGO_URI": "mongodb://localhost:27017",
    "MONGO_DB": "recommendation_db",
    "MONGO_COLLECTION": "products",
    "PRODUCTS_FILE": "products.json",
}

# Load configuration from config.json if present (data directory)
CONFIG_FILE = Path(__file__).resolve().parent.parent / "data" / "config.json"
_config: Dict[str, Any] = {}
if CONFIG_FILE.exists():
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            _config = json.load(f)
    except Exception:
        _config = {}

# Start with defaults, update from config file
_configured: Dict[str, Any] = {**_defaults, **_config}

# Then allow environment variables to override config values
def _get_env_override(key: str, cast_type=None, default=None):
    val = os.getenv(key)
    if val is None:
        return default
    if cast_type is bool:
        return val.lower() in ("1", "true", "yes", "on")
    if cast_type is int:
        try:
            return int(val)
        except Exception:
            return default
    return val

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or _configured.get("GEMINI_API_KEY")
GEMINI_MODEL = _get_env_override("GEMINI_MODEL", default=_configured.get("GEMINI_MODEL"))
GEMINI_FALLBACK_MODEL = _get_env_override("GEMINI_FALLBACK_MODEL", default=_configured.get("GEMINI_FALLBACK_MODEL"))
USE_MONGO = _get_env_override("USE_MONGO", cast_type=bool, default=_configured.get("USE_MONGO"))
MONGO_URI = _get_env_override("MONGO_URI", default=_configured.get("MONGO_URI"))
MONGO_DB = _get_env_override("MONGO_DB", default=_configured.get("MONGO_DB"))
MONGO_COLLECTION = _get_env_override("MONGO_COLLECTION", default=_configured.get("MONGO_COLLECTION"))
PRODUCTS_FILE = _get_env_override("PRODUCTS_FILE", default=_configured.get("PRODUCTS_FILE"))
USE_POSTGRES=_get_env_override("USE_POSTGRES", cast_type=bool, default=_configured.get("USE_POSTGRES")) 
POSTGRES_DSN = _get_env_override("POSTGRES_DSN", default=_configured.get("POSTGRES_DSN"))   

# Resolve PRODUCTS_FILE to absolute path relative to data directory if needed
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRODUCTS_PATH = Path(PROJECT_ROOT / "data" / PRODUCTS_FILE)

# Expose a dict view if needed
def as_dict() -> Dict[str, Any]:
    return {
        "GEMINI_API_KEY": GEMINI_API_KEY,
        "GEMINI_MODEL": GEMINI_MODEL,
        "GEMINI_FALLBACK_MODEL": GEMINI_FALLBACK_MODEL,
        "USE_MONGO": USE_MONGO,
        "MONGO_URI": MONGO_URI,
        "MONGO_DB": MONGO_DB,
        "MONGO_COLLECTION": MONGO_COLLECTION,
        "PRODUCTS_PATH": str(PRODUCTS_PATH),
    }
