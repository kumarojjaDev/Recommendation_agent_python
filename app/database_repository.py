"""
Repository layer prepared for PostgreSQL but currently backed by local JSON.

This module exposes small helper functions used by `main.py` to fetch
product data. By default the implementation reads PRODUCTS_PATH and returns
Pydantic `Product` instances. If configured to use Postgres (USE_POSTGRES),
it will attempt to read from the configured database; if that fails it falls
back to the local JSON file.

To avoid circular imports we import the `Product` model inside the functions
at runtime rather than at module import time.
"""

from pathlib import Path
import json
import logging
from typing import List, Optional, Any, Iterable

# Import config values (ensure these exist in your app.config)
from app.config import PRODUCTS_PATH, USE_POSTGRES, POSTGRES_DSN

# Import psycopg2 dependencies only if available
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except Exception:
    PSYCOPG2_AVAILABLE = False

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DATA_FILE = Path(PRODUCTS_PATH)


def _load_json_data() -> List[dict]:
    """Load product data from JSON file. Returns empty list on error."""
    if not DATA_FILE.exists():
        logger.warning("Products JSON file not found at %s", DATA_FILE)
        return []

    try:
        with DATA_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            logger.warning("Products JSON root is not a list; got %s", type(data).__name__)
            return []
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in products file %s: %s", DATA_FILE, e)
        return []
    except Exception as e:
        logger.exception("Unexpected error loading products JSON: %s", e)
        return []


def _rows_to_dicts(rows) -> List[dict]:
    """Convert psycopg2 RealDict rows to plain dicts (safe copy)."""
    return [dict(r) for r in rows] if rows else []


def _load_postgres_data(limit: Optional[int] = None, offset: int = 0) -> List[dict]:
    """
    Load product data from Postgres.

    Expects a table named `products` with columns:
      - id (int)
      - name (text)
      - category (text)
      - brand (text)
      - model (text)
      - attributes (jsonb)
      - tags (text[])  -- Postgres array
      - image_url (text)
      - description (text)

    Returns list of dicts whose structure matches the original JSON objects.
    """
    if not PSYCOPG2_AVAILABLE:
        logger.warning("psycopg2 not installed; Postgres support disabled.")
        return []

    if not POSTGRES_DSN:
        logger.warning("POSTGRES_DSN not configured; skipping Postgres load.")
        return []

    conn = None
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Build SQL with safe parameter placeholders
        sql = """
            SELECT
              id,
              name,
              category,
              brand,
              model,
              attributes::jsonb AS attributes,
              -- convert text[] tags into JSON array (null-safe)
              CASE WHEN tags IS NULL THEN NULL ELSE array_to_json(tags) END AS tags,
              image_url,
              description
            FROM products
            ORDER BY id
        """
        params = []
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
        if offset:
            sql += " OFFSET %s"
            params.append(int(offset))

        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close()
        return _rows_to_dicts(rows)
    except Exception as e:
        logger.exception("Failed to load data from Postgres: %s", e)
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _load_data(limit: Optional[int] = None, offset: int = 0) -> List[dict]:
    """Load data from the configured source (Postgres or JSON)."""
    if USE_POSTGRES:
        pg_data = _load_postgres_data(limit=limit, offset=offset)
        if pg_data:
            return pg_data
        logger.info("Falling back to local JSON data source.")
    return _load_json_data()


def _to_products(data: Iterable[dict]) -> List[object]:
    """
    Convert sequence of dicts to list of Pydantic Product instances.

    We import Product at runtime to avoid circular imports.
    """
    if not data:
        return []

    try:
        # import locally to avoid circular import at module import time
        from models.schemas import Product  # adjust path if your app structure differs
    except Exception:
        # try alternative import path if app.* structure is used elsewhere
        try:
            from app.models.schemas import Product  # type: ignore
        except Exception as e:
            logger.exception("Failed to import Product schema: %s", e)
            # As a last resort, return raw dicts
            return [item for item in data]

    products = []
    for item in data:
        try:
            products.append(Product.parse_obj(item))
        except Exception as e:
            logger.warning("Failed to parse product item into Product model: %s; item=%r", e, item)
            # skip invalid items (or optionally append raw item)
    return products


def get_all_products(limit: Optional[int] = None, offset: int = 0) -> List[object]:
    """
    Return a list of `Product` instances loaded from the configured data source.

    Parameters:
    - limit: optional maximum number of products to return (useful for pagination)
    - offset: optional number of products to skip

    Returns:
    - List[object] where each object is an instance of the Pydantic Product model
      (imported at runtime). If the Product model cannot be imported, raw dicts
      are returned as a fallback.
    """
    raw = _load_data(limit=limit, offset=offset)
    return _to_products(raw)


def get_product_by_id(product_id: Any) -> Optional[object]:
    """
    Retrieve a single product by its id (exact match).

    Tries Postgres lookup first (if enabled), otherwise falls back to JSON scan.
    """
    # If using Postgres, try a direct query to avoid pulling all products
    if USE_POSTGRES and PSYCOPG2_AVAILABLE and POSTGRES_DSN:
        conn = None
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Attempt to match numeric or string id; use parameterized query
            sql = """
              SELECT
                id,
                name,
                category,
                brand,
                model,
                attributes::jsonb AS attributes,
                CASE WHEN tags IS NULL THEN NULL ELSE array_to_json(tags) END AS tags,
                image_url,
                description
              FROM products
              WHERE id = %s
              LIMIT 1;
            """
            cur.execute(sql, (product_id,))
            row = cur.fetchone()
            cur.close()
            if row:
                # _to_products expects an iterable of dicts; return first element
                prods = _to_products([dict(row)])
                return prods[0] if prods else None
        except Exception:
            logger.exception("Postgres query failed for id=%r; falling back to JSON", product_id)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # fallback: scan JSON or already-loaded list
    products = get_all_products()
    for p in products:
        try:
            pid = getattr(p, "id", None)
        except Exception:
            pid = None
        if pid == product_id or str(pid) == str(product_id):
            return p
    return None


def find_by_exact_or_partial_name(name: str) -> Optional[object]:
    """
    Find a product by exact or partial name using the configured data source.

    Returns a Product instance or None.
    """
    lowered = name.strip().lower()

    # If Postgres is available, prefer server-side LIKE queries for speed
    if USE_POSTGRES and PSYCOPG2_AVAILABLE and POSTGRES_DSN:
        conn = None
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Exact match first
            cur.execute("SELECT * FROM products WHERE lower(name) = %s LIMIT 1;", (lowered,))
            row = cur.fetchone()
            if row:
                cur.close()
                return _to_products([dict(row)])[0]

            # Word-based partial match (all words must appear in name)
            words = [w for w in lowered.split() if w]
            if words:
                # Build combined ILIKE conditions safely
                conditions = " AND ".join("name ILIKE %s" for _ in words)
                params = [f"%{w}%" for w in words]
                sql = f"SELECT * FROM products WHERE {conditions} LIMIT 1;"
                cur.execute(sql, params)
                row = cur.fetchone()
                cur.close()
                if row:
                    return _to_products([dict(row)])[0]
            cur.close()
        except Exception:
            logger.exception("Postgres name search failed for %r; falling back to JSON", name)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # fallback to in-memory JSON search
    products = get_all_products()
    # exact match
    for p in products:
        try:
            pname = p.name
        except Exception:
            pname = p.get("name") if isinstance(p, dict) else None
        if pname and pname.lower() == lowered:
            return p

    # partial word-based match
    words = lowered.split()
    for p in products:
        try:
            pname = p.name.lower()
        except Exception:
            pname = (p.get("name") or "").lower() if isinstance(p, dict) else ""
        if all(word in pname for word in words):
            return p

    return None


def find_by_tag(tag: str) -> List[object]:
    """
    Return a list of products that have `tag` in their tags list.
    Case-insensitive match.

    Uses server-side query on Postgres when available.
    """
    if not tag:
        return []

    lowered = tag.strip().lower()

    # If Postgres is available, perform server-side query
    if USE_POSTGRES and PSYCOPG2_AVAILABLE and POSTGRES_DSN:
        conn = None
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Use ANY with lower() to match case-insensitively.
            # This assumes tags is a text[] column.
            sql = """
              SELECT
                id, name, category, brand, model,
                attributes::jsonb AS attributes,
                CASE WHEN tags IS NULL THEN NULL ELSE array_to_json(tags) END AS tags,
                image_url, description
              FROM products
              WHERE EXISTS (
                SELECT 1 FROM unnest(tags) AS t WHERE lower(t) = %s
              )
              ORDER BY id;
            """
            cur.execute(sql, (lowered,))
            rows = cur.fetchall()
            cur.close()
            return _to_products(_rows_to_dicts(rows))
        except Exception:
            logger.exception("Postgres tag query failed for tag=%r; falling back to JSON", tag)
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    # fallback to in-memory filter
    results = []
    products = get_all_products()
    for p in products:
        try:
            tags = getattr(p, "tags", None)
        except Exception:
            tags = p.get("tags") if isinstance(p, dict) else None

        if not tags:
            continue
        # tags may be list[str] or a comma-separated string; normalize
        if isinstance(tags, str):
            tag_list = [t.strip().lower() for t in tags.split(",") if t.strip()]
        else:
            tag_list = [str(t).lower() for t in tags]

        if lowered in tag_list:
            results.append(p)
    return results


# --------------------
# Postgres notes:
# - Set USE_POSTGRES=true in config or environment variable to enable Postgres.
# - Provide POSTGRES_DSN in app.config (e.g. "postgres://user:pass@host:5432/dbname").
# - The products table should exist with the columns described in _load_postgres_data.
# - psycopg2 is required for Postgres support: pip install psycopg2-binary
# --------------------
