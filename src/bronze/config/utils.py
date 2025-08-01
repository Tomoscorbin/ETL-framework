from pathlib import Path

import yaml

from src import settings


def load_table_config(table_key: str) -> dict:
    """Load and resolve the source table config for the given key."""
    config_path = Path(__file__).parent / "source_tables.yml"
    with config_path.open("r") as f:
        full_config = yaml.safe_load(f)

    if table_key not in full_config:
        raise ValueError(f"Source table '{table_key}' not found in config.")

    return _resolve_catalog_variables(full_config[table_key])


def _resolve_catalog_variables(conf: dict) -> dict:
    """Resolve catalog, schema, and optional description from a config dict."""
    return {
        "catalog": _resolve(conf.get("catalog"), default=settings.SOURCE_CATALOG),
        "schema": _resolve(conf.get("schema"), default=settings.SOURCE_SCHEMA),
        "table": conf["table"],
        "description": conf.get("description", ""),
    }


def _resolve(value: str | None, default: str) -> str:
    """Resolve catalog, schema, and optional description from a config dict."""
    if not value:
        return default
    if value.startswith("${") and value.endswith("}"):
        var_name = value[2:-1]
        return getattr(settings, var_name, default)
    return value


def get_fully_qualified_name(conf: dict) -> str:
    """Return the full table name from a config in catalog.schema.table format."""
    return f"{conf['catalog']}.{conf['schema']}.{conf['table']}"
