from src.delta_engine.types import ThreePartTableName
from src.delta_engine.utils import escape


def select_primary_key_name_for_table(t: ThreePartTableName) -> str:
    """SQL to fetch the PRIMARY KEY constraint name for a table (Unity Catalog)."""
    c, s, n = map(escape, t)
    return f"""
      SELECT constraint_name AS name
      FROM information_schema.table_constraints
      WHERE constraint_type = 'PRIMARY KEY'
        AND table_catalog = '{c}'
        AND table_schema  = '{s}'
        AND table_name    = '{n}'
      LIMIT 1
    """


def select_primary_key_columns_for_table(three_part: ThreePartTableName) -> str:
    """
    Return SQL to list PRIMARY KEY columns (with ordinal_position) for a given table.
    Columns are ordered by their position in the PK.
    """
    catalog, schema, table = three_part

    return f"""
      SELECT
        kcu.column_name        AS column_name,
        kcu.ordinal_position   AS ordinal_position
      FROM information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON  tc.constraint_catalog = kcu.constraint_catalog
        AND tc.constraint_schema  = kcu.constraint_schema
        AND tc.constraint_name    = kcu.constraint_name
        AND tc.table_catalog      = kcu.table_catalog
        AND tc.table_schema       = kcu.table_schema
        AND tc.table_name         = kcu.table_name
      WHERE tc.table_catalog   = '{escape(catalog)}'
        AND tc.table_schema    = '{escape(schema)}'
        AND tc.table_name      = '{escape(table)}'
        AND tc.constraint_type = 'PRIMARY KEY'
      ORDER BY kcu.ordinal_position
      """
