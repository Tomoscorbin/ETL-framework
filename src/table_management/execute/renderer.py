from typing import Mapping, Sequence
import pyspark.sql.types as T

def construct_full_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"

def _escape(value: str) -> str:
    return (value or "").replace("'", "''")

def _format_tblproperties(props: Mapping[str, str]) -> str:
    pairs = [f"'{_escape(str(k))}' = '{_escape('' if v is None else str(v))}'" for k, v in props.items()]
    if not pairs:
        raise ValueError("SET TBLPROPERTIES called with empty properties.")
    return ", ".join(pairs)

class SqlRenderer:
    """Pure SQL renderers. Stateless."""
    def set_tblproperties(self, full_name: str, props: Mapping[str, str]) -> str:
        return f"ALTER TABLE {full_name} SET TBLPROPERTIES ({_format_tblproperties(props)});"

    def set_table_comment(self, full_name: str, table_comment: str) -> str:
        return f"COMMENT ON TABLE {full_name} IS '{_escape(table_comment)}';"

    def add_primary_key(self, full_name: str, columns: Sequence[str]) -> str:
        cols = ", ".join(columns)
        return f"ALTER TABLE {full_name} ADD PRIMARY KEY ({cols});"

    def drop_primary_key(self, full_name: str) -> str:
        return f"ALTER TABLE {full_name} DROP PRIMARY KEY IF EXISTS;"

    def set_column_comment(self, full_name: str, column_name: str, comment: str) -> str:
        return f"ALTER TABLE {full_name} CHANGE COLUMN {column_name} COMMENT '{_escape(comment)}';"

    def add_column(self, full_name: str, name: str, dtype: T.DataType, comment: str) -> str:
        type_sql = dtype.simpleString()
        comment_sql = f" COMMENT '{_escape(comment)}'" if comment else ""
        return f"ALTER TABLE {full_name} ADD COLUMNS ({name} {type_sql}{comment_sql});"
    
    def drop_columns(self, full_name: str, column_names: list[str]) -> str:
        cols = ", ".join(column_names)
        return f"ALTER TABLE {full_name} DROP COLUMNS ({cols});"

    def change_nullability(self, full_name: str, column_name: str, make_nullable: bool) -> str:
        op = "DROP NOT NULL" if make_nullable else "SET NOT NULL"
        return f"ALTER TABLE {full_name} ALTER COLUMN {column_name} {op};"

    def create_description(self, full_name: str, schema: T.StructType, table_comment: str) -> str:
        cols = ", ".join(
            f"{f.name} {f.dataType.simpleString()}" + ("" if f.nullable else " NOT NULL")
            for f in schema.fields
        )
        return f"-- createIfNotExists {full_name} ({cols}) COMMENT '{_escape(table_comment)}'"
