from src.delta_engine.common_tpyes import ThreePartTableName


def escape(s: str) -> str:
    return s.replace("'", "''")

def select_primary_key_name_for_table(t: ThreePartTableName) -> str:
    catalog, schema, table = map(escape, t)
    return f"""
      SELECT constraint_name AS name
      FROM information_schema.table_constraints
      WHERE constraint_type='PRIMARY KEY'
        AND table_catalog='{catalog}' AND table_schema='{schema}' AND table_name='{table}'
      LIMIT 1
    """

def select_foreign_key_names_for_source_table(t: ThreePartTableName) -> str:
    catalog, schema, table = map(escape, t)
    return f"""
      SELECT constraint_name AS name
      FROM information_schema.referential_constraints
      WHERE table_catalog='{catalog}' AND table_schema='{schema}' AND table_name='{table}'
    """

def select_referencing_foreign_keys_for_target_table(t: ThreePartTableName) -> str:
    catalog, schema, table = map(escape, t)
    return f"""
      SELECT rc.constraint_name AS name,
             rc.table_catalog  AS src_catalog,
             rc.table_schema   AS src_schema,
             rc.table_name     AS src_table
      FROM information_schema.referential_constraints rc
      JOIN information_schema.table_constraints tc
        ON rc.unique_constraint_name = tc.constraint_name
      WHERE tc.table_catalog='{catalog}' AND tc.table_schema='{schema}' AND tc.table_name='{table}'
    """
