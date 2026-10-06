"""Generic stage-then-upsert into a SQL Server table pair.

Used by reports that persist rows into their own helper tables (Fill-Kill feed,
PBO substitute collection). Each call:

  1. ``ensure_tables`` — create the schema / staging / target tables if missing.
     Both tables carry the same primary key, so a duplicate key in the incoming
     rows fails the load.
  2. ``stage_and_upsert`` — in ONE transaction: TRUNCATE the staging table, load
     the incoming rows into it, then MERGE staging into the target on the key.
     Matched rows are updated only when a value actually changed (so
     ``update stamp`` reflects real changes); new keys are inserted. Nothing is
     ever deleted from the target.

The staging table therefore always holds exactly the last batch loaded.

A table is described by ``column_types`` (an ordered {column: SQL type} dict
whose order is the load order) and ``key_columns``. Columns listed in
``ignore_on_compare`` (provenance such as ``source file``) are still written,
but a change in them alone does not count as a change. Schema / table names
come from trusted config and are bracket-quoted as identifiers.
"""

import numpy as np
import pandas as pd

from src.db import get_pyodbc_connection

DEFAULT_IGNORE_ON_COMPARE = ("source file",)


def _q(name):
    return f"[{name}]"


def _column_defs(column_types, key_columns):
    defs = []
    for col, sql_type in column_types.items():
        null = "NOT NULL" if col in key_columns else "NULL"
        defs.append(f"        {_q(col)} {sql_type} {null}")
    return defs


def _pk(table, key_columns):
    return (
        f"        CONSTRAINT [PK_{table}] PRIMARY KEY CLUSTERED "
        f"({', '.join(_q(c) for c in key_columns)})"
    )


def _ddl(schema, staging_table, target_table, column_types, key_columns):
    columns = _column_defs(column_types, key_columns)
    staging_cols = ",\n".join(
        columns
        + [
            "        [load stamp] DATETIME NOT NULL DEFAULT GETDATE()",
            _pk(staging_table, key_columns),
        ]
    )
    target_cols = ",\n".join(
        columns
        + [
            "        [create stamp] DATETIME NOT NULL DEFAULT GETDATE()",
            "        [update stamp] DATETIME NOT NULL DEFAULT GETDATE()",
            _pk(target_table, key_columns),
        ]
    )
    return [
        f"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');",
        f"""IF OBJECT_ID(N'[{schema}].[{staging_table}]', N'U') IS NULL
    CREATE TABLE [{schema}].[{staging_table}] (
{staging_cols}
    );""",
        f"""IF OBJECT_ID(N'[{schema}].[{target_table}]', N'U') IS NULL
    CREATE TABLE [{schema}].[{target_table}] (
{target_cols}
    );""",
    ]


def ensure_tables(conn, schema, staging_table, target_table, column_types, key_columns):
    """Create the schema and both tables if they do not exist yet (idempotent)."""
    cursor = conn.cursor()
    for statement in _ddl(schema, staging_table, target_table, column_types, key_columns):
        cursor.execute(statement)
    conn.commit()


def _merge_sql(schema, staging_table, target_table, load_columns, key_columns,
               ignore_on_compare):
    compare = [c for c in load_columns if c not in key_columns and c not in ignore_on_compare]
    on = " AND ".join(f"t.{_q(c)} = s.{_q(c)}" for c in key_columns)
    s_cmp = ", ".join(f"s.{_q(c)}" for c in compare)
    t_cmp = ", ".join(f"t.{_q(c)}" for c in compare)
    set_clause = ",\n            ".join(
        f"{_q(c)} = s.{_q(c)}" for c in load_columns if c not in key_columns
    )
    insert_cols = ", ".join(_q(c) for c in load_columns)
    insert_vals = ", ".join(f"s.{_q(c)}" for c in load_columns)
    # EXISTS(... EXCEPT ...) is a NULL-safe "any column differs" test.
    return f"""
    SET NOCOUNT ON;
    DECLARE @actions TABLE (action NVARCHAR(10));

    MERGE [{schema}].[{target_table}] WITH (HOLDLOCK) AS t
    USING [{schema}].[{staging_table}] AS s
        ON {on}
    WHEN MATCHED AND EXISTS (SELECT {s_cmp} EXCEPT SELECT {t_cmp}) THEN
        UPDATE SET
            {set_clause},
            [update stamp] = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_cols}, [create stamp], [update stamp])
        VALUES ({insert_vals}, GETDATE(), GETDATE())
    OUTPUT $action INTO @actions;

    SELECT
        COALESCE(SUM(CASE WHEN action = 'INSERT' THEN 1 ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN action = 'UPDATE' THEN 1 ELSE 0 END), 0)
    FROM @actions;
    """


def _clean_value(value):
    if value is None or value is pd.NA or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, np.integer):
        return int(value)
    return value


def _to_records(df, load_columns):
    """DataFrame -> list of tuples of plain Python values (NA -> None)."""
    return [
        tuple(_clean_value(v) for v in row)
        for row in df[load_columns].astype(object).itertuples(index=False, name=None)
    ]


def stage_and_upsert(df, conn_cfg, schema, staging_table, target_table,
                     column_types, key_columns,
                     ignore_on_compare=DEFAULT_IGNORE_ON_COMPARE):
    """Load *df* into staging and MERGE it into the target in one transaction.

    *conn_cfg* holds driver/server/database/trusted_connection. Returns a dict
    with the staged / inserted / updated / unchanged row counts.
    """
    load_columns = list(column_types)
    missing = [c for c in load_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Rows to load are missing columns: {missing}")

    conn = get_pyodbc_connection(conn_cfg)
    try:
        ensure_tables(conn, schema, staging_table, target_table, column_types, key_columns)

        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.execute(f"TRUNCATE TABLE [{schema}].[{staging_table}];")

        records = _to_records(df, load_columns)
        if records:
            insert_sql = (
                f"INSERT INTO [{schema}].[{staging_table}] "
                f"({', '.join(_q(c) for c in load_columns)}) "
                f"VALUES ({', '.join('?' for _ in load_columns)})"
            )
            cursor.executemany(insert_sql, records)

        cursor.execute(_merge_sql(
            schema, staging_table, target_table, load_columns, key_columns,
            ignore_on_compare,
        ))
        inserted, updated = cursor.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    counts = {
        "staged": len(records),
        "inserted": int(inserted),
        "updated": int(updated),
        "unchanged": len(records) - int(inserted) - int(updated),
    }
    print(
        f"Staged {counts['staged']} row(s) into [{schema}].[{staging_table}]; "
        f"upserted into [{schema}].[{target_table}] — inserted {counts['inserted']}, "
        f"updated {counts['updated']}, unchanged {counts['unchanged']}."
    )
    return counts
