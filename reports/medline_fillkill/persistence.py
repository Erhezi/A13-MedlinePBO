"""Stage-then-upsert persistence for the Medline Fill-Kill feed.

Each run:
  1. ``ensure_tables`` — create the schema / staging / target tables if missing.
  2. ``stage_and_upsert`` — in ONE transaction: TRUNCATE the staging table, load
     the incoming (incremental) rows into it, then MERGE staging into the target
     keyed on (Sales Order #, Order Line #). Matched rows are updated only when a
     value actually changed, so ``update stamp`` reflects real changes; new keys
     are inserted. Nothing is ever deleted from the target.

The staging table therefore always holds exactly the last file(s) loaded, which
is handy for checking what the latest hourly run brought in.

Schema / table names come from ``config['persistence']`` (trusted config) and are
bracket-quoted as identifiers.
"""

import numpy as np
import pandas as pd

from src.db import get_pyodbc_connection

from reports.medline_fillkill.ingestion import KEY_COLUMNS, LOAD_COLUMNS

# Column -> SQL type, in LOAD_COLUMNS order. Identifiers keep their leading
# zeros (VARCHAR); PO is the 10-digit PO number and POLine the integer line.
COLUMN_TYPES = {
    "Sold-To": "VARCHAR(20)",
    "Sold-To Name": "VARCHAR(100)",
    "Ship-To": "VARCHAR(20)",
    "Ship-To Name": "VARCHAR(100)",
    "Ship-To Name 2": "VARCHAR(100)",
    "Order Date": "DATE",
    "Order Time": "TIME(0)",
    "PO #": "VARCHAR(50)",
    "PO Line": "VARCHAR(10)",
    "Sales Order #": "VARCHAR(20)",
    "Order Line #": "VARCHAR(10)",
    "Item": "VARCHAR(50)",
    "Item Description": "VARCHAR(255)",
    "Item # on PO": "VARCHAR(50)",
    "Customer Item #": "VARCHAR(50)",
    "Mfr. Item #": "VARCHAR(100)",
    "Manufacturer Name": "VARCHAR(100)",
    "Order Qty": "INT",
    "UM": "VARCHAR(10)",
    "Filled Qty": "INT",
    "Plant": "VARCHAR(10)",
    "Buyer Name": "VARCHAR(100)",
    "Internal Order Comments from SAP (Z106)": "VARCHAR(1000)",
    "PO": "VARCHAR(10)",
    "POLine": "INT",
    "source file": "VARCHAR(260)",
}
assert list(COLUMN_TYPES) == LOAD_COLUMNS, "COLUMN_TYPES must follow LOAD_COLUMNS"

# Columns compared to decide whether a matched row changed (provenance excluded,
# so re-receiving an unchanged line does not bump its update stamp).
COMPARE_COLUMNS = [c for c in LOAD_COLUMNS if c not in KEY_COLUMNS and c != "source file"]


def _q(name):
    return f"[{name}]"


def _column_defs():
    defs = []
    for col, sql_type in COLUMN_TYPES.items():
        null = "NOT NULL" if col in KEY_COLUMNS else "NULL"
        defs.append(f"        {_q(col)} {sql_type} {null}")
    return defs


def _pk(table):
    return (
        f"        CONSTRAINT [PK_{table}] PRIMARY KEY CLUSTERED "
        f"({', '.join(_q(c) for c in KEY_COLUMNS)})"
    )


def _ddl(schema, staging_table, target_table):
    # Both tables carry the (Sales Order #, Order Line #) primary key: ingestion
    # already rejects a file that breaks it, and the staging PK backs that up.
    staging_cols = ",\n".join(
        _column_defs()
        + ["        [load stamp] DATETIME NOT NULL DEFAULT GETDATE()", _pk(staging_table)]
    )
    target_cols = ",\n".join(
        _column_defs()
        + [
            "        [create stamp] DATETIME NOT NULL DEFAULT GETDATE()",
            "        [update stamp] DATETIME NOT NULL DEFAULT GETDATE()",
            _pk(target_table),
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


def ensure_tables(conn, schema, staging_table, target_table):
    """Create the schema and both tables if they do not exist yet (idempotent)."""
    cursor = conn.cursor()
    for statement in _ddl(schema, staging_table, target_table):
        cursor.execute(statement)
    conn.commit()


def _merge_sql(schema, staging_table, target_table):
    on = " AND ".join(f"t.{_q(c)} = s.{_q(c)}" for c in KEY_COLUMNS)
    s_cmp = ", ".join(f"s.{_q(c)}" for c in COMPARE_COLUMNS)
    t_cmp = ", ".join(f"t.{_q(c)}" for c in COMPARE_COLUMNS)
    set_clause = ",\n            ".join(
        f"{_q(c)} = s.{_q(c)}" for c in LOAD_COLUMNS if c not in KEY_COLUMNS
    )
    insert_cols = ", ".join(_q(c) for c in LOAD_COLUMNS)
    insert_vals = ", ".join(f"s.{_q(c)}" for c in LOAD_COLUMNS)
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


def _to_records(df):
    """DataFrame -> list of tuples of plain Python values (NA -> None)."""
    return [
        tuple(_clean_value(v) for v in row)
        for row in df[LOAD_COLUMNS].astype(object).itertuples(index=False, name=None)
    ]


def stage_and_upsert(df, persist_cfg):
    """Load *df* into staging and MERGE it into the target in one transaction.

    Returns a dict with the staged / inserted / updated / unchanged row counts.
    """
    schema = persist_cfg["schema"]
    staging_table = persist_cfg["staging_table"]
    target_table = persist_cfg["target_table"]

    conn = get_pyodbc_connection(persist_cfg)
    try:
        ensure_tables(conn, schema, staging_table, target_table)

        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.execute(f"TRUNCATE TABLE [{schema}].[{staging_table}];")

        records = _to_records(df)
        if records:
            insert_sql = (
                f"INSERT INTO [{schema}].[{staging_table}] "
                f"({', '.join(_q(c) for c in LOAD_COLUMNS)}) "
                f"VALUES ({', '.join('?' for _ in LOAD_COLUMNS)})"
            )
            cursor.executemany(insert_sql, records)

        cursor.execute(_merge_sql(schema, staging_table, target_table))
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
