"""Generic stage-then-load into a SQL Server table pair.

Used by reports that persist rows into their own helper tables (Fill-Kill and
Order Timing feeds, Daily Autosub, PBO substitute collection). Each call:

  1. ``ensure_tables`` — create the schema / staging / target tables if missing,
     and add any column in ``column_types`` that an existing table lacks (as a
     NULL column at the end, so a new derived column needs no manual ALTER;
     its value in rows already loaded stays NULL until they are reloaded or
     back-filled). Both tables carry the same primary key, so a duplicate key
     in the incoming rows fails the load.
  2. Then, in ONE transaction: TRUNCATE the staging table, load the incoming
     rows into it, and either

     - ``stage_and_upsert`` — MERGE staging into the target on the key. Matched
       rows are updated only when a value actually changed (so ``update stamp``
       reflects real changes); new keys are inserted. Nothing is ever deleted
       from the target. Or
     - ``stage_and_replace`` — DELETE the target rows of one partition (e.g.
       one Report Date) and INSERT staging in their place, so the partition
       ends up holding exactly the incoming rows. For day-to-date snapshots
       whose rows can change or disappear between files. That target carries
       a ``load stamp`` instead of create / update stamps.

The staging table therefore always holds exactly the last batch loaded.

A table is described by ``column_types`` (an ordered {column: SQL type} dict
whose order is the load order) and ``key_columns``. With ``surrogate_key`` the
target instead gets an INT IDENTITY column of that name as its primary key and
``key_columns`` become a UNIQUE constraint (still what the MERGE matches on);
the staging table keeps ``key_columns`` as its primary key. Columns listed in
``ignore_on_compare`` (provenance such as ``source file``) are still written,
but a change in them alone does not count as a change. Schema / table names
come from trusted config and are bracket-quoted as identifiers.
"""

import numpy as np
import pandas as pd

from src.db import get_pyodbc_connection

DEFAULT_IGNORE_ON_COMPARE = ("source file",)

# Audit columns appended to the target table (DATETIME, default GETDATE()).
CHANGE_STAMPS = ("create stamp", "update stamp")   # stage_and_upsert
LOAD_STAMPS = ("load stamp",)                      # stage_and_replace


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


def _unique(table, key_columns):
    return (
        f"        CONSTRAINT [UQ_{table}] UNIQUE "
        f"({', '.join(_q(c) for c in key_columns)})"
    )


def _ddl(schema, staging_table, target_table, column_types, key_columns,
         surrogate_key=None, target_stamps=CHANGE_STAMPS):
    columns = _column_defs(column_types, key_columns)
    staging_cols = ",\n".join(
        columns
        + [
            "        [load stamp] DATETIME NOT NULL DEFAULT GETDATE()",
            _pk(staging_table, key_columns),
        ]
    )
    if surrogate_key:
        target_head = [f"        {_q(surrogate_key)} INT IDENTITY(1,1) NOT NULL"]
        target_keys = [
            _pk(target_table, [surrogate_key]),
            _unique(target_table, key_columns),
        ]
    else:
        target_head = []
        target_keys = [_pk(target_table, key_columns)]
    target_cols = ",\n".join(
        target_head
        + columns
        + [f"        {_q(stamp)} DATETIME NOT NULL DEFAULT GETDATE()" for stamp in target_stamps]
        + target_keys
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


def _add_missing_columns(cursor, schema, table, column_types, key_columns):
    """ALTER TABLE ADD (as NULL) every column of *column_types* the table lacks.

    A key column cannot be added this way (it must be NOT NULL, and the table
    already has rows), so a missing one raises instead.
    """
    existing = {
        row[0].lower()
        for row in cursor.execute(
            "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)",
            f"[{schema}].[{table}]",
        ).fetchall()
    }
    missing = [col for col in column_types if col.lower() not in existing]
    missing_keys = [col for col in missing if col in key_columns]
    if missing_keys:
        raise ValueError(
            f"[{schema}].[{table}] lacks key column(s) {missing_keys}, which cannot be "
            "added to an existing table; migrate it by hand."
        )
    for col in missing:
        cursor.execute(
            f"ALTER TABLE [{schema}].[{table}] ADD {_q(col)} {column_types[col]} NULL;"
        )
        print(f"NOTE: added column {_q(col)} {column_types[col]} to [{schema}].[{table}].")


def ensure_tables(conn, schema, staging_table, target_table, column_types, key_columns,
                  surrogate_key=None, target_stamps=CHANGE_STAMPS):
    """Create the schema and both tables if missing, and add missing columns (idempotent)."""
    cursor = conn.cursor()
    for statement in _ddl(schema, staging_table, target_table, column_types, key_columns,
                          surrogate_key, target_stamps):
        cursor.execute(statement)
    for table in (staging_table, target_table):
        _add_missing_columns(cursor, schema, table, column_types, key_columns)
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


def _check_load_columns(df, column_types):
    load_columns = list(column_types)
    missing = [c for c in load_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Rows to load are missing columns: {missing}")
    return load_columns


def _reload_staging(cursor, schema, staging_table, df, load_columns):
    """TRUNCATE the staging table and insert *df* into it; return the records."""
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
    return records


def stage_and_upsert(df, conn_cfg, schema, staging_table, target_table,
                     column_types, key_columns,
                     ignore_on_compare=DEFAULT_IGNORE_ON_COMPARE,
                     surrogate_key=None):
    """Load *df* into staging and MERGE it into the target in one transaction.

    *conn_cfg* holds driver/server/database/trusted_connection. Returns a dict
    with the staged / inserted / updated / unchanged row counts.
    """
    load_columns = _check_load_columns(df, column_types)

    conn = get_pyodbc_connection(conn_cfg)
    try:
        ensure_tables(conn, schema, staging_table, target_table, column_types, key_columns,
                      surrogate_key)

        cursor = conn.cursor()
        records = _reload_staging(cursor, schema, staging_table, df, load_columns)

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


def stage_and_replace(df, conn_cfg, schema, staging_table, target_table,
                      column_types, key_columns, partition_column, partition_value):
    """Replace one partition of the target with *df*, in one transaction.

    TRUNCATE staging -> load *df* -> DELETE the target rows whose
    *partition_column* equals *partition_value* -> INSERT staging into the
    target. Afterwards that partition holds exactly *df*, so rows that dropped
    off the source are gone. An empty *df* empties the partition, which is why
    *partition_value* is passed explicitly rather than read from the rows.

    *partition_column* should lead *key_columns* so the delete is a range seek
    on the clustered primary key. Returns a dict with the staged / deleted /
    inserted row counts.
    """
    load_columns = _check_load_columns(df, column_types)
    stray = df[partition_column].ne(partition_value)
    if stray.any():
        raise ValueError(
            f"{stray.sum()} row(s) have {partition_column} other than {partition_value}: "
            f"{sorted(map(str, df.loc[stray, partition_column].unique()))[:5]}"
        )

    conn = get_pyodbc_connection(conn_cfg)
    try:
        ensure_tables(conn, schema, staging_table, target_table, column_types, key_columns,
                      target_stamps=LOAD_STAMPS)

        cursor = conn.cursor()
        records = _reload_staging(cursor, schema, staging_table, df, load_columns)

        cursor.execute(
            f"DELETE FROM [{schema}].[{target_table}] WHERE {_q(partition_column)} = ?;",
            partition_value,
        )
        deleted = cursor.rowcount
        cols = ", ".join(_q(c) for c in load_columns)
        cursor.execute(
            f"INSERT INTO [{schema}].[{target_table}] ({cols}, [load stamp]) "
            f"SELECT {cols}, GETDATE() FROM [{schema}].[{staging_table}];"
        )
        inserted = cursor.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    counts = {"staged": len(records), "deleted": int(deleted), "inserted": int(inserted)}
    print(
        f"Staged {counts['staged']} row(s) into [{schema}].[{staging_table}]; replaced "
        f"{partition_column} {partition_value} in [{schema}].[{target_table}] — deleted "
        f"{counts['deleted']}, inserted {counts['inserted']}."
    )
    return counts
