"""Persistence for the Daily Autosub Management feed.

Each file is upserted into two tables, each through its own staging table (see
src/staged_upsert.py: one transaction per table, update only on a real change,
never delete):

- ``target_table`` (MedlineAutosub) — the daily snapshot, one row per
  (PRIMARY ML #, AutoSub, Report Date). An identity ``ID`` is the surrogate
  primary key; that triple is a unique key and what the MERGE matches on.
- ``relation_table`` (MedlineAutosubRelation) — which auto-subs Medline has
  assigned to each primary item, keyed on (PRIMARY ML #, AutoSub).
  ``create stamp`` is when the pair was first seen, ``update stamp`` when its
  PRIMARY PARENT or AutoSub Seq last changed. Pairs that drop off the report
  are kept.
"""

from src import staged_upsert

from reports.medline_autosub.ingestion import KEY_COLUMNS, LOAD_COLUMNS

SURROGATE_KEY = "ID"

# Column -> SQL type, in LOAD_COLUMNS order. Item numbers are VARCHAR.
COLUMN_TYPES = {
    "Branch Number": "VARCHAR(10)",
    "Customer Group": "VARCHAR(20)",
    "Customer Number": "VARCHAR(20)",
    "PRIMARY PARENT": "VARCHAR(30)",
    "PRIMARY ML #": "VARCHAR(30)",
    "FCST": "INT",
    "MIOH1": "DECIMAL(10,2)",
    "ETA for Primary": "DATE",
    "AutoSub": "VARCHAR(30)",
    "AutoSub Seq": "TINYINT",
    "Branch Forecast": "INT",
    "DED QTY": "INT",
    "MIOH": "DECIMAL(10,2)",
    "Report Date": "DATE",
    "source file": "VARCHAR(260)",
}
assert list(COLUMN_TYPES) == LOAD_COLUMNS, "COLUMN_TYPES must follow LOAD_COLUMNS"

RELATION_KEY_COLUMNS = ["PRIMARY ML #", "AutoSub"]
RELATION_COLUMN_TYPES = {
    col: COLUMN_TYPES[col]
    for col in ["PRIMARY ML #", "PRIMARY PARENT", "AutoSub", "AutoSub Seq"]
}


def stage_and_upsert(df, persist_cfg):
    """Stage *df* and upsert it into the daily Autosub table; return row counts."""
    return staged_upsert.stage_and_upsert(
        df,
        persist_cfg,
        schema=persist_cfg["schema"],
        staging_table=persist_cfg["staging_table"],
        target_table=persist_cfg["target_table"],
        column_types=COLUMN_TYPES,
        key_columns=KEY_COLUMNS,
        surrogate_key=SURROGATE_KEY,
    )


def upsert_relations(df, persist_cfg):
    """Upsert *df*'s (PRIMARY ML #, AutoSub) pairs into the relation table.

    One file has a single Report Date, so its pairs are already unique.
    """
    return staged_upsert.stage_and_upsert(
        df[list(RELATION_COLUMN_TYPES)],
        persist_cfg,
        schema=persist_cfg["schema"],
        staging_table=persist_cfg["relation_staging_table"],
        target_table=persist_cfg["relation_table"],
        column_types=RELATION_COLUMN_TYPES,
        key_columns=RELATION_KEY_COLUMNS,
    )


def persist_file(df, persist_cfg):
    """Upsert one prepared file into both tables. Returns (daily, relation) counts."""
    return stage_and_upsert(df, persist_cfg), upsert_relations(df, persist_cfg)
