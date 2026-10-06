"""Persist Medline's substitute suggestions from each PBO file.

Every Tue/Thu PBO run (and the one-off backfill) pulls the item and its
substitute columns out of the raw report and upserts them into
``config['persistence'].target_table`` (MedlinePBO.MedlineSubCollection) via a
staging table, so the subs Medline has suggested accumulate over time.

Rows are kept only when Medline gave a sub: at least one of 'Customer Prefered
Sub' / 'Substitute Material Number' is filled. The primary key is
(Medline Item, Customer Prefered Sub, Substitute Material Number); SQL Server
does not allow NULL in a key, so an empty sub column is stored as ''. Source
column names are kept as-is, including Medline's spelling of "Prefered".
"""

import os

import pandas as pd

from src import staged_upsert

KEY_COLUMNS = ["Medline Item", "Customer Prefered Sub", "Substitute Material Number"]
SUB_COLUMNS = ["Customer Prefered Sub", "Substitute Material Number"]
SOURCE_FILE_COLUMN = "source file"

# Column -> SQL type, in load order.
COLUMN_TYPES = {
    "Medline Item": "VARCHAR(30)",
    "Manufacturer": "VARCHAR(100)",
    "Manufacturer Item": "VARCHAR(50)",
    "Material Description": "VARCHAR(255)",
    "SUOM": "VARCHAR(10)",
    "Packaging String": "VARCHAR(500)",
    "Parent Item": "VARCHAR(30)",
    "Customer Prefered Sub": "VARCHAR(30)",
    "Substitute Material Number": "VARCHAR(30)",
    SOURCE_FILE_COLUMN: "VARCHAR(260)",
}
EXTRACT_COLUMNS = [c for c in COLUMN_TYPES if c != SOURCE_FILE_COLUMN]


def extract_sub_collection(df, source_name):
    """Return the distinct (item, sub) rows of one PBO file, ready to load.

    *df* is the raw PBO frame (as read by ingestion.read_pbo_file); it is not
    modified.
    """
    missing = [c for c in EXTRACT_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{source_name}: missing columns for the sub collection: {missing}")

    subs = df[EXTRACT_COLUMNS].copy()
    for col in EXTRACT_COLUMNS:
        subs[col] = subs[col].str.strip().replace("", pd.NA)

    subs = subs[subs[SUB_COLUMNS].notna().any(axis=1)]
    no_item = subs["Medline Item"].isna()
    if no_item.any():
        raise ValueError(f"{source_name}: {no_item.sum()} row(s) with a sub have no 'Medline Item'.")
    subs[SUB_COLUMNS] = subs[SUB_COLUMNS].fillna("")

    # The raw report repeats some lines verbatim; drop those first, then make the
    # key distinct. If one key still carries different details within the same
    # file, keep the first and say so.
    subs = subs.drop_duplicates()
    conflicts = subs.duplicated(KEY_COLUMNS, keep=False)
    if conflicts.any():
        examples = subs.loc[conflicts, KEY_COLUMNS].drop_duplicates().head(5)
        print(
            f"WARNING: {source_name}: {len(examples)}+ key(s) have differing details "
            f"within the file; kept the first row. e.g. "
            f"{[tuple(k) for k in examples.itertuples(index=False)]}"
        )
    subs = subs.drop_duplicates(KEY_COLUMNS, keep="first").reset_index(drop=True)

    subs[SOURCE_FILE_COLUMN] = os.path.basename(source_name)
    print(f"Sub collection: {len(subs)} distinct (item, sub) row(s) from {os.path.basename(source_name)}.")
    return subs[list(COLUMN_TYPES)]


def stage_and_upsert(subs, persist_cfg):
    """Stage *subs* and upsert them into the sub-collection table; return counts."""
    return staged_upsert.stage_and_upsert(
        subs,
        persist_cfg,
        schema=persist_cfg["schema"],
        staging_table=persist_cfg["staging_table"],
        target_table=persist_cfg["target_table"],
        column_types=COLUMN_TYPES,
        key_columns=KEY_COLUMNS,
    )


def persist_file_subs(df, source_path, persist_cfg):
    """Extract one PBO file's subs and upsert them. Returns the row counts."""
    return stage_and_upsert(extract_sub_collection(df, source_path), persist_cfg)
