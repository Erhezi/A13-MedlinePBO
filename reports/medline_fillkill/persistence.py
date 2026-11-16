"""Persistence for the Medline Fill-Kill feed.

Each run stages the incoming rows into ``config['persistence'].staging_table``
and upserts them into ``target_table`` on (Sales Order #, Order Line #) — see
src/staged_upsert.py for the stage -> MERGE mechanics (one transaction, update
only on a real change, never delete). The staging table therefore always holds
exactly the last file loaded.
"""

from src import staged_upsert

from reports.medline_fillkill.ingestion import KEY_COLUMNS, LOAD_COLUMNS

# Column -> SQL type, in LOAD_COLUMNS order. Identifiers keep their leading
# zeros (VARCHAR); PO is the 10-digit PO number, and POLine / ShipTo the PO line
# / ship-to number without leading zeros (still VARCHAR; POLine is VARCHAR(5)
# like the PO line in PLMPreprocessorShared). In v1.1 ShipTo was appended to the
# existing tables by ensure_tables and POLine altered from INT by hand. In v1.2
# ensure_tables appended the 15 MHS v3 columns (Product Allocation Qty ..
# DED UOM Code). Plant, from the earlier WPH export, is no longer loaded but
# stays in the existing tables.
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
    "Buyer Name": "VARCHAR(100)",
    "Internal Order Comments from SAP (Z106)": "VARCHAR(1000)",
    "Product Allocation Qty": "INT",
    "Open Product Allocation Qty": "INT",
    "PA Fulfill Flag": "VARCHAR(10)",
    "Possible Subst. Item": "VARCHAR(50)",
    "Customer Preferred Substitute Item": "VARCHAR(50)",
    "DED Qty": "INT",
    "DED Valid From Date": "DATE",
    "Forecast Qty": "DECIMAL(18,3)",
    "30 Day Delivered Qty": "INT",
    "Est Avail Date": "DATE",
    "ReasonTypeText": "VARCHAR(50)",
    "MBO Type Description": "VARCHAR(100)",
    "Allocation Period Type": "VARCHAR(20)",
    "Allocation Level": "VARCHAR(20)",
    "DED UOM Code": "VARCHAR(50)",
    "PO": "VARCHAR(10)",
    "POLine": "VARCHAR(5)",
    "ShipTo": "VARCHAR(20)",
    "source file": "VARCHAR(260)",
}
assert list(COLUMN_TYPES) == LOAD_COLUMNS, "COLUMN_TYPES must follow LOAD_COLUMNS"


def stage_and_upsert(df, persist_cfg):
    """Stage *df* and upsert it into the Fill-Kill table; return row counts."""
    return staged_upsert.stage_and_upsert(
        df,
        persist_cfg,
        schema=persist_cfg["schema"],
        staging_table=persist_cfg["staging_table"],
        target_table=persist_cfg["target_table"],
        column_types=COLUMN_TYPES,
        key_columns=KEY_COLUMNS,
    )
