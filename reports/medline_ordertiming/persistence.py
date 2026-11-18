"""Persistence for the Medline Order Detail (orders & confirms timing) feed.

Each file is a day-to-date snapshot, so a load REPLACES its Report Date rather
than merging: in one transaction, ``staging_table`` is truncated and reloaded
with the file, the target's rows for that Report Date are deleted, and staging
is inserted in their place (see ``src/staged_upsert.stage_and_replace``).

- Through the day each hourly file holds that day's lines so far; the latest
  file replaces the earlier one, so lines that changed (shipped, delivered,
  ASN'd) or dropped off are current.
- A new day's first file has a new Report Date and leaves earlier days alone.
- Reloading a file is harmless, and a file with no lines empties its day.

The target's primary key is (Report Date, Order #, Order Line), with Report
Date first so the day delete is a range seek. ``load stamp`` is when the row's
file was loaded.

Each lot shipped on a line also goes to ``lot_table`` (MedlineLot), staged in
``lot_staging_table`` and upserted (MERGE, never deleted) on (Order #,
Order Line, Lot Seq), with create / update stamps — see ``upsert_lots``. It is
a separate transaction after the day replace; both loads are idempotent, so a
failure in the second is put right by the next run.
"""

from src import staged_upsert

from reports.medline_ordertiming.ingestion import (
    KEY_COLUMNS, LOAD_COLUMNS, LOT_KEY_COLUMNS, LOT_LOAD_COLUMNS, REPORT_DATE_COLUMN,
)

# Column -> SQL type, in LOAD_COLUMNS order. Identifiers keep their leading
# zeros (VARCHAR), except Order Line, stored without them since v1.1; PO is the
# 10-digit PO number, and POLine / ShipTo / SoldTo the PO line / ship-to /
# sold-to number without leading zeros (still VARCHAR; POLine is VARCHAR(5)
# like the PO line in PLMPreprocessorShared). SoldTo was appended to the
# existing tables by ensure_tables in v1.1. Lot holds every lot of a multi-lot
# line joined with " | ", hence VARCHAR(255) (both tables ALTERed from
# VARCHAR(50) once, in v1.2).
COLUMN_TYPES = {
    "Order Create Date": "DATE",
    "Sold-To #": "VARCHAR(20)",
    "Sold-To Name": "VARCHAR(100)",
    "Ship-To #": "VARCHAR(20)",
    "Ship-To Name": "VARCHAR(100)",
    "Order #": "VARCHAR(20)",
    "Channel": "VARCHAR(20)",
    "PO#": "VARCHAR(50)",
    "Buyer Name": "VARCHAR(100)",
    "Cust Item #": "VARCHAR(50)",
    "PO Line #": "VARCHAR(10)",
    "Item # on PO": "VARCHAR(50)",
    "Medline Item #": "VARCHAR(50)",
    "Medline Category": "VARCHAR(20)",
    "Mfr Item #": "VARCHAR(100)",
    "Manufacturer": "VARCHAR(100)",
    "Item Description": "VARCHAR(255)",
    "Order Qty": "INT",
    "Open Qty": "INT",
    "Shipped Qty": "INT",
    "UM": "VARCHAR(10)",
    "Bad Line": "VARCHAR(10)",
    "Rej Code": "VARCHAR(10)",
    "Line Type": "VARCHAR(10)",
    "EDI Date Re": "DATE",
    "EDI Data Recv Time": "TIME(0)",
    "Order Create Time": "TIME(0)",
    "855 Date": "DATE",
    "855 Time": "TIME(0)",
    "Mins  to Order Create": "INT",
    "Mins to Confirm": "INT",
    "SAP Delv Doc #": "VARCHAR(20)",
    "SAP Delv Doc Create Date": "DATE",
    "SAP Delv Doc Create Time": "TIME(0)",
    "ASN Creation Date": "DATE",
    "ASN Creation Time": "TIME(0)",
    "Branch": "VARCHAR(10)",
    "Order Line": "VARCHAR(10)",
    "Lot": "VARCHAR(255)",
    "Expiration": "DATE",
    "PO Type": "VARCHAR(20)",
    "SCAC": "VARCHAR(10)",
    "SortCode": "VARCHAR(20)",
    "PO": "VARCHAR(10)",
    "POLine": "VARCHAR(5)",
    "ShipTo": "VARCHAR(20)",
    "SoldTo": "VARCHAR(20)",
    "Report Date": "DATE",
    "source file": "VARCHAR(260)",
}
assert list(COLUMN_TYPES) == LOAD_COLUMNS, "COLUMN_TYPES must follow LOAD_COLUMNS"

# MedlineLot: one row per lot shipped on an order line (a single Lot each).
LOT_COLUMN_TYPES = {
    "Order #": "VARCHAR(20)",
    "Order Line": "VARCHAR(10)",
    "Lot Seq": "INT",
    "Lot": "VARCHAR(50)",
    "Expiration": "DATE",
    "Order Create Date": "DATE",
    "Manufacturer": "VARCHAR(100)",
    "Mfr Item #": "VARCHAR(100)",
    "Medline Item #": "VARCHAR(50)",
    "Item Description": "VARCHAR(255)",
}
assert list(LOT_COLUMN_TYPES) == LOT_LOAD_COLUMNS, "LOT_COLUMN_TYPES must follow LOT_LOAD_COLUMNS"


def replace_day(df, report_date, persist_cfg):
    """Replace *report_date*'s rows in the target with *df*; return row counts.

    A file with fewer lines than the day already holds is still loaded (lines
    can legitimately drop off), but the shrink is printed as a WARNING.
    """
    counts = staged_upsert.stage_and_replace(
        df,
        persist_cfg,
        schema=persist_cfg["schema"],
        staging_table=persist_cfg["staging_table"],
        target_table=persist_cfg["target_table"],
        column_types=COLUMN_TYPES,
        key_columns=KEY_COLUMNS,
        partition_column=REPORT_DATE_COLUMN,
        partition_value=report_date,
    )
    if counts["inserted"] < counts["deleted"]:
        print(
            f"WARNING: Report Date {report_date} shrank from {counts['deleted']} to "
            f"{counts['inserted']} line(s) — an older file loaded after a newer one?"
        )
    return counts


def upsert_lots(lots, persist_cfg):
    """Stage *lots* and upsert them into MedlineLot; return row counts.

    Lot Seq 1 is the line's earliest-expiring lot. A lot is never deleted, so
    one that drops off a later file stays as last seen.
    """
    return staged_upsert.stage_and_upsert(
        lots,
        persist_cfg,
        schema=persist_cfg["schema"],
        staging_table=persist_cfg["lot_staging_table"],
        target_table=persist_cfg["lot_table"],
        column_types=LOT_COLUMN_TYPES,
        key_columns=LOT_KEY_COLUMNS,
    )
