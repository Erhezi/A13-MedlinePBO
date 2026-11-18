"""Ingestion & cleansing for Medline's Order Detail (orders & confirms timing) feed.

The source is Medline's "MHS - ORDER DETAIL REPORT-<yyyy-mm-dd-hh-mm-ss>.xls"
export ("Order Detail with Confirm and ASN Timing" in its footer). One sheet:

    header      43 columns (SOURCE_COLUMNS)
    data        one row per order line: (Order #, Order Line) is unique
    blank row
    totals      "Grand Totals", then "# of PO's:", "# of Medline Orders:",
                "# of Order Lines:", "# of Bad / Rejected Lines:", "EDI 850 PO
                Count", "EDI 855 Confirm Count", "EDI 856 ASN Count" — label in
                Sold-To Name, value in Ship-To #, every other cell blank
    blank row
    footer      "Dates & Times Time Zone:", the report title,
                "Date Range:mm/dd/yyyy to mm/dd/yyyy" and the run date — first
                cell only

Each file is a day-to-date snapshot: the Date Range is a single day, which
becomes ``Report Date``, and a later file the same day repeats the earlier
lines plus the new ones (see persistence.py for the day-replace load).

Once a line ships, Medline writes it once per lot shipped (seen from
2026-11-17): every column repeats except Lot and Expiration, and the
quantities are the line's, not the lot's. ``split_lots`` folds such a line back
into one row (lots joined in Lot, earliest Expiration) and lists every lot
separately for the MedlineLot table (see persistence.py). Medline's
"# of Order Lines" counts lines, so it is checked against the folded rows.

Checks that fail the run before anything is written: a missing column, no
single-day Date Range, a data row missing Order # / Order Line, a line that
repeats with anything other than Lot / Expiration differing, and an
unparseable date / time / number. A "# of Order Lines" total that disagrees
with the lines read is a warning that is also emailed (``Prepared.alerts``);
a mismatch in the other totals, or a column outside SOURCE_COLUMNS (not
loaded), is only a warning (ETL-health Error column).

Source column names are kept as-is (whitespace-stripped, so typos such as
"EDI Date Re" and the double space in "Mins  to Order Create" stay).
``Order Line`` is loaded without its leading zeros ("000010" -> "10", still
text), as in the Fill-Kill feed (since v1.1). Derived columns (see
src/medline_keys.py), then Report Date and source file, follow:

- ``PO``     — the leading 10 digits of ``PO#`` ("3000571298-STPO" -> "3000571298")
- ``POLine`` — ``PO Line #`` with leading zeros removed ("000001" -> "1")
- ``ShipTo`` — ``Ship-To #`` with leading zeros removed ("0002522416" -> "2522416")
- ``SoldTo`` — ``Sold-To #`` with leading zeros removed ("0002159659" -> "2159659")
"""

import os
import re
from collections import namedtuple
from datetime import datetime

import pandas as pd

from src import medline_keys

LINE_KEY = ["Order #", "Order Line"]
KEY_COLUMNS = ["Report Date"] + LINE_KEY

SOURCE_COLUMNS = [
    "Order Create Date", "Sold-To #", "Sold-To Name", "Ship-To #", "Ship-To Name",
    "Order #", "Channel", "PO#", "Buyer Name", "Cust Item #", "PO Line #",
    "Item # on PO", "Medline Item #", "Medline Category", "Mfr Item #",
    "Manufacturer", "Item Description", "Order Qty", "Open Qty", "Shipped Qty",
    "UM", "Bad Line", "Rej Code", "Line Type", "EDI Date Re", "EDI Data Recv Time",
    "Order Create Time", "855 Date", "855 Time", "Mins  to Order Create",
    "Mins to Confirm", "SAP Delv Doc #", "SAP Delv Doc Create Date",
    "SAP Delv Doc Create Time", "ASN Creation Date", "ASN Creation Time",
    "Branch", "Order Line", "Lot", "Expiration", "PO Type", "SCAC", "SortCode",
]
DERIVED_COLUMNS = ["PO", "POLine", "ShipTo", "SoldTo"]
REPORT_DATE_COLUMN = "Report Date"
SOURCE_FILE_COLUMN = "source file"

# Column order shared by the staging and target tables (see persistence.py).
LOAD_COLUMNS = SOURCE_COLUMNS + DERIVED_COLUMNS + [REPORT_DATE_COLUMN, SOURCE_FILE_COLUMN]

# One row per lot shipped on a line, for the MedlineLot tables.
LOT_COLUMN = "Lot"
EXPIRATION_COLUMN = "Expiration"
LOT_SEQ_COLUMN = "Lot Seq"
LOT_SEPARATOR = " | "
LOT_KEY_COLUMNS = LINE_KEY + [LOT_SEQ_COLUMN]
LOT_LOAD_COLUMNS = LOT_KEY_COLUMNS + [
    LOT_COLUMN, EXPIRATION_COLUMN, "Order Create Date", "Manufacturer", "Mfr Item #",
    "Medline Item #", "Item Description",
]

# What prepare() returns. *alerts* are the warnings (also in *warnings*) that
# are emailed to notification.warning_recipients.
Prepared = namedtuple("Prepared", "rows lots report_date warnings alerts")

# Real Excel dates/times, except that Medline writes "SAP Delv Doc Create Time"
# and "ASN Creation Time" as text ("7:30:18AM") and "ASN Creation Date" as
# "10/07/2026" — format="mixed" parses each cell on its own.
DATE_COLUMNS = [
    "Order Create Date", "EDI Date Re", "855 Date", "SAP Delv Doc Create Date",
    "ASN Creation Date", "Expiration",
]
TIME_COLUMNS = [
    "EDI Data Recv Time", "Order Create Time", "855 Time",
    "SAP Delv Doc Create Time", "ASN Creation Time",
]
INT_COLUMNS = [
    "Order Qty", "Open Qty", "Shipped Qty", "Mins  to Order Create", "Mins to Confirm",
]

# The totals block: label in TOTALS_LABEL_COLUMN, value in TOTALS_VALUE_COLUMN.
TOTALS_LABEL_COLUMN = "Sold-To Name"
TOTALS_VALUE_COLUMN = "Ship-To #"
ORDER_LINES_TOTAL = "# of Order Lines"

DATE_RANGE_PATTERN = re.compile(
    r"Date Range:\s*(\d{1,2}/\d{1,2}/\d{4})\s*to\s*(\d{1,2}/\d{1,2}/\d{4})"
)


def read_order_detail_file(file_path):
    """Read one export: strip names/values, validate, split off totals and footer.

    Returns (rows, totals, footer, warnings). A missing column raises; a
    column outside SOURCE_COLUMNS is not loaded and only yields a warning
    (ETL-health Error column, no email), as in the Fill-Kill feed.
    """
    source_name = os.path.basename(file_path)
    df = pd.read_excel(file_path, engine="calamine", dtype=str)
    df.columns = [str(col).strip() for col in df.columns]
    extra = unexpected_columns(df)
    validate_columns(df, SOURCE_COLUMNS, extra)
    warnings = []
    if extra:
        warnings.append(
            f"Warning: {source_name} has {len(extra)} column(s) outside the expected "
            f"layout, not loaded: {extra}"
        )
        print(warnings[-1])
    df = df[SOURCE_COLUMNS].copy()
    for col in SOURCE_COLUMNS:
        df[col] = df[col].str.strip().replace("", pd.NA)
    return (*split_trailer(df), warnings)


def unexpected_columns(df):
    """Columns of *df* outside SOURCE_COLUMNS.

    A blank-header column with no values (pandas names it "Unnamed: <n>") is
    just empty formatted cells, not a new column, so it is ignored.
    """
    return [
        col for col in df.columns
        if col not in SOURCE_COLUMNS
        and not (col.startswith("Unnamed:") and df[col].isna().all())
    ]


def validate_columns(df, required_columns, unexpected=()):
    """Raise ValueError if any *required_columns* are missing from *df*.

    *unexpected* (the file's columns outside the expected layout) is added to
    the message, so a renamed column shows up as a missing / unexpected pair.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        detail = f"Missing columns: {missing}"
        if unexpected:
            detail += f"; unexpected columns (renamed?): {list(unexpected)}"
        raise ValueError(f"Validation Failed. {detail}")
    print("Column validation successful.")
    return True


def split_trailer(df):
    """Split the data rows from the totals block and the footer, by shape.

    - blank: every cell empty (the spacer rows) — dropped
    - footer: only the first cell filled
    - totals: only the totals label (and its value) filled

    No order line looks like either (every line has an Order #), so a data row
    with a missing key is kept and fails ``validate_keys`` loudly instead.
    """
    filled = df.notna()
    blank = ~filled.any(axis=1)
    footer = filled[SOURCE_COLUMNS[0]] & ~filled[SOURCE_COLUMNS[1:]].any(axis=1)
    others = [c for c in SOURCE_COLUMNS if c not in (TOTALS_LABEL_COLUMN, TOTALS_VALUE_COLUMN)]
    totals = filled[TOTALS_LABEL_COLUMN] & ~filled[others].any(axis=1)
    data = ~(blank | footer | totals)
    return df[data].reset_index(drop=True), df[totals], df[footer]


def parse_report_date(footer, source_name):
    """Return the single day of the footer's "Date Range: mm/dd/yyyy to mm/dd/yyyy"."""
    for text in footer[SOURCE_COLUMNS[0]].dropna():
        match = DATE_RANGE_PATTERN.search(text)
        if match:
            start, end = (datetime.strptime(d, "%m/%d/%Y").date() for d in match.groups())
            if start != end:
                raise ValueError(
                    f"{source_name}: Date Range {start} to {end} spans more than one day; "
                    "the day-replace load expects a single day."
                )
            return start
    raise ValueError(f"{source_name}: no 'Date Range: mm/dd/yyyy to mm/dd/yyyy' line in the footer.")


def parse_totals(totals):
    """{label without trailing colon: int} for the totals rows that carry a value.

    A label with a blank value (the zero-line file leaves the counts blank)
    counts as 0; "Grand Totals" is a heading and is skipped.
    """
    parsed = {}
    for label, value in zip(totals[TOTALS_LABEL_COLUMN], totals[TOTALS_VALUE_COLUMN]):
        label = label.rstrip(":").strip()
        if label.lower() == "grand totals":
            continue
        parsed[label] = 0 if pd.isna(value) else int(float(value))
    return parsed


def check_totals(df, totals, source_name):
    """Compare Medline's totals with the lines read; return (warnings, alerts).

    *df* holds one row per order line (after ``split_lots``). A "# of Order
    Lines" total that differs from the line count (a cut-short attachment, a
    changed layout, or a new way of repeating lines) is a warning that is also
    an alert, i.e. emailed; the load goes ahead. The other totals are checked
    against what they appear to count (verified on the 2026-10-07 sample) and
    only warn, since their exact definition is Medline's.
    """
    expected = {
        ORDER_LINES_TOTAL: len(df),
        "# of PO's": df["PO#"].nunique(),
        "# of Medline Orders": df["Order #"].nunique(),
        "# of Bad / Rejected Lines": int(df["Bad Line"].eq("YES").sum()),
    }
    warnings, alerts = [], []
    for label, actual in expected.items():
        if label not in totals:
            warnings.append(f"Warning: {source_name} has no '{label}' total to check")
        elif totals[label] != actual:
            if label == ORDER_LINES_TOTAL:
                alerts.append(
                    f"Warning: {source_name} file line {totals[label]}, {actual} line were read"
                )
                warnings.append(alerts[-1])
                continue
            warnings.append(
                f"Warning: {source_name} total '{label}' is {totals[label]}, rows give {actual}"
            )
    for w in warnings:
        print(w)
    if not warnings:
        print(f"Totals check passed — {len(df)} order lines.")
    return warnings, alerts


def split_lots(df, source_name):
    """Fold a line shipped from several lots into one row; list every lot.

    Medline writes a shipped line once per lot, repeating every column but Lot
    and Expiration. Returns (lines, lots):

    - lines: one row per order line, in file order. A multi-lot line keeps its
      other values; Lot becomes its lots joined with " | " (earliest
      expiration first) and Expiration the earliest.
    - lots: one row per lot for every row with a Lot, single-lot lines
      included, numbered Lot Seq 1..n within the line by expiration then lot,
      in LOT_LOAD_COLUMNS.

    A line repeated with anything else differing is not a lot split: raise, as
    a repeated key always did. Rows missing a key are left for validate_keys.
    """
    df = df.drop_duplicates().reset_index(drop=True)
    by_expiry = (
        df.assign(_expiry=pd.to_datetime(df[EXPIRATION_COLUMN], format="mixed"))
        .sort_values([*LINE_KEY, "_expiry", LOT_COLUMN], na_position="last", kind="stable")
    )
    lots = by_expiry[by_expiry[LOT_COLUMN].notna()].copy()
    lots[LOT_SEQ_COLUMN] = lots.groupby(LINE_KEY).cumcount() + 1
    lots = lots[LOT_LOAD_COLUMNS].reset_index(drop=True)

    keyed = df[LINE_KEY].notna().all(axis=1)
    repeated = keyed & df.duplicated(LINE_KEY, keep=False)
    if not repeated.any():
        return df, lots

    others = [c for c in df.columns if c not in (LOT_COLUMN, EXPIRATION_COLUMN)]
    variants = df.loc[repeated, others].drop_duplicates()
    clash = variants.duplicated(LINE_KEY, keep=False)
    if clash.any():
        clash_keys = variants.loc[clash, LINE_KEY].drop_duplicates()
        examples = [tuple(k) for k in clash_keys.head(10).itertuples(index=False)]
        raise ValueError(
            f"{source_name}: {len(clash_keys)} order line(s) {LINE_KEY} repeat with values "
            f"other than Lot / Expiration differing (not a lot split), e.g. {examples}."
        )

    combined = (
        by_expiry[by_expiry.index.isin(df.index[repeated])]
        .groupby(LINE_KEY)
        .agg(**{
            LOT_COLUMN: (LOT_COLUMN, _join_lots),
            # "first" skips blanks, and the rows are in expiration order
            EXPIRATION_COLUMN: (EXPIRATION_COLUMN, "first"),
        })
    )
    lines = df[~(keyed & df.duplicated(LINE_KEY))].reset_index(drop=True)
    multi = pd.MultiIndex.from_frame(lines[LINE_KEY]).isin(combined.index)
    for col in (LOT_COLUMN, EXPIRATION_COLUMN):
        lines.loc[multi, col] = (
            combined[col].reindex(pd.MultiIndex.from_frame(lines.loc[multi, LINE_KEY])).to_numpy()
        )
    print(
        f"NOTE: {multi.sum()} order line(s) shipped from more than one lot "
        f"({repeated.sum()} rows) — one row each, lots joined in '{LOT_COLUMN}'."
    )
    return lines, lots


def _join_lots(lots):
    lots = lots.dropna().drop_duplicates()
    return LOT_SEPARATOR.join(lots) if len(lots) else pd.NA


def validate_keys(df, source_name):
    """Fail if any order line is missing Order # / Order Line or repeats them."""
    missing_key = df[LINE_KEY].isna().any(axis=1)
    if missing_key.any():
        raise ValueError(
            f"{source_name}: {missing_key.sum()} row(s) are missing a key value {LINE_KEY} "
            f"(file row(s) {(df.index[missing_key] + 2).tolist()[:10]})."
        )
    dup = df.duplicated(subset=LINE_KEY, keep=False)
    if dup.any():
        dup_keys = df.loc[dup, LINE_KEY].drop_duplicates()
        examples = [tuple(k) for k in dup_keys.head(10).itertuples(index=False)]
        raise ValueError(
            f"{source_name}: key {LINE_KEY} is not unique — {len(dup_keys)} key(s) "
            f"repeated across {dup.sum()} rows, e.g. {examples}."
        )
    print(f"Key check passed — {len(df)} unique {LINE_KEY}.")


def add_derived_columns(df):
    """Append PO and POLine (from 'PO#' / 'PO Line #'), ShipTo and SoldTo."""
    df = medline_keys.add_po_columns(df, "PO#", "PO Line #")
    df = medline_keys.add_ship_to_column(df, "Ship-To #")
    return medline_keys.add_sold_to_column(df, "Sold-To #")


def coerce_types(df):
    """Dates -> date, times -> time, quantities and minutes -> int.

    Parsing is strict so an unexpected format fails the run (and triggers the
    failure email) rather than silently loading blanks.
    """
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], format="mixed").dt.date
    for col in TIME_COLUMNS:
        df[col] = pd.to_datetime(df[col], format="mixed").dt.time
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col]).round().astype("Int64")
    return df


def coerce_lot_types(lots):
    """Order Create Date / Expiration -> date, Lot Seq -> int."""
    for col in ("Order Create Date", EXPIRATION_COLUMN):
        lots[col] = pd.to_datetime(lots[col], format="mixed").dt.date
    lots[LOT_SEQ_COLUMN] = lots[LOT_SEQ_COLUMN].astype("Int64")
    return lots


def prepare(file_path):
    """Read, check and cleanse ONE export into the load shape.

    Returns a ``Prepared``: *rows* (one per order line, LOAD_COLUMNS), *lots*
    (one per lot shipped, LOT_LOAD_COLUMNS), *report_date* (from the footer,
    so known even for a file with no order lines), *warnings* and *alerts*.
    """
    source_name = os.path.basename(file_path)
    df, totals, footer, column_warnings = read_order_detail_file(file_path)
    report_date = parse_report_date(footer, source_name)
    print(f"Read {len(df)} row(s) from {source_name} (Report Date {report_date}).")
    df = medline_keys.strip_order_line(df, "Order Line")
    df, lots = split_lots(df, source_name)
    warnings, alerts = check_totals(df, parse_totals(totals), source_name)
    warnings = column_warnings + warnings
    validate_keys(df, source_name)
    df = add_derived_columns(df)
    df = coerce_types(df)
    lots = coerce_lot_types(lots)
    df[REPORT_DATE_COLUMN] = report_date
    df[SOURCE_FILE_COLUMN] = source_name
    return Prepared(df[LOAD_COLUMNS], lots, report_date, warnings, alerts)
