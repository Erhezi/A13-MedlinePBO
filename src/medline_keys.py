"""Derived match keys shared by the Medline order feeds (Fill-Kill, Order Timing).

Medline's exports carry our PO and ship-to numbers in their own shape:

- ``PO``     — the leading 10 digits of the PO number ("2600301531-STPO" -> "2600301531"),
               which is how Montefiore systems key a PO
- ``POLine`` — the PO line without leading zeros, kept as text ("000003" -> "3"),
               the format of POLine in PLMPreprocessorShared's PO tables
- ``ShipTo`` — the ship-to number without leading zeros, kept as text
               ("0002464430" -> "2464430"); an all-zero value keeps one "0"

Values that do not fit are left blank and counted in a WARNING line in the log
rather than failing the run.
"""


def add_po_columns(df, po_column, po_line_column):
    """Append PO (leading 10 digits of *po_column*) and POLine (*po_line_column* sans zeros)."""
    df["PO"] = df[po_column].str.extract(r"^(\d{10})", expand=False)
    digits = df[po_line_column].str.fullmatch(r"\d+").fillna(False).astype(bool)
    df["POLine"] = _strip_leading_zeros(df[po_line_column].where(digits))

    bad_po = df["PO"].isna() & df[po_column].notna()
    if bad_po.any():
        print(
            f"WARNING: {bad_po.sum()} row(s) have a '{po_column}' that does not start with "
            f"10 digits; PO left blank. e.g. {df.loc[bad_po, po_column].head(3).tolist()}"
        )
    bad_line = df["POLine"].isna() & df[po_line_column].notna()
    if bad_line.any():
        print(
            f"WARNING: {bad_line.sum()} row(s) have a non-numeric '{po_line_column}'; POLine "
            f"left blank. e.g. {df.loc[bad_line, po_line_column].head(3).tolist()}"
        )
    return df


def add_ship_to_column(df, ship_to_column):
    """Append ShipTo: *ship_to_column* with its leading zeros removed."""
    df["ShipTo"] = _strip_leading_zeros(df[ship_to_column])
    return df


def _strip_leading_zeros(series):
    """Drop leading zeros, keeping at least one character ("000" -> "0")."""
    return series.str.replace(r"^0+(?=.)", "", regex=True)
