import re

import numpy as np
import pandas as pd


def read_pbo_file(file_path):
    """Read the Medline PBO Excel file and deduplicate."""
    df = pd.read_excel(file_path, engine="calamine", dtype=str)
    df = df.drop_duplicates()
    return df


def validate_columns(df, required_columns):
    """Raise ValueError if any *required_columns* are missing from *df*."""
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Validation Failed. Missing columns: {missing}")
    print("Column validation successful.")
    return True


def apply_jesse_selection(df, risk_levels, duration_threshold):
    """Mark rows matching risk / duration / forecast criteria with 'x'.

    Parameters
    ----------
    risk_levels : list[str]
        e.g. ["High", "Severe"]
    duration_threshold : float
        Minimum duration value (inclusive).
    """
    pattern = "|".join(risk_levels)
    risk_mask = df["Risk Level"].fillna("").str.contains(pattern, case=False)
    duration_mask = df["Duration"].astype(float) >= duration_threshold
    forecast_mask = df["Forecast Qty"].astype(float) != 0

    combined = risk_mask & duration_mask & forecast_mask
    df["Jesse Selection"] = np.where(combined, "x", "")

    selected = (df["Jesse Selection"] == "x").sum()
    print(f"Selection applied. {selected} items marked as 'x'.")
    return df


# ── UOM extraction ────────────────────────────

_PKG_PATTERN = re.compile(r"(\d+(?:\.\d+)?)\s+([A-Z]+)/([A-Z]+)")


def _extract_pkgstr(row):
    item_id = row["Medline Item"]
    pkg_str = row["Packaging String"]

    if not pkg_str or pd.isna(pkg_str) or not isinstance(pkg_str, str):
        return [(item_id, "", "", "")]

    matches = _PKG_PATTERN.findall(pkg_str)
    return [(item_id, m[2], m[1], float(m[0])) for m in matches]


def extract_uom_table(df):
    """Parse 'Packaging String' into a UOM conversion DataFrame."""
    records = []
    for _, row in df.iterrows():
        records.extend(_extract_pkgstr(row))

    return pd.DataFrame(
        records,
        columns=["Medline Item", "Convert to UOM", "UOM", "Conversion Factor"],
    )
