import re

import numpy as np
import pandas as pd


def read_pbo_file(file_path, required_columns=None):
    """Read the Medline PBO Excel file and deduplicate report rows."""
    df = pd.read_excel(file_path, engine="calamine", dtype=str, skiprows=1)
    if required_columns is not None:
        missing = [c for c in required_columns if c not in df.columns]
        if missing:
            raise ValueError(f"Validation Failed. Missing columns: {missing}")
    df = df.drop_duplicates(subset=required_columns)
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


def _denominator_matches_suom(denominator, suom):
    """Guard against truncated packaging strings.

    The 'Packaging String' is sometimes cut off mid-entry, turning a complete
    ``NN AA/BB`` into something whose denominator ``BB`` is incomplete
    (e.g. ``25 RL/CS`` truncated to ``25 RL/C``). A valid (possibly truncated)
    denominator is always a non-empty prefix of the item's *suom*, so:

    - ``25 RL/C``  with SUOM ``CS`` -> ``"CS".startswith("C")``  -> kept
    - ``25 AA/B``  with SUOM ``CS`` -> ``"CS".startswith("B")``  -> dropped
    - ``25`` / ``25 R`` (no ``/BB``) never match the pattern, so never reach here
    """
    return bool(suom) and bool(denominator) and suom.startswith(denominator)


def _extract_pkgstr(row):
    item_id = row["Medline Item"]
    pkg_str = row["Packaging String"]

    if not pkg_str or pd.isna(pkg_str) or not isinstance(pkg_str, str):
        return [(item_id, "", "", "")]

    suom = row["SUOM"]
    suom = suom.strip().upper() if isinstance(suom, str) else ""

    records = []
    for factor, uom, denominator in _PKG_PATTERN.findall(pkg_str):
        if _denominator_matches_suom(denominator, suom):
            # Emit the full SUOM, not the parsed (possibly truncated) denominator.
            records.append((item_id, suom, uom, float(factor)))
    return records


def extract_uom_table(df):
    """Parse 'Packaging String' into a UOM conversion DataFrame."""
    records = []
    for _, row in df.iterrows():
        records.extend(_extract_pkgstr(row))

    uom_df = pd.DataFrame(
        records,
        columns=["Medline Item", "Convert to UOM", "UOM", "Conversion Factor"],
    )
    # The source data can list the same item more than once, producing
    # duplicate conversion rows; collapse them.
    return uom_df.drop_duplicates(ignore_index=True)
