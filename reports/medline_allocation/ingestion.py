"""Ingestion & cleansing for the Medline Allocation report.

The source is a system-exported ``.xls`` Product Allocation Report with a footer
of summary rows (blank ``SoldTo #``) that must be trimmed before processing.

An export only covers allocation weeks whose *start* date falls in the month it
was generated for, so a run whose ISO week started in the previous month must
stack the companion "Previous Month" export underneath the current-month one —
see ``read_allocation_files`` and ``weeks.week_starts_in_previous_month``.
"""

import numpy as np
import pandas as pd


def read_allocation_file(file_path):
    """Read the Product Allocation .xls export as all-string columns."""
    return pd.read_excel(file_path, engine="calamine", dtype=str)


def read_allocation_files(file_paths, required_columns):
    """Read, validate, and footer-trim each export in *file_paths*, then stack.

    Each export carries its own summary footer, so trimming happens per file and
    the frames are concatenated afterwards with a fresh index (``Dummy ID`` and
    ``remove_footer`` both depend on a clean 0..n-1 index). Later frames are
    aligned to the first frame's columns so a schema change on one side cannot
    silently introduce all-blank columns.

    The exports are disjoint by construction — a week's start date falls in
    exactly one month — so the stack needs no de-duplication.
    """
    frames = []
    for file_path in file_paths:
        df = read_allocation_file(file_path)
        validate_columns(df, required_columns)
        frames.append(remove_footer(df))

    if len(frames) == 1:
        return frames[0].reset_index(drop=True)

    base_columns = frames[0].columns
    aligned = [frames[0]] + [f.reindex(columns=base_columns) for f in frames[1:]]
    stacked = pd.concat(aligned, ignore_index=True)
    row_counts = " + ".join(str(len(f)) for f in frames)
    print(f"Stacked {len(frames)} files: {row_counts} = {len(stacked)} rows.")
    return stacked


def validate_columns(df, required_columns):
    """Raise ValueError if any *required_columns* are missing from *df*."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Validation Failed. Missing columns: {missing}")
    print("Column validation successful.")
    return True


def remove_footer(df, key_column="SoldTo #"):
    """Drop the trailing summary/footer rows (those with a blank key column)."""
    first_footer_idx = df[df[key_column].isna()].index.min()
    if pd.notna(first_footer_idx):
        df = df.loc[: first_footer_idx - 1]
    print("footer removed.")
    return df


def apply_jesse_selection(df, c_group="C1605"):
    """Mark rows in *c_group* whose allocation period contains today with 'x'."""
    today = pd.Timestamp.today().normalize()
    df["Allocation Period Start Dt"] = pd.to_datetime(
        df["Allocation Period Start Dt"], errors="coerce"
    )
    df["Allocation Period End Dt"] = pd.to_datetime(
        df["Allocation Period End Dt"], errors="coerce"
    )

    c_group_mask = df["C Group"] == c_group
    period_mask = (
        (df["Allocation Period Start Dt"] <= today)
        & (df["Allocation Period End Dt"] >= today)
    )
    combined_mask = c_group_mask & period_mask
    df["Jesse Selection"] = np.where(combined_mask, "x", "")

    selected_count = (df["Jesse Selection"] == "x").sum()
    print(f"Selection applied. {selected_count} items marked as 'x'.")
    return df


def make_year_week(df):
    """Add an ISO 'YearWeek' column derived from the allocation period start."""
    iso = df["Allocation Period Start Dt"].dt.isocalendar()
    df["YearWeek"] = iso["year"].astype(str) + iso["week"].astype(str).str.zfill(2)
    print("Year Week Applied.")
    return df


def clean_int_cols(df, int_cols):
    """Coerce the configured quantity columns to int (blank/NaN -> 0)."""
    for col in int_cols:
        df[col] = df[col].apply(lambda x: 0 if pd.isna(x) else int(str(x).strip()))
    print("integer columns are converted.")
    return df
