"""Stateful side effects for the Allocation report.

Upserts this week's rows into the report's own helper tables, so later runs can
read prior weeks back to compute risk-of-loss. The target schema/table names and
the engine are supplied by the pipeline from ``config['persistence']`` so the
helper tables can move to a different database without code changes.
"""

from datetime import date

import pandas as pd
from sqlalchemy import text

from reports.medline_allocation.weeks import get_year_week


UNSPSC_COLS = [
    "Manufacturer Name", "Manufacturer Item #",
    "Material #", "SalesUOM Code", "Material Qty",
    "UNSPSC Level 3 Number", "UNSPSC Level 3 Name",
]

TRACKING_COLS = [
    "Allocation Period Start Dt", "Allocation Period End Dt",
    "SoldTo #", "Material #", "SalesUOM Code", "Material Qty",
    "Weekly Allocation Qty", "C Group", "Qty available to order",
    "Allocation Qty", "Ordered Qty",
    "Manufacturer Name", "Manufacturer Item #", "YearWeek",
]


def _clean_int(value):
    if pd.isna(value):
        return None
    return int(value)


def _clean_text(value):
    if pd.isna(value):
        return None
    return str(value).strip()


def _unspsc_upsert_sql(schema, table):
    # schema/table come from config (trusted), bracket-quoted as identifiers.
    return text(f"""
    UPDATE [{schema}].[{table}]
    SET
        [Manufacturer Name] = :manufacturer_name,
        [Manufacturer Item #] = :manufacturer_item,
        [UNSPSC Level 3 Number] = :unspsc_level_3_number,
        [UNSPSC Level 3 Name] = :unspsc_level_3_name,
        [update stamp] = GETDATE()
    WHERE
          [Material #] = :material
      AND [SalesUOM Code] = :salesuom
      AND [Material Qty] = :material_qty;

    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO [{schema}].[{table}]
        (
            [Manufacturer Name], [Manufacturer Item #],
            [Material #], [SalesUOM Code], [Material Qty],
            [UNSPSC Level 3 Number], [UNSPSC Level 3 Name],
            [create stamp], [update stamp]
        )
        VALUES
        (
            :manufacturer_name, :manufacturer_item,
            :material, :salesuom, :material_qty,
            :unspsc_level_3_number, :unspsc_level_3_name,
            GETDATE(), GETDATE()
        );
    END;
    """)


def upsert_unspsc(df, engine, schema, table, unspsc_cols=UNSPSC_COLS):
    """Upsert the manufacturer/UNSPSC reference rows keyed by material+UOM+qty."""
    upsert_sql = _unspsc_upsert_sql(schema, table)
    df_load = df[unspsc_cols]

    pk_cols = ["Material #", "SalesUOM Code", "Material Qty"]
    df_load = df_load.dropna(subset=pk_cols).copy()
    for col in pk_cols:
        df_load[col] = df_load[col].astype(str).str.strip()
    df_load = df_load[
        (df_load["Material #"] != "")
        & (df_load["SalesUOM Code"] != "")
        & (df_load["Material Qty"] != "")
    ].copy()
    df_load = df_load.drop_duplicates(subset=pk_cols, keep="first")

    records = [
        {
            "manufacturer_name": _clean_text(row["Manufacturer Name"]),
            "manufacturer_item": _clean_text(row["Manufacturer Item #"]),
            "material": _clean_text(row["Material #"]),
            "salesuom": _clean_text(row["SalesUOM Code"]),
            "material_qty": _clean_int(row["Material Qty"]),
            "unspsc_level_3_number": _clean_text(row["UNSPSC Level 3 Number"]),
            "unspsc_level_3_name": _clean_text(row["UNSPSC Level 3 Name"]),
        }
        for _, row in df_load.iterrows()
    ]

    with engine.begin() as conn:
        for rec in records:
            conn.execute(upsert_sql, rec)

    print(f"Upserted {len(records)} rows into [{schema}].[{table}] (UNSPSC).")
    return len(records)


def _tracking_upsert_sql(schema, table):
    return text(f"""
    UPDATE [{schema}].[{table}]
    SET
        [Allocation Period Start Dt] = :allocation_start_dt,
        [Allocation Period End Dt] = :allocation_end_dt,
        [Material Qty] = :material_qty,
        [Weekly Allocation Qty] = :weekly_allocation_qty,
        [Qty available to order] = :qty_available_to_order,
        [Allocation Qty] = :allocation_qty,
        [Ordered Qty] = :ordered_qty,
        [Manufacturer Name] = :manufacturer_name,
        [Manufacturer Item #] = :manufacturer_item,
        [update stamp] = GETDATE()
    WHERE [YearWeek] = :yearweek
      AND [SoldTo #] = :soldto
      AND [Material #] = :material
      AND [SalesUOM Code] = :salesuom
      AND [C Group] = :c_group;

    IF @@ROWCOUNT = 0
    BEGIN
        INSERT INTO [{schema}].[{table}]
        (
            [Allocation Period Start Dt], [Allocation Period End Dt],
            [SoldTo #], [Material #], [SalesUOM Code], [Material Qty],
            [Weekly Allocation Qty], [C Group], [Qty available to order],
            [Allocation Qty], [Ordered Qty],
            [Manufacturer Name], [Manufacturer Item #], [YearWeek],
            [create stamp], [update stamp]
        )
        VALUES
        (
            :allocation_start_dt, :allocation_end_dt,
            :soldto, :material, :salesuom, :material_qty,
            :weekly_allocation_qty, :c_group, :qty_available_to_order,
            :allocation_qty, :ordered_qty,
            :manufacturer_name, :manufacturer_item, :yearweek,
            GETDATE(), GETDATE()
        );
    END;
    """)


def upsert_tracking(df, engine, schema, table, cols_to_load=TRACKING_COLS):
    """Upsert this week's allocation rows; return the current 'YYYYWW' string."""
    current_yearweek = get_year_week(date.today())
    upsert_sql = _tracking_upsert_sql(schema, table)

    df_load = df[cols_to_load]

    pk_cols = ["YearWeek", "SoldTo #", "Material #", "SalesUOM Code", "C Group"]
    required_pk_cols = ["YearWeek", "SoldTo #", "Material #"]
    nullable_pk_cols = ["SalesUOM Code", "C Group"]

    df_load = df_load.dropna(subset=required_pk_cols).copy()
    for col in required_pk_cols:
        df_load[col] = df_load[col].astype(str).str.strip()
    df_load = df_load[
        (df_load["YearWeek"] != "")
        & (df_load["SoldTo #"] != "")
        & (df_load["Material #"] != "")
    ].copy()
    for col in nullable_pk_cols:
        df_load[col] = df_load[col].fillna("").astype(str).str.strip()
    df_load = df_load.drop_duplicates(subset=pk_cols, keep="first")

    records = [
        {
            "allocation_start_dt": row["Allocation Period Start Dt"],
            "allocation_end_dt": row["Allocation Period End Dt"],
            "soldto": row["SoldTo #"],
            "material": _clean_text(row["Material #"]),
            "salesuom": _clean_text(row["SalesUOM Code"]),
            "material_qty": _clean_int(row["Material Qty"]),
            "weekly_allocation_qty": _clean_int(row["Weekly Allocation Qty"]),
            "c_group": row["C Group"],
            "qty_available_to_order": _clean_int(row["Qty available to order"]),
            "allocation_qty": _clean_int(row["Allocation Qty"]),
            "ordered_qty": _clean_int(row["Ordered Qty"]),
            "manufacturer_name": _clean_text(row["Manufacturer Name"]),
            "manufacturer_item": _clean_text(row["Manufacturer Item #"]),
            "yearweek": row["YearWeek"],
        }
        for _, row in df_load.iterrows()
    ]

    with engine.begin() as conn:
        for rec in records:
            conn.execute(upsert_sql, rec)

    print(f"Upserted {len(records)} rows into [{schema}].[{table}] for YearWeek {current_yearweek}")
    return current_yearweek
