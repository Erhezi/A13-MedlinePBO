"""Data transformation & calculation pipeline.

Every public function in this module takes DataFrames in and returns a
DataFrame out, keeping the pipeline testable and composable.
"""

from datetime import datetime

import numpy as np
import pandas as pd


# ── Step 1: build helper tables ─────────────────────────────────────

def build_ehc(df_inv, df_usage, df_long_desc):
    """Inventory enriched with usage rates and long descriptions."""
    return (
        df_inv[
            ["VendorItem", "Item", "AvailableQty", "OnOrderQty",
             "DefaultBuyUOM", "BuyUOMMultiplier", "StockUOM", "report stamp"]
        ]
        .merge(df_usage[["Item", "AverageDailyIssueOut"]], on="Item", how="left")
        .merge(df_long_desc, on="Item", how="left")
    )


def build_substitutes(df_plmlink, df_inv, df_usage, df_long_desc):
    """Build substitute-item table with inventory, usage, and descriptions."""
    df_sub = (
        df_plmlink
        .merge(
            df_inv[["VendorItem", "Item", "AvailableQty", "OnOrderQty",
                     "DefaultBuyUOM", "BuyUOMMultiplier", "StockUOM"]],
            left_on="Replace Item", right_on="Item",
            suffixes=("", "_a"), how="left",
        )
        .merge(
            df_usage[["Item", "AverageDailyIssueOut"]],
            left_on="Replace Item", right_on="Item",
            suffixes=("", "_b"), how="left",
        )
        .merge(
            df_long_desc,
            left_on="Replace Item", right_on="Item",
            suffixes=("", "_c"), how="left",
        )
    )
    df_sub.drop(columns=["Item_a", "Item_b", "Item_c"], inplace=True)

    # DIOH for substitute items
    df_sub["DIOH"] = df_sub["AvailableQty"] / df_sub["AverageDailyIssueOut"]
    df_sub["DIOH"] = df_sub.apply(
        lambda r: 0.0 if r["AvailableQty"] == 0 else r["DIOH"], axis=1
    )
    df_sub["With Sub"] = "With Sub"
    return df_sub


# ── Step 2: merge PBO with inventory ────────────────────────────────

def merge_inventory(df_pbo, df_ehc):
    """Left-join the PBO file to the enriched inventory on Medline Item."""
    df_pbo = df_pbo.copy()
    df_pbo["Dummy ID"] = list(range(1, len(df_pbo) + 1))
    return df_pbo.merge(
        df_ehc,
        left_on="Medline Item", right_on="VendorItem",
        how="left", indicator=True,
    )


def calculate_dioh_metrics(df_m, review_threshold):
    """Compute DIOH, Days to Get Well, Flag, Matched, and No-Issue-Out cols."""
    df = df_m.copy()

    df["DIOH"] = df["AvailableQty"] / df["AverageDailyIssueOut"]
    df["DIOH"] = df.apply(
        lambda r: 0.0 if r["AvailableQty"] == 0 else r["DIOH"], axis=1
    )

    df["Days to Get Well"] = (
        pd.to_datetime(df["Est Delivery Date"]) - datetime.today()
    ).dt.days
    df["Get Well - DIOH"] = df["Days to Get Well"] - df["DIOH"]

    df["Flag"] = df["Get Well - DIOH"].apply(
        lambda x: "Review" if x >= review_threshold else "Okay"
    )
    df["Matched IMDCSTRM"] = df["_merge"].apply(
        lambda x: "Matched" if x == "both" else "Not Matched"
    )
    df["No Issue Out (Last 365 Days)"] = df[["_merge", "AverageDailyIssueOut"]].apply(
        lambda r: "No Issue Out"
        if (r["_merge"] == "both" and pd.isnull(r["AverageDailyIssueOut"]))
        else "",
        axis=1,
    )
    return df


# ── Step 3: substitutes & type coercion ─────────────────────────────

def merge_substitutes(df_m, df_sub):
    """Merge substitute data and coerce date / numeric columns."""
    df = df_m.merge(df_sub, on="Item", how="left", suffixes=("", "_Repl."))

    for col in ["Est Depletion Date", "Est Available Date", "Est Delivery Date"]:
        df[col] = pd.to_datetime(df[col])

    for col in ["MIOH", "Forecast Qty", "Duration"]:
        df[col] = df[col].astype(float)

    return df


# ── Step 4: item-group aggregation ──────────────────────────────────

def aggregate_item_groups(df_msub, df_plmusage, review_threshold):
    """Aggregate inventory across item groups and compute group-level DIOH."""
    has_ig = df_msub[~df_msub["Item Group"].isnull()]

    item = has_ig[["Item", "Item Group", "AvailableQty"]].drop_duplicates()
    replace_item = has_ig[
        ["Replace Item", "Item Group", "AvailableQty_Repl."]
    ].drop_duplicates()
    replace_item.columns = ["Item", "Item Group", "AvailableQty"]

    pre_agg = pd.concat([item, replace_item])
    qty_by_ig = pre_agg.groupby("Item Group").agg({"AvailableQty": "sum"})

    df_ig_qty = qty_by_ig.merge(df_plmusage, on="Item Group", how="left")
    df_ig_qty["DIOH"] = df_ig_qty["AvailableQty"] / df_ig_qty["rolling_daily_avg_7"]
    df_ig_qty.columns = [
        "Item Group", "AvailableQty_ig", "AverageDailyIssueOut_ig", "DIOH_ig"
    ]

    ig_days = has_ig[["Item", "Item Group", "Days to Get Well"]].drop_duplicates()
    df_ig = df_ig_qty.merge(ig_days, on="Item Group", how="left")
    df_ig["Get Well - DIOH_ig"] = df_ig["Days to Get Well"] - df_ig["DIOH_ig"]
    df_ig.drop(columns=["Days to Get Well"], inplace=True)
    df_ig["Flag_ig"] = df_ig["Get Well - DIOH_ig"].apply(
        lambda x: "Review" if x >= review_threshold else "Okay"
    )
    return df_ig


def build_full_dataset(df_msub, df_ig):
    """Merge the item-group metrics back onto the main dataset."""
    return df_msub.merge(df_ig, on=["Item", "Item Group"], how="left")


# ── Step 5: review recommendations ──────────────────────────────────

def calculate_review_recommendations(df_full, review_threshold):
    """Filter to 'Review' items and compute target coverage & order qty."""
    cols = [
        "Medline Item", "Packaging String", "Item", "Flag",
        "With Sub", "Flag_ig", "Item Group", "Replace Item",
        "AverageDailyIssueOut", "AverageDailyIssueOut_ig",
        "DefaultBuyUOM", "BuyUOMMultiplier",
        "DefaultBuyUOM_Repl.", "BuyUOMMultiplier_Repl.",
        "Get Well - DIOH", "Get Well - DIOH_ig",
    ]
    df = df_full[df_full["Flag"] == "Review"][cols].copy()

    df["Target Coverage"] = df["Get Well - DIOH"] - review_threshold
    df["Target Coverage_ig"] = df["Get Well - DIOH_ig"] - review_threshold
    df["Target Coverage_ig"] = df["Target Coverage_ig"].apply(
        lambda x: x if (x > 0 or pd.isnull(x)) else 0
    )

    df["Target Item Qty (EA)"] = df["Target Coverage"] * df["AverageDailyIssueOut"]
    df["Target Item Qty (EA)_ig"] = (
        df["Target Coverage_ig"] * df["AverageDailyIssueOut_ig"]
    )
    df["Recommended Order Qty (EA)"] = df["Target Item Qty (EA)_ig"].fillna(
        df["Target Item Qty (EA)"]
    )

    df["Recomended Order Qty (in BuyUOM)"] = np.ceil(
        df["Recommended Order Qty (EA)"] / df["BuyUOMMultiplier"]
    )
    df["Recomended Order Qty (in BuyUOM)_Repl."] = np.ceil(
        df["Recommended Order Qty (EA)"] / df["BuyUOMMultiplier_Repl."]
    )
    return df


# ── Step 6: UOM conversions ─────────────────────────────────────────

def build_uom_conversions(uom_df):
    """Normalise the raw UOM table into a full conversion-factor map.

    Returns *medline_cf_df* used downstream for UOM validation and
    alternative-UOM calculations.
    """
    uom = uom_df.copy()
    uom["Distributor Base UOM"] = uom["Convert to UOM"].apply(
        lambda x: "CA" if x == "CS" else x
    )
    uom["Conversion Factor"] = uom["Conversion Factor"].apply(
        lambda x: float(x) if x != "" else 0.0
    )
    uom = uom[uom["Distributor Base UOM"] != ""].copy()

    # group by conversion factor + base UOM
    uom_t = (
        uom.groupby(["Medline Item", "Conversion Factor", "Distributor Base UOM"])
        .apply(lambda g: ",".join(g["UOM"]))
        .reset_index()
    )
    uom_t.columns = ["Medline Item", "Conversion Factor", "Distributor Base UOM", "to UOM"]
    uom_t.sort_values(
        by=["Medline Item", "Conversion Factor"], ascending=[True, False], inplace=True
    )

    # multi-level conversion
    merged = pd.merge(
        uom_t, uom_t, on=["Medline Item", "Distributor Base UOM"],
        suffixes=("_bulk", "_small"),
    )
    result = merged[merged["Conversion Factor_small"] > merged["Conversion Factor_bulk"]].copy()
    result["New Factor"] = result["Conversion Factor_small"] / result["Conversion Factor_bulk"]

    conversion_df = result[
        ["Medline Item", "New Factor", "to UOM_bulk", "to UOM_small"]
    ].rename(columns={
        "to UOM_bulk": "Distributor Base UOM",
        "to UOM_small": "to UOM",
        "New Factor": "Conversion Factor",
    })
    conversion_df["Distributor Base UOM"] = conversion_df["Distributor Base UOM"].str.split(",")
    expanded = conversion_df.explode("Distributor Base UOM").reset_index(drop=True)

    extended = pd.concat([uom_t, expanded], ignore_index=True)
    extended.sort_values(
        by=["Medline Item", "Conversion Factor", "Distributor Base UOM"], inplace=True
    )

    # compact aggregation
    compact = (
        extended.groupby(["Medline Item", "Conversion Factor", "to UOM"])
        .apply(lambda g: ",".join(g["Distributor Base UOM"]))
        .reset_index()
    )
    compact.columns = ["Medline Item", "Conversion Factor", "to UOM", "from UOM"]

    medline_cf_df = extended.merge(
        compact, on=["Medline Item", "Conversion Factor", "to UOM"]
    )
    return medline_cf_df


# ── Step 7: alternative UOM recommendations ─────────────────────────

def _remove_default_uom(row):
    from_uom_str = row["from UOM"]
    default_uom = row["DefaultBuyUOM"]
    if pd.isna(from_uom_str) or pd.isna(default_uom):
        return from_uom_str
    uom_list = [u.strip() for u in str(from_uom_str).split(",") if u.strip()]
    target = str(default_uom).strip()
    return ",".join(u for u in uom_list if u != target)


def apply_uom_alternatives(df_review, medline_cf_df):
    """Compute alternative-UOM order quantities for review items.

    Returns *df_review_to_merge* — the set of columns to join back
    onto the full dataset.
    """
    # UOM validation
    uom_validation = df_review.merge(
        medline_cf_df,
        left_on=["Medline Item", "BuyUOMMultiplier", "DefaultBuyUOM"],
        right_on=["Medline Item", "Conversion Factor", "Distributor Base UOM"],
        how="left",
    )[
        ["Medline Item", "DefaultBuyUOM", "BuyUOMMultiplier",
         "Recommended Order Qty (EA)", "Recomended Order Qty (in BuyUOM)",
         "Distributor Base UOM", "Conversion Factor", "from UOM", "to UOM"]
    ].copy()

    unresolved = set(
        uom_validation[uom_validation["Distributor Base UOM"].isnull()]["Medline Item"]
    )
    if unresolved:
        print(f"UOM items needing further resolution: {unresolved}")
    else:
        print("UOM validation: all good.")

    # UOM prep & split into same / alt
    uom_prep = df_review.merge(medline_cf_df, on="Medline Item", how="left")[
        ["Medline Item", "DefaultBuyUOM", "BuyUOMMultiplier",
         "Recommended Order Qty (EA)", "Recomended Order Qty (in BuyUOM)",
         "Distributor Base UOM", "Conversion Factor", "from UOM", "to UOM"]
    ].copy()
    uom_prep = uom_prep[~uom_prep["Distributor Base UOM"].isnull()].copy()

    same_mask = (
        (uom_prep["DefaultBuyUOM"] == uom_prep["Distributor Base UOM"])
        & (uom_prep["BuyUOMMultiplier"] == uom_prep["Conversion Factor"])
    )
    same_uom = uom_prep[same_mask].copy()

    alt_uom = uom_prep[~same_mask].copy()
    alt_uom.drop(columns=["Distributor Base UOM"], inplace=True)
    alt_uom.drop_duplicates(inplace=True)

    medline_alt = same_uom[
        ["Medline Item", "DefaultBuyUOM", "BuyUOMMultiplier", "to UOM",
         "Recommended Order Qty (EA)", "Recomended Order Qty (in BuyUOM)"]
    ].merge(
        alt_uom[["Medline Item", "from UOM", "to UOM", "Conversion Factor"]],
        on=["Medline Item", "to UOM"], how="left",
    )
    medline_alt["from UOM"] = medline_alt.apply(_remove_default_uom, axis=1)

    # alternative order quantities
    medline_alt["Recomnded Order Qty (In Alternative from UOM)"] = np.ceil(
        medline_alt["Recommended Order Qty (EA)"] / medline_alt["Conversion Factor"]
    )
    medline_alt["Alternative UOM Group"] = (
        medline_alt.groupby("Medline Item").cumcount() + 1
    )
    medline_alt["Alternative UOM Group"] = (
        "Group " + medline_alt["Alternative UOM Group"].astype(str)
    )

    # merge back to review
    df_review_calc = df_review.merge(
        medline_alt[
            ["Medline Item", "Alternative UOM Group", "to UOM", "from UOM",
             "Conversion Factor", "Recomnded Order Qty (In Alternative from UOM)"]
        ],
        on="Medline Item", how="left",
    )

    keep_cols = [
        "Medline Item",
        "Alternative UOM Group", "Conversion Factor",
        "Recomended Order Qty (in BuyUOM)",
        "Recomended Order Qty (in BuyUOM)_Repl.",
        "Recommended Order Qty (EA)",
        "Recomnded Order Qty (In Alternative from UOM)",
        "Target Coverage", "Target Coverage_ig",
        "Target Item Qty (EA)", "Target Item Qty (EA)_ig",
        "from UOM", "to UOM",
    ]
    return df_review_calc[keep_cols].copy()


# ── Step 8: final assembly ───────────────────────────────────────────

def assemble_output(df_full, df_review_to_merge, timestamp_value):
    """Join review recommendations back and attach the inventory timestamp."""
    df_output = df_full.merge(df_review_to_merge, on="Medline Item", how="left")
    df_output["Inventory Data As Of"] = str(timestamp_value)[:19]
    return df_output
