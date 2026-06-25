"""Transformations for the Medline Allocation report.

Ported from the MedlineAllocation prototype notebook. Self-contained: although
the inventory/substitute/item-group steps parallel the PBO report, the column
sets differ (Allocation keys off ``Material #``/``Item`` and carries no
``VendorItem``/``report stamp``/``Est *`` columns), so the logic is kept here
rather than shared.
"""

import numpy as np
import pandas as pd


def build_enrichment(df_inv, df_usage, df_long_desc):
    """Aggregate inventory + usage across locations and attach descriptions.

    Returns ``(df_inv_agg, df_usage_agg, df_ehc)``; ``df_ehc`` carries the
    ``In IPYCSTRM`` flag.
    """
    df_inv_agg = (
        df_inv.groupby(["Item", "DefaultBuyUOM", "BuyUOMMultiplier", "StockUOM"])
        .agg({"AvailableQty": "sum", "OnOrderQty": "sum"})
        .reset_index()
    )
    df_usage_agg = (
        df_usage.groupby(["Item"]).agg({"AverageDailyIssueOut": "sum"}).reset_index()
    )

    df_ehc = df_inv_agg.merge(df_usage_agg, on="Item", how="left").merge(
        df_long_desc, on="Item", how="left"
    )

    ipyc_set = set(df_inv[df_inv["Location"] == "IPYCSTRM"]["Item"])
    df_ehc["In IPYCSTRM"] = df_ehc["Item"].apply(
        lambda x: "Yes" if x in ipyc_set else "No"
    )
    return df_inv_agg, df_usage_agg, df_ehc


def build_substitutes(df_plmlink, df_inv_agg, df_usage_agg, df_long_desc):
    """Build the substitute-item table (inventory, usage, description, DIOH)."""
    df_sub = (
        df_plmlink.merge(
            df_inv_agg, left_on=["Replace Item"], right_on=["Item"],
            suffixes=("", "_a"), how="left",
        )
        .merge(
            df_usage_agg, left_on=["Replace Item"], right_on=["Item"],
            suffixes=("", "_b"), how="left",
        )
        .merge(
            df_long_desc, left_on=["Replace Item"], right_on=["Item"],
            suffixes=("", "_c"), how="left",
        )
    )
    df_sub.drop(columns=["Item_a", "Item_b", "Item_c"], inplace=True)

    df_sub["DIOH"] = df_sub["AvailableQty"] / df_sub["AverageDailyIssueOut"]
    df_sub["DIOH"] = df_sub.apply(
        lambda x: 0.0 if x["AvailableQty"] == 0 else x["DIOH"], axis=1
    )
    df_sub["With Sub"] = "With Sub"
    return df_sub


def resolve_item_and_ea(df_mini, df_collect_item):
    """Resolve the internal ``Item`` from ``Material #`` and add EA quantities."""
    df = df_mini.merge(
        df_collect_item, left_on=["Material #"], right_on=["VendorItem"], how="left"
    )

    df["Weekly Allocation Qty (EA)"] = df["Weekly Allocation Qty"] * df["Material Qty"]
    df["Weekly Allocation Qty (EA, by MPN)"] = (
        df.groupby("Manufacturer Item #")["Weekly Allocation Qty (EA)"]
        .transform("sum")
        .fillna(df["Weekly Allocation Qty (EA)"])
        .astype(int)
    )

    df["Qty available to order (EA)"] = df["Qty available to order"] * df["Material Qty"]
    df["Qty available to order (EA, by MPN)"] = (
        df.groupby("Manufacturer Item #")["Qty available to order (EA)"]
        .transform("sum")
        .fillna(df["Qty available to order (EA)"])
        .astype(int)
    )
    return df


def build_risk_of_loss(df_lost_alloc, current_yearweek, prior_weeks):
    """Compute current-week risk-of-loss from prior weeks of helper data.

    ``prior_weeks`` is an ordered list of the four prior 'YYYYWW' strings.
    """
    df_lost_alloc = df_lost_alloc.copy()
    df_lost_alloc["_key"] = df_lost_alloc.apply(
        lambda x: x["SoldTo #"] + x["Material #"] + x["SalesUOM Code"] + x["C Group"],
        axis=1,
    )
    curr_item_set = list(
        set(df_lost_alloc[df_lost_alloc["YearWeek"] == current_yearweek]["_key"])
    )

    weeks = [current_yearweek] + list(prior_weeks)
    grid = pd.MultiIndex.from_product(
        [weeks, curr_item_set], names=["YearWeek", "_key"]
    ).to_frame(index=False)

    df_grid = grid.merge(df_lost_alloc, on=["YearWeek", "_key"], how="left")
    df_grid.sort_values(by=["_key", "YearWeek"], inplace=True)

    df_grid["Weekly Allocation ffill"] = (
        df_grid.groupby(["_key"])["Weekly Allocation Qty"].ffill().fillna(0)
    )
    df_grid["available to order ffill"] = (
        df_grid.groupby(["_key"])["Qty available to order"].ffill().fillna(0)
    )
    df_grid["available PW1"] = df_grid.groupby(["_key"])["available to order ffill"].shift(1)
    df_grid["available PW2"] = df_grid.groupby(["_key"])["available to order ffill"].shift(2)
    df_grid["available PW3"] = df_grid.groupby(["_key"])["available to order ffill"].shift(3)
    df_grid["available PW"] = df_grid.apply(
        lambda x: min(x["available PW1"], x["available PW2"], x["available PW3"]), axis=1
    )
    df_grid["Risk of Loss Qty"] = df_grid.apply(
        lambda x: min([x["Weekly Allocation ffill"], x["available PW"]]), axis=1
    )

    df_m = df_grid[df_grid["YearWeek"] == current_yearweek].copy()
    df_m["Risk of Loss Qty"] = df_m["Risk of Loss Qty"].fillna(0).astype(int)
    df_m["Risk of Loss Qty"] = df_m["Risk of Loss Qty"].apply(lambda x: x if x > 0 else 0)
    df_m["Risk of Loss Qty (EA)"] = df_m["Risk of Loss Qty"] * df_m["Material Qty"]

    alloc_join_key = ["SoldTo #", "Material #", "SalesUOM Code", "C Group"]
    keep = alloc_join_key + ["Risk of Loss Qty", "Risk of Loss Qty (EA)"]
    return df_m[keep].copy()


def merge_and_metrics(df_mini_itemlabel, df_lost_alloc_to_join, df_ehc):
    """Join allocation rows with risk-of-loss + enriched inventory; add DIOH/flags."""
    df_m = df_mini_itemlabel.merge(
        df_lost_alloc_to_join,
        on=["SoldTo #", "Material #", "SalesUOM Code", "C Group"],
        how="left",
    ).merge(df_ehc, on=["Item"], how="left", indicator=True)

    df_m["DIOH"] = df_m["AvailableQty"] / df_m["AverageDailyIssueOut"]
    df_m["DIOH"] = df_m.apply(
        lambda x: 0.0 if x["AvailableQty"] == 0 else x["DIOH"], axis=1
    )
    df_m["Matched IMDC + IPYC"] = df_m["_merge"].apply(
        lambda x: "Matched" if x == "both" else "Not Matched"
    )
    df_m["No Issue Out (Last 365 Days)"] = df_m[["_merge", "AverageDailyIssueOut"]].apply(
        lambda x: "No Issue Out"
        if (x["_merge"] == "both" and pd.isnull(x["AverageDailyIssueOut"]))
        else "",
        axis=1,
    )
    df_m["DIOH (Allocation Qty)"] = (
        df_m["Weekly Allocation Qty (EA)"] / df_m["AverageDailyIssueOut"]
    )
    df_m["DIOH (Allocation Qty, by MPN)"] = (
        df_m["Weekly Allocation Qty (EA, by MPN)"] / df_m["AverageDailyIssueOut"]
    )
    df_m["Risk of Loss Qty (EA, by MPN)"] = (
        df_m.groupby("Manufacturer Item #")["Risk of Loss Qty (EA)"]
        .transform("sum")
        .fillna(df_m["Risk of Loss Qty (EA)"])
    )
    return df_m


def merge_substitutes(df_m, df_sub):
    """Join the substitute table onto the main frame with a ``_Repl.`` suffix."""
    return df_m.merge(df_sub, on=["Item"], how="left", suffixes=("", "_Repl."))


def aggregate_item_groups(df_msub, df_plmusage):
    """Aggregate availability across item groups and compute group-level DIOH."""
    has_ig = df_msub[~df_msub["Item Group"].isnull()]

    item = has_ig[["Item", "Item Group", "AvailableQty"]].drop_duplicates()
    replace_item = has_ig[
        ["Replace Item", "Item Group", "AvailableQty_Repl."]
    ].drop_duplicates()
    replace_item.columns = ["Item", "Item Group", "AvailableQty"]

    pre_agg = pd.concat([item, replace_item])
    qty_by_ig = pre_agg.groupby(["Item Group"]).agg({"AvailableQty": "sum"})

    df_ig_qty = qty_by_ig.merge(df_plmusage, on="Item Group", how="left")
    df_ig_qty["DIOH"] = df_ig_qty["AvailableQty"] / df_ig_qty["rolling_daily_avg_7"]
    df_ig_qty.columns = [
        "Item Group", "AvailableQty_ig", "AverageDailyIssueOut_ig", "DIOH_ig"
    ]

    df_ig_days = has_ig[["Item", "Item Group"]].drop_duplicates()
    return df_ig_qty.merge(df_ig_days, on="Item Group", how="left")


def build_full(df_msub, df_ig):
    """Merge the item-group metrics back onto the main dataset."""
    return df_msub.merge(df_ig, on=["Item", "Item Group"], how="left")


def recommend(df_full, target_dioh):
    """Flag Review items and compute recommended order quantities.

    Mutates *df_full* (adds DIOH_final / Flag / Min DIOH) and returns
    ``(df_full, df_review)``.
    """
    df_full["DIOH_final"] = df_full["DIOH_ig"].fillna(df_full["DIOH"])
    df_full["Flag"] = df_full["DIOH_final"].apply(
        lambda x: "Review" if x < target_dioh else "Okay"
    )
    df_full["Min DIOH"] = target_dioh

    cols = [
        "Dummy ID", "Flag", "SalesUOM Code", "Material Qty",
        "Qty available to order (EA)", "Qty available to order (EA, by MPN)",
        "Item", "DIOH_final", "Min DIOH",
        "With Sub", "Item Group", "Replace Item",
        "AverageDailyIssueOut", "AverageDailyIssueOut_ig",
        "DefaultBuyUOM", "BuyUOMMultiplier",
        "DefaultBuyUOM_Repl.", "BuyUOMMultiplier_Repl.",
    ]
    df_review = df_full[df_full["Flag"] == "Review"][cols].copy()

    df_review["Recommended Order Qty (EA)"] = (
        target_dioh - df_review["DIOH_final"]
    ) * (df_review["AverageDailyIssueOut_ig"].fillna(df_review["AverageDailyIssueOut"]))

    df_review["Recommended Order Qty (in BuyUOM)"] = np.ceil(
        df_review["Recommended Order Qty (EA)"] / df_review["BuyUOMMultiplier"]
    )
    df_review["Recomended Order Qty (in BuyUOM)_Repl."] = np.ceil(
        df_review["Recommended Order Qty (EA)"] / df_review["BuyUOMMultiplier_Repl."]
    )

    df_review["Recommended Order Qty with Allocation (EA)"] = df_review[
        ["Recommended Order Qty (EA)", "Qty available to order (EA, by MPN)"]
    ].apply(lambda x: x.min(), axis=1)
    df_review["Recommended Order Qty with Allocation (in BuyUOM)"] = np.ceil(
        df_review["Recommended Order Qty with Allocation (EA)"]
        / df_review["BuyUOMMultiplier"]
    )
    df_review["Recommended Order Qty with Allocation (in BuyUOM)_Repl."] = np.ceil(
        df_review["Recommended Order Qty with Allocation (EA)"]
        / df_review["BuyUOMMultiplier_Repl."]
    )
    return df_full, df_review


def assemble_output(df_full, df_review, df_small, current_yearweek, timestamp_value):
    """Join recommendations back, attach risk/timestamp, and append other weeks."""
    df_output = df_full.merge(
        df_review[
            [
                "Dummy ID",
                "Recommended Order Qty (EA)",
                "Recommended Order Qty (in BuyUOM)",
                "Recomended Order Qty (in BuyUOM)_Repl.",
                "Recommended Order Qty with Allocation (EA)",
                "Recommended Order Qty with Allocation (in BuyUOM)",
                "Recommended Order Qty with Allocation (in BuyUOM)_Repl.",
            ]
        ],
        on=["Dummy ID"],
        how="left",
    )
    df_output["Risk of Loss Qty (in SalesUOM, by MPN)"] = np.ceil(
        df_output["Risk of Loss Qty (EA, by MPN)"] / df_output["Material Qty"]
    )
    df_output["Inventory Data As Of"] = str(timestamp_value)[:19]

    other_weeks = df_small[df_small["YearWeek"] != current_yearweek]
    return pd.concat([df_output, other_weeks], ignore_index=True)
