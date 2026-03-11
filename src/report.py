"""Excel report styling and export."""

import os

import pandas as pd


def build_output_filename(latest_file, config):
    """Construct the output file path from the source filename and config."""
    version = config["report"]["version"]
    split_token = config["report"]["file_name_split_token"]
    output_dir = config["report"]["output_dir"]

    suffix = latest_file.split(split_token)[1]
    file_name = f"Processed_Monte_PBO (v{version}){suffix}"
    return os.path.join(output_dir, file_name)


def _get_column_order(cfg):
    cols = cfg["report"]["columns"]
    return (
        cols["base_report"]
        + cols["item"]
        + cols["item_group"]
        + cols["rmd"]
        + cols["rmd_debug"]
        + cols["stamp"]
    )


def _get_col_to_hide(cfg):
    cols = cfg["report"]["columns"]
    return (
        cfg["report"]["col_to_hide"]
        + cols["rmd_debug"]
        + cols["stamp"]
    )


def reorder_columns(df, config):
    """Return a copy of *df* with columns in the configured report order."""
    order = _get_column_order(config)
    return df[order].copy()


def apply_inventory_styling(df, file_name, config):
    """Write *df* to an xlsx with full formatting, conditional colours, etc."""
    cfg = config["report"]
    cols = cfg["columns"]
    review_threshold = cfg["review_threshold"]
    col_to_hide = _get_col_to_hide(config)
    date_cols = cfg["date_cols"]
    two_decimal_cols = cfg["two_decimal_cols"]
    thousands_sep_cols = cfg["thousands_sep_cols"]
    fill_cols = cfg["fill_cols"]

    base_report = cols["base_report"]
    item = cols["item"]
    item_group = cols["item_group"]
    rmd = cols["rmd"]

    # ── sorting ──
    df = df.sort_values(
        by=["Matched IMDCSTRM", "Get Well - DIOH", "Get Well - DIOH_ig"],
        ascending=[True, False, False],
        na_position="last",
    )

    # ── type coercion ──
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)

    for col in two_decimal_cols + thousands_sep_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── write to Excel ──
    writer = pd.ExcelWriter(
        file_name, engine="xlsxwriter",
        datetime_format="mm/dd/yyyy", date_format="mm/dd/yyyy",
    )

    with writer:
        df.to_excel(writer, sheet_name="Full", index=False)
        workbook = writer.book
        worksheet = writer.sheets["Full"]

        # header formats
        hdr = {
            "bold": True, "border": 1,
            "text_wrap": True, "align": "center", "valign": "vcenter",
        }
        fmt_base = workbook.add_format({**hdr, "bg_color": "#94c5e3"})
        fmt_item = workbook.add_format({**hdr, "bg_color": "#003769", "font_color": "white"})
        fmt_ig = workbook.add_format({**hdr, "bg_color": "#112b47", "font_color": "white"})
        fmt_rmd = workbook.add_format({**hdr, "bg_color": "#ca006c", "font_color": "white"})

        # number formats
        fmt_two_dec = workbook.add_format({"num_format": "#,##0.00"})
        fmt_thousands = workbook.add_format({"num_format": "#,##0"})

        # conditional highlight formats
        fmt_red = workbook.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006"})
        fmt_green = workbook.add_format({"bg_color": "#C6EFCE", "font_color": "#006100"})

        # misc formats
        fmt_fill = workbook.add_format({"align": "fill"})
        fmt_banded = workbook.add_format({"bg_color": "#F2F2F2"})
        fmt_border = workbook.add_format({"border": 1, "border_color": "#D3D3D3"})

        worksheet.set_row(0, 45)
        worksheet.freeze_panes(1, 0)

        # global border
        worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {
            "type": "formula", "criteria": "=ROW()>0", "format": fmt_border,
        })

        # filter on Matched
        if "Matched IMDCSTRM" in df.columns:
            match_idx = df.columns.get_loc("Matched IMDCSTRM")
            for row_num, value in enumerate(df["Matched IMDCSTRM"]):
                if value != "Matched":
                    worksheet.set_row(row_num + 1, None, None, {"hidden": True})
            worksheet.filter_column(match_idx, "x == Matched")

        # per-column formatting
        for col_num, col_name in enumerate(df.columns):
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

            if col_name in date_cols:
                worksheet.set_column(col_num, col_num, 12)
            elif col_name in two_decimal_cols:
                worksheet.set_column(col_num, col_num, 12, fmt_two_dec)
            elif col_name in thousands_sep_cols:
                worksheet.set_column(col_num, col_num, 10, fmt_thousands)

            if col_name in fill_cols:
                worksheet.set_column(col_num, col_num, 30, fmt_fill)

            if col_name in col_to_hide:
                worksheet.set_column(col_num, col_num, None, None, {"hidden": True})

            # styled headers
            fmt_map = None
            if col_name in base_report:
                fmt_map = fmt_base
            elif col_name in item:
                fmt_map = fmt_item
            elif col_name in item_group:
                fmt_map = fmt_ig
            elif col_name in rmd:
                fmt_map = fmt_rmd

            if fmt_map:
                worksheet.write(0, col_num, col_name, fmt_map)

        # banding
        worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {
            "type": "formula", "criteria": "=MOD(ROW(),2)=0", "format": fmt_banded,
        })

        # conditional highlighting on Get Well columns
        for col_name in ["Get Well - DIOH", "Get Well - DIOH_ig"]:
            if col_name in df.columns:
                idx = df.columns.get_loc(col_name)
                worksheet.conditional_format(1, idx, len(df), idx, {
                    "type": "blanks", "stop_if_true": True,
                })
                worksheet.conditional_format(1, idx, len(df), idx, {
                    "type": "cell", "criteria": ">=",
                    "value": review_threshold, "format": fmt_red,
                })
                worksheet.conditional_format(1, idx, len(df), idx, {
                    "type": "cell", "criteria": "<",
                    "value": review_threshold, "format": fmt_green,
                })

    print(f"Report saved to {file_name}")
