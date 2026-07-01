"""Config-driven Excel report styling and export.

One styling engine serves every report. All report-specific choices — fonts,
header colours, column order, sort keys, the visible-row filter, conditional
colouring, and cell highlighting — are read from ``config['report']`` so the
engine itself stays report-agnostic.
"""

import os

import pandas as pd


# ── filename + column order helpers ──────────────────────────────────


def build_output_filename(latest_file, config):
    """Construct the output file path from the source filename and config.

    ``report.output_basename`` + version, with the suffix taken from the source
    name after ``file_name_split_token``. The output is always ``.xlsx``.
    """
    report = config["report"]
    version = report["version"]
    split_token = report["file_name_split_token"]
    output_dir = report["output_dir"]
    basename = report["output_basename"]

    suffix = latest_file.split(split_token)[1]
    if suffix.lower().endswith(".xls"):
        suffix = suffix[: -len(".xls")] + ".xlsx"
    elif not suffix.lower().endswith(".xlsx"):
        suffix = suffix + ".xlsx"

    file_name = f"{basename} (v{version}){suffix}"
    return os.path.join(output_dir, file_name)


def _get_column_order(config):
    cols = config["report"]["columns"]
    order = []
    for group in config["report"]["column_order"]:
        order += cols[group]
    return order


def _get_col_to_hide(config):
    cols = config["report"]["columns"]
    hidden = list(config["report"]["col_to_hide"])
    for group in config["report"].get("hide_groups", []):
        hidden += cols[group]
    return hidden


def reorder_columns(df, config):
    """Return a copy of *df* with columns in the configured report order."""
    return df[_get_column_order(config)].copy()


# ── styling ───────────────────────────────────────────────────────────


def _with_numeric_suffix(file_name, suffix_number):
    stem, ext = os.path.splitext(file_name)
    return f"{stem} ({suffix_number}){ext}"


def _write_extra_sheet(workbook, sheet_name, df_extra):
    df_extra.to_excel(workbook, sheet_name=sheet_name, index=False)
    worksheet = workbook.sheets[sheet_name]
    worksheet.freeze_panes(1, 0)
    worksheet.autofilter(0, 0, len(df_extra), len(df_extra.columns) - 1)


def _write_workbook(df, file_name, config, filter_value, extra_sheets):
    report = config["report"]
    cols = report["columns"]

    font = report["font"]
    base_font = {"font_name": font["name"], "font_size": font["size"]}
    header_height = report["header_row_height"]
    default_col_width = report["default_col_width"]

    date_cols = report["date_cols"]
    two_decimal_cols = report["two_decimal_cols"]
    thousands_sep_cols = report["thousands_sep_cols"]
    fill_cols = report["fill_cols"]
    col_to_hide = _get_col_to_hide(config)

    writer = pd.ExcelWriter(
        file_name, engine="xlsxwriter",
        datetime_format="mm/dd/yyyy", date_format="mm/dd/yyyy",
    )

    with writer:
        df.to_excel(writer, sheet_name="Full", index=False)
        workbook = writer.book
        worksheet = writer.sheets["Full"]

        hdr = {
            **base_font,
            "bold": True, "border": 1,
            "text_wrap": True, "align": "center", "valign": "vcenter",
        }

        fmt_default = workbook.add_format({**base_font})
        worksheet.set_column(0, len(df.columns) - 1, default_col_width, fmt_default)

        # header formats: column -> fmt, built from report.header_colors
        header_fmt_by_col = {}
        for group, spec in report["header_colors"].items():
            props = {**hdr, "bg_color": spec["bg"]}
            if spec.get("font"):
                props["font_color"] = spec["font"]
            group_fmt = workbook.add_format(props)
            for col_name in cols.get(group, []):
                header_fmt_by_col[col_name] = group_fmt

        # Yellow-header override for explicitly listed columns (e.g. the PBO
        # v1.6 desired-DIOH block), applied on top of any group colour.
        yellow_header_cols = report.get("yellow_header_cols", [])
        if yellow_header_cols:
            yellow_fmt = workbook.add_format({**hdr, "bg_color": "#FFFF00"})
            for col_name in yellow_header_cols:
                header_fmt_by_col[col_name] = yellow_fmt

        fmt_two_dec = workbook.add_format({**base_font, "num_format": "#,##0.00"})
        fmt_thousands = workbook.add_format({**base_font, "num_format": "#,##0"})
        fmt_text = workbook.add_format(
            {**base_font, "align": "left", "valign": "vcenter", "text_wrap": False}
        )
        fmt_banded = workbook.add_format({**base_font, "bg_color": "#F2F2F2"})
        fmt_border = workbook.add_format(
            {**base_font, "border": 1, "border_color": "#D3D3D3"}
        )
        fmt_date = workbook.add_format({**base_font, "num_format": "mm/dd/yyyy"})

        worksheet.set_row(0, header_height)
        freeze_row, freeze_col = report.get("freeze_panes", [1, 0])
        worksheet.freeze_panes(freeze_row, freeze_col)

        worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {
            "type": "formula", "criteria": "=ROW()>0", "format": fmt_border,
        })

        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

        for col_num, col_name in enumerate(df.columns):
            if col_name in date_cols:
                worksheet.set_column(col_num, col_num, 12)
                for row_num, value in enumerate(df[col_name], start=1):
                    if pd.notna(value):
                        value = pd.to_datetime(value).to_pydatetime()
                        worksheet.write_datetime(row_num, col_num, value, fmt_date)
                    else:
                        worksheet.write_blank(row_num, col_num, None, fmt_date)
            elif col_name in two_decimal_cols:
                worksheet.set_column(col_num, col_num, 12, fmt_two_dec)
            elif col_name in thousands_sep_cols:
                worksheet.set_column(col_num, col_num, 10, fmt_thousands)

            if col_name in fill_cols:
                worksheet.set_column(col_num, col_num, 30, fmt_text)

            if col_name in col_to_hide:
                worksheet.set_column(col_num, col_num, None, None, {"hidden": True})

            if col_name in header_fmt_by_col:
                worksheet.write(
                    0, col_num, _dynamic_header(col_name), header_fmt_by_col[col_name]
                )

        # Blank-fill empties so long neighbouring text does not overflow into them.
        for row_num in range(1, len(df) + 1):
            for col_num in range(len(df.columns)):
                value = df.iloc[row_num - 1, col_num]
                if pd.isna(value) or value == "":
                    worksheet.write(row_num, col_num, " ", fmt_default)

        # Live Excel formulas overwrite the blank-fill placeholders above.
        _apply_formula_columns(workbook, worksheet, df, report, base_font)

        worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {
            "type": "formula", "criteria": "=MOD(ROW(),2)=0", "format": fmt_banded,
        })

        _apply_conditional_color(workbook, worksheet, df, report, base_font)
        _apply_value_fills(workbook, worksheet, df, report, base_font)
        _apply_highlight(workbook, worksheet, df, report, base_font)
        _apply_row_filter(worksheet, df, report, filter_value)

        for sheet_name, df_extra in (extra_sheets or []):
            if df_extra is not None and not df_extra.empty:
                _write_extra_sheet(writer, sheet_name, df_extra)


def _apply_conditional_color(workbook, worksheet, df, report, base_font):
    """Red/green scale on threshold columns (e.g. PBO 'Get Well - DIOH')."""
    spec = report.get("conditional_color")
    if not spec:
        return
    threshold = report[spec["threshold_key"]]
    fmt_red = workbook.add_format(
        {**base_font, "bg_color": "#FFC7CE", "font_color": "#9C0006"}
    )
    fmt_green = workbook.add_format(
        {**base_font, "bg_color": "#C6EFCE", "font_color": "#006100"}
    )
    for col_name in spec["columns"]:
        if col_name not in df.columns:
            continue
        idx = df.columns.get_loc(col_name)
        worksheet.conditional_format(1, idx, len(df), idx, {
            "type": "blanks", "stop_if_true": True,
        })
        worksheet.conditional_format(1, idx, len(df), idx, {
            "type": "cell", "criteria": ">=", "value": threshold, "format": fmt_red,
        })
        worksheet.conditional_format(1, idx, len(df), idx, {
            "type": "cell", "criteria": "<", "value": threshold, "format": fmt_green,
        })


def _apply_value_fills(workbook, worksheet, df, report, base_font):
    """Fill a cell's background when its own numeric value meets a comparison.

    ``report.value_fills`` is a list of rules::

        value_fills:
          - column: "MIOH"
            criteria: ">"
            value: 0
            bg: "#C6EFCE"   # light green

    The comparison is guarded with ``ISNUMBER`` so blank-fill placeholders (a
    space, which Excel ranks above any number) are never matched.
    """
    specs = report.get("value_fills")
    if not specs:
        return
    from xlsxwriter.utility import xl_col_to_name

    for spec in specs:
        col_name = spec["column"]
        if col_name not in df.columns:
            continue
        idx = df.columns.get_loc(col_name)
        letter = xl_col_to_name(idx)
        fmt = workbook.add_format({**base_font, "bg_color": spec["bg"]})
        criteria = spec.get("criteria", ">")
        value = spec["value"]
        worksheet.conditional_format(1, idx, len(df), idx, {
            "type": "formula",
            "criteria": f"=AND(ISNUMBER(${letter}2),${letter}2{criteria}{value})",
            "format": fmt,
        })


def _apply_highlight(workbook, worksheet, df, report, base_font):
    """Highlight target columns where a driver column equals a value (Allocation)."""
    spec = report.get("highlight")
    if not spec:
        return
    from xlsxwriter.utility import xl_col_to_name

    driver_col = spec["driver_col"]
    if driver_col not in df.columns:
        return
    driver_letter = xl_col_to_name(df.columns.get_loc(driver_col))
    fmt_hl = workbook.add_format({**base_font, "font_color": "red"})
    last_row = len(df) + 1
    for col_name in spec["target_cols"]:
        if col_name not in df.columns:
            continue
        col_letter = xl_col_to_name(df.columns.get_loc(col_name))
        worksheet.conditional_format(
            f"{col_letter}2:{col_letter}{last_row}",
            {
                "type": "formula",
                "criteria": f'=${driver_letter}2="{spec["driver_value"]}"',
                "format": fmt_hl,
            },
        )


def _apply_row_filter(worksheet, df, report, filter_value):
    """Autofilter to, and hide every row not matching, a single column value.

    The value comes from ``filter_value`` (runtime) when given, else
    ``row_filter.value`` (static, e.g. PBO 'Matched').
    """
    spec = report.get("row_filter")
    if not spec:
        return
    col = spec["column"]
    value = filter_value if filter_value is not None else spec.get("value")
    if col not in df.columns or value is None:
        return
    idx = df.columns.get_loc(col)
    worksheet.filter_column(idx, f"x == {value}")
    for row_num, cell in enumerate(df[col], start=1):
        if str(cell) != str(value):
            worksheet.set_row(row_num, None, None, {"hidden": True})


def _dynamic_header(col_name):
    """Substitute a literal ``YYYY-MM-DD`` token in a header with today's date."""
    if "YYYY-MM-DD" in col_name:
        from datetime import date
        return col_name.replace("YYYY-MM-DD", date.today().isoformat())
    return col_name


def _apply_formula_columns(workbook, worksheet, df, report, base_font):
    """Write live Excel formulas into columns configured under ``formula_cols``.

    Each entry maps a target column to a rule::

        formula_cols:
          "Qty to order for Desired DIOH":
            template: "=(({Desired DIOH}-{DIOH})*{AverageDailyIssueOut}-{OnOrderQty})/{BuyUOMMultiplier}"
            only_when: { column: "Flag", value: "Review" }

    Every ``{Column Name}`` token is replaced with that column's live cell
    reference for the current row (e.g. ``AN8``), so the formula recalculates in
    Excel if a referenced cell is edited. With ``only_when`` set, the formula is
    written only for rows whose gate column equals the value; other rows keep the
    blank-fill placeholder.
    """
    spec = report.get("formula_cols")
    if not spec:
        return

    import re
    from xlsxwriter.utility import xl_col_to_name

    two_decimal_cols = report.get("two_decimal_cols", [])
    thousands_sep_cols = report.get("thousands_sep_cols", [])
    token_re = re.compile(r"\{([^{}]+)\}")

    for col_name, rule in spec.items():
        if col_name not in df.columns:
            continue
        referenced = token_re.findall(rule["template"])
        missing = [c for c in referenced if c not in df.columns]
        if missing:
            print(f"Formula column '{col_name}' skipped — missing columns: {missing}")
            continue

        target_idx = df.columns.get_loc(col_name)
        if col_name in two_decimal_cols:
            fmt = workbook.add_format({**base_font, "num_format": "#,##0.00"})
        elif col_name in thousands_sep_cols:
            fmt = workbook.add_format({**base_font, "num_format": "#,##0"})
        else:
            fmt = workbook.add_format({**base_font})

        col_letters = {c: xl_col_to_name(df.columns.get_loc(c)) for c in referenced}

        gate = rule.get("only_when")
        gate_idx = df.columns.get_loc(gate["column"]) if gate else None
        gate_value = str(gate["value"]) if gate else None

        for i in range(len(df)):
            if gate is not None and str(df.iloc[i, gate_idx]) != gate_value:
                continue
            excel_row = i + 2  # header occupies Excel row 1; data starts at row 2
            formula = token_re.sub(
                lambda m: f"{col_letters[m.group(1)]}{excel_row}", rule["template"]
            )
            worksheet.write_formula(i + 1, target_idx, formula, fmt)


def apply_inventory_styling(df, file_name, config, filter_value=None, extra_sheets=None):
    """Sort, coerce types, write the styled 'Full' sheet, and save.

    Retries with a numeric suffix if the target path is locked.

    Parameters
    ----------
    filter_value : str | None
        Runtime value for ``report.row_filter`` (e.g. the current YearWeek).
    extra_sheets : list[tuple[str, DataFrame]] | None
        Optional additional worksheets, written only when non-empty.
    """
    report = config["report"]
    date_cols = report["date_cols"]
    two_decimal_cols = report["two_decimal_cols"]
    thousands_sep_cols = report["thousands_sep_cols"]

    # ── sorting (existing columns only) ──
    sort_pairs = [
        (col, asc)
        for col, asc in zip(report["sort_by"], report["sort_ascending"])
        if col in df.columns
    ]
    if sort_pairs:
        df = df.sort_values(
            by=[c for c, _ in sort_pairs],
            ascending=[a for _, a in sort_pairs],
            na_position="last",
        )

    # ── type coercion ──
    df = df.copy()
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)
    for col in two_decimal_cols + thousands_sep_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    candidate = file_name
    suffix_number = 0
    while True:
        try:
            _write_workbook(df, candidate, config, filter_value, extra_sheets)
            break
        except PermissionError:
            if not os.path.exists(candidate):
                raise
            suffix_number += 1
            candidate = _with_numeric_suffix(file_name, suffix_number)

    if candidate != file_name:
        print(
            f"Primary output file was locked. Report saved to alternate path: {candidate}"
        )
    else:
        print(f"Styling applied, report saved to {candidate}")

    return candidate
