import pandas as pd
import pyodbc


def get_connection(config):
    """Open a pyodbc connection using settings from config['database']."""
    db = config["database"]
    return pyodbc.connect(
        driver=db["driver"],
        server=db["server"],
        database=db["database"],
        trusted_connection=db["trusted_connection"],
    )


# SQL templates — {location_filter} is substituted at runtime.
SQL_TEMPLATES = {
    "inventory": r"""
        SELECT *
        FROM (
            SELECT Location, Item, ItemDescription, Active, Discontinued,
                   VendorItem, Vendor, VendorName,
                   ManufacturerNumber, StrippedManufacturerNumber, ManufacturerName,
                   StockUOM, DefaultBuyUOM, BuyUOMMultiplier, UnitCostInStockUOM,
                   AvailableQty, OnOrderQty, [update stamp], [report stamp],
                   ROW_NUMBER() OVER (
                       PARTITION BY [Location], Item
                       ORDER BY [report stamp] DESC
                   ) AS RK
            FROM [DM_MONTYNT\dli2].INVENTORY_LOCATION
            WHERE Location IN ({location_filter})
              AND Active = 'Yes'
              AND Discontinued = 'No'
        ) c
        WHERE RK = 1
    """,
    "usage": """
        SELECT Location, Item,
               SUM(QtyInLum) * 1.0 / 365 AS AverageDailyIssueOut
        FROM (
            SELECT *
            FROM plm.DailyIssueOutQty
            WHERE Location IN ({location_filter})
              AND trx_date BETWEEN DATEADD(DAY, -366, GETDATE()) AND GETDATE()
        ) c
        GROUP BY Location, Item
    """,
    "long_desc": r"""
        SELECT Item, Description3
        FROM [DM_MONTYNT\dli2].MDM_ITEM
    """,
    "plmlink": """
        SELECT [Item Group], Item, [Replace Item], [Stage]
        FROM plm.Itemlink
        WHERE Stage NOT IN ('Deleted', 'Completed', 'Pending Item Number')
    """,
    "plmusage": """
        SELECT [Item Group], rolling_daily_avg_7
        FROM PLM.PLMItemGroupBRRolling
        WHERE Location IN ({location_filter})
    """,
    "timestamp": r"""
        SELECT MAX([report stamp]) AS stamp
        FROM [DM_MONTYNT\dli2].INVENTORY_LOCATION
        WHERE Location IN ({location_filter})
          AND Active = 'Yes'
          AND Discontinued = 'No'
          AND [report stamp] >= DATEADD(DAY, -10, GETDATE())
    """,
}


def _build_location_filter(locations):
    escaped_locations = [location.replace("'", "''") for location in locations]
    return ", ".join(f"'{location}'" for location in escaped_locations)


def fetch_all_tables(conn, locations):
    """Execute every SQL template and return a dict of DataFrames.

    Keys: inventory, usage, long_desc, plmlink, plmusage, timestamp
    """
    if isinstance(locations, str):
        locations = [locations]

    location_filter = _build_location_filter(locations)
    results = {}
    for name, template in SQL_TEMPLATES.items():
        sql = template.format(location_filter=location_filter)
        results[name] = pd.read_sql_query(sql, conn)
    return results


# ── ETL Health tracking ──────────────────────────────────────


def insert_etl_health(
    config,
    *,
    source_file_path,
    last_run_time,
    task_status,
    row_count,
    duration,
    package_path,
    log_file_path,
    error_message,
):
    """Insert a row into [MedlinePBO].[ETLHealth] on the ETL-health server.

    Uses a *separate* connection from the main PRIME database.
    """
    etl = config["etl_health"]
    conn = pyodbc.connect(
        driver=etl["driver"],
        server=etl["server"],
        database=etl["database"],
        trusted_connection=etl["trusted_connection"],
    )
    sql = (
        f"INSERT INTO [{etl['schema']}].[{etl['table']}] "
        "([ProcessName], [ProcessID], [SourceFilePath], [LastRunTime], "
        "[TargetTableName], [TaskStatus], [RowCount], [Duration], [PackagePath], "
        "[LogFilePath], [STGTableName], [ProcessFrequency], [Error]) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    params = (
        etl["process_name"],
        etl["process_id"],
        source_file_path or "",
        last_run_time,
        "Not Applicable",
        task_status,
        row_count,
        duration,
        package_path,
        log_file_path or "",
        "Not Applicable",
        etl["process_frequency"],
        error_message,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        print(f"ETL Health logged — {task_status}")
    finally:
        conn.close()
