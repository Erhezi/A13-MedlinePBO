"""Database connections, table fetching, and ETL-health logging.

Report-agnostic. SQL templates live in ``src/queries.py`` (shared base) and in
each report's own ``queries.py`` (report-specific additions).
"""

import pandas as pd
import pyodbc

from src.queries import BASE_TEMPLATES


def get_connection(config):
    """Open a pyodbc connection to the main PRIME database (config['database'])."""
    db = config["database"]
    return pyodbc.connect(
        driver=db["driver"],
        server=db["server"],
        database=db["database"],
        trusted_connection=db["trusted_connection"],
    )


def get_scs_connection(config):
    """Open a pyodbc connection to the SCSFileIngestor server (config['etl_health']).

    The same server hosts ETL-health tracking and report-specific helper tables
    (e.g. MedlineAllocation.WeeklyAllocHelper).
    """
    etl = config["etl_health"]
    return pyodbc.connect(
        driver=etl["driver"],
        server=etl["server"],
        database=etl["database"],
        trusted_connection=etl["trusted_connection"],
    )


def get_scs_engine(config):
    """Return a SQLAlchemy engine for the SCSFileIngestor server.

    Used by reports that upsert into their own helper tables.
    """
    from sqlalchemy import create_engine

    etl = config["etl_health"]
    driver = etl["driver"].strip("{}").replace(" ", "+")
    url = (
        f"mssql+pyodbc://{etl['server']}/{etl['database']}"
        f"?driver={driver}&trusted_connection={etl['trusted_connection']}"
    )
    return create_engine(url, fast_executemany=True)


def _build_location_filter(locations):
    escaped_locations = [location.replace("'", "''") for location in locations]
    return ", ".join(f"'{location}'" for location in escaped_locations)


def fetch_tables(conn, locations, templates=BASE_TEMPLATES):
    """Execute every SQL template and return a dict of DataFrames keyed by name.

    ``templates`` defaults to the shared ``BASE_TEMPLATES``; a report passes its
    own dict (base + extras) to fetch additional tables in one call.
    """
    if isinstance(locations, str):
        locations = [locations]

    location_filter = _build_location_filter(locations)
    results = {}
    for name, template in templates.items():
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
    """Insert a row into [<schema>].[<table>] on the ETL-health server.

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
