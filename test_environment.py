#!/usr/bin/env python3
"""Environment diagnostic tests for the Medline PBO pipeline.

Verifies that the current machine has:
  1. Read/write access to the I: drive output directory
  2. Database connectivity to PRIME and ETL Health servers
  3. MS Graph authentication and API access

Usage:
    python test_environment.py                      # uses config.yaml
    python test_environment.py --config other.yaml  # custom config
"""

import argparse
import os
import sys

import pyodbc
import requests

from src.config_loader import load_config, load_secrets
from src.db import get_connection
from src.msgraph import get_access_token
from src.secret_crypto import SECRET_ENV_VAR

# ── Bookkeeping ──────────────────────────────────────────────

_results = []


def _record(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    tag = f"  [{status}] {name}"
    if detail:
        tag += f"  —  {detail}"
    print(tag)
    _results.append((name, passed))


# ── Test Group 1: I: Drive Access ────────────────────────────


def test_drive_access(config):
    print("\n=== Test Group 1: I: Drive Access ===")
    output_dir = config["report"]["output_dir"]
    print(f"  Target directory: {output_dir}")

    # 1a — directory exists
    exists = os.path.isdir(output_dir)
    _record(
        "1a  Directory exists",
        exists,
        "" if exists else "Ensure the I: drive is mapped and the path exists.",
    )
    if not exists:
        _record(
            "1b  Read/write access",
            False,
            "Skipped — directory does not exist.",
        )
        return

    # 1b — write, read-back, delete
    test_file = os.path.join(output_dir, "_pbo_write_test.tmp")
    sentinel = "pbo-environment-test"
    try:
        with open(test_file, "w", encoding="utf-8") as f:
            f.write(sentinel)
        with open(test_file, "r", encoding="utf-8") as f:
            content = f.read()
        ok = content == sentinel
        _record(
            "1b  Read/write access",
            ok,
            "" if ok else "File was written but read-back content did not match.",
        )
    except PermissionError as exc:
        _record(
            "1b  Read/write access",
            False,
            f"Permission denied: {exc}",
        )
    except OSError as exc:
        _record(
            "1b  Read/write access",
            False,
            str(exc),
        )
    finally:
        try:
            os.remove(test_file)
        except OSError:
            pass


# ── Test Group 2: Database Connectivity ──────────────────────


def test_database(config):
    print("\n=== Test Group 2: Database Connectivity ===")

    db_cfg = config["database"]
    etl_cfg = config["etl_health"]

    # 2a — connect to PRIME
    prime_conn = None
    print(f"  PRIME server: {db_cfg['server']}  database: {db_cfg['database']}")
    try:
        prime_conn = get_connection(config)
        _record("2a  Connect to PRIME", True)
    except (pyodbc.Error, Exception) as exc:
        _record("2a  Connect to PRIME", False, str(exc))

    # 2b — read INVENTORY_LOCATION
    if prime_conn:
        try:
            cursor = prime_conn.cursor()
            cursor.execute(
                r"SELECT TOP 1 * FROM [DM_MONTYNT\dli2].INVENTORY_LOCATION"
            )
            row = cursor.fetchone()
            _record(
                "2b  Read INVENTORY_LOCATION",
                row is not None,
                "" if row else "Query returned no rows.",
            )
        except (pyodbc.Error, Exception) as exc:
            _record("2b  Read INVENTORY_LOCATION", False, str(exc))
    else:
        _record("2b  Read INVENTORY_LOCATION", False, "Skipped — no PRIME connection.")

    # 2c — read plm.Itemlink
    if prime_conn:
        try:
            cursor = prime_conn.cursor()
            cursor.execute("SELECT TOP 1 * FROM plm.Itemlink")
            row = cursor.fetchone()
            _record(
                "2c  Read plm.Itemlink",
                row is not None,
                "" if row else "Query returned no rows.",
            )
        except (pyodbc.Error, Exception) as exc:
            _record("2c  Read plm.Itemlink", False, str(exc))
    else:
        _record("2c  Read plm.Itemlink", False, "Skipped — no PRIME connection.")

    if prime_conn:
        prime_conn.close()

    # 2d — connect to ETL Health server
    print(f"  ETL Health server: {etl_cfg['server']}  database: {etl_cfg['database']}")
    etl_conn = None
    try:
        etl_conn = pyodbc.connect(
            driver=etl_cfg["driver"],
            server=etl_cfg["server"],
            database=etl_cfg["database"],
            trusted_connection=etl_cfg["trusted_connection"],
        )
        _record("2d  Connect to ETL Health DB", True)
    except (pyodbc.Error, Exception) as exc:
        _record("2d  Connect to ETL Health DB", False, str(exc))

    # 2e — read ETLHealth table
    if etl_conn:
        try:
            schema = etl_cfg["schema"]
            table = etl_cfg["table"]
            cursor = etl_conn.cursor()
            cursor.execute(f"SELECT TOP 1 * FROM [{schema}].[{table}]")
            row = cursor.fetchone()
            if row:
                _record("2e  Read ETLHealth table", True)
            else:
                print("  [WARN] 2e  Read ETLHealth table  —  Table is accessible but empty.")
                _results.append(("2e  Read ETLHealth table", True))
        except (pyodbc.Error, Exception) as exc:
            _record("2e  Read ETLHealth table", False, str(exc))
        finally:
            etl_conn.close()
    else:
        _record("2e  Read ETLHealth table", False, "Skipped — no ETL Health connection.")


# ── Test Group 3: MS Graph Authentication ────────────────────


def test_msgraph(config, secrets):
    print("\n=== Test Group 3: MS Graph Authentication ===")

    # 3a — acquire access token
    token = None
    try:
        token = get_access_token(secrets, config)
        _record("3a  Acquire access token", True)
    except (requests.HTTPError, Exception) as exc:
        _record("3a  Acquire access token", False, str(exc))

    # 3b — list mail folders (uses existing Mail.Read permission)
    from_email = config["email"]["from_email"]
    if token:
        graph = config["email"]["graph_endpoint"]
        url = f"{graph}/v1.0/users/{from_email}/mailFolders"
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(url, headers=headers, params={"$top": 1}, timeout=20)
            resp.raise_for_status()
            folders = resp.json().get("value", [])
            detail = f"folder: {folders[0]['displayName']}" if folders else "(no folders)"
            _record("3b  Read mail folders", True, detail)
        except (requests.HTTPError, Exception) as exc:
            _record("3b  Read mail folders", False, str(exc))
    else:
        _record("3b  Read mail folders", False, "Skipped — no access token.")


# ── Main ─────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Medline PBO environment diagnostics")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()

    # Guard: passphrase must be set
    if not os.getenv(SECRET_ENV_VAR, "").strip():
        print(
            f"ERROR: {SECRET_ENV_VAR} is not set.\n"
            "Run 'python first_time_setup.py' first to configure secrets."
        )
        sys.exit(1)

    print("Loading config and secrets...")
    config = load_config(args.config)
    secrets = load_secrets()

    # Run all test groups
    test_drive_access(config)
    test_database(config)
    test_msgraph(config, secrets)

    # Summary
    passed = sum(1 for _, ok in _results if ok)
    total = len(_results)
    print(f"\n{'=' * 40}")
    print(f"  {passed}/{total} tests passed", end="")
    if passed == total:
        print("  —  ALL PASS")
    else:
        failed = [name for name, ok in _results if not ok]
        print(f"  —  FAILED: {', '.join(failed)}")
    print(f"{'=' * 40}")
    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
