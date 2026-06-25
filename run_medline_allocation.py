"""Medline Allocation report — entry point.

Usage:
    python run_medline_allocation.py
    python run_medline_allocation.py --config <path-to-config.yaml>

Scheduled separately from the PBO report (this one runs weekly). The shared
runner (src/runner.py) provides the timeout watchdog, logging, notification,
ETL-health, and maintenance; the report-specific steps live in
reports/medline_allocation/pipeline.py.
"""

import os
import warnings

from src import runner
from reports.medline_allocation.pipeline import run as pipeline_run

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

DEFAULT_CONFIG = os.path.join("reports", "medline_allocation", "config.yaml")


if __name__ == "__main__":
    runner.run(__file__, pipeline_run, default_config=DEFAULT_CONFIG)
