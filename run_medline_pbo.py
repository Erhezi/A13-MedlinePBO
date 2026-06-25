"""Medline PBO report — entry point.

Usage:
    python run_medline_pbo.py
    python run_medline_pbo.py --config <path-to-config.yaml>

Scheduled separately from the Allocation report. The shared runner
(src/runner.py) provides the timeout watchdog, logging, notification,
ETL-health, and maintenance; the report-specific steps live in
reports/medline_pbo/pipeline.py.
"""

import os
import warnings

from src import runner
from reports.medline_pbo.pipeline import run as pipeline_run

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

DEFAULT_CONFIG = os.path.join("reports", "medline_pbo", "config.yaml")


if __name__ == "__main__":
    runner.run(__file__, pipeline_run, default_config=DEFAULT_CONFIG)
