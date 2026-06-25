"""Unified entry point for the Medline report pipelines.

Usage:
    python main.py --report medline_pbo
    python main.py --report medline_allocation

    if need to override the default config path, use --config:
    python main.py --report medline_pbo --config <path-to-config.yaml>
    python main.py --report medline_allocation --config <path-to-config.yaml>

The shared runner (src/runner.py) provides the timeout watchdog, logging,
notification, ETL-health, and maintenance. Each report's own steps live in
reports/<report>/pipeline.py and are selected here by --report.
"""

import argparse
import os
import warnings

from src import runner
from reports.medline_pbo.pipeline import run as medline_pbo_run
from reports.medline_allocation.pipeline import run as medline_allocation_run

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# report name -> (default config path, pipeline function)
REGISTRY = {
    "medline_pbo": (
        os.path.join("reports", "medline_pbo", "config.yaml"),
        medline_pbo_run,
    ),
    "medline_allocation": (
        os.path.join("reports", "medline_allocation", "config.yaml"),
        medline_allocation_run,
    ),
}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Medline report pipelines")
    parser.add_argument(
        "--report", required=True, choices=sorted(REGISTRY),
        help="Which report to run.",
    )
    parser.add_argument(
        "--config", default=None, help="Override the report's default config path.",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--log-path", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    default_config, pipeline_fn = REGISTRY[args.report]
    config_path = args.config or default_config
    package_path = os.path.abspath(__file__)

    if args.worker:
        runner.run_worker(pipeline_fn, config_path, args.log_path, package_path)
    else:
        runner.run_parent(
            entry_path=package_path,
            config_path=config_path,
            package_path=package_path,
            forward_args=["--report", args.report],
        )


if __name__ == "__main__":
    main()
