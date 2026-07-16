"""Unified entry point for the Medline report pipelines.

Usage:
    python main.py --report medline_pbo
    python main.py --report medline_allocation

    if need to override the default config path, use --config:
    python main.py --report medline_pbo --config <path-to-config.yaml>
    python main.py --report medline_allocation --config <path-to-config.yaml>

    Notification mode (defaults to test so a bare run can never email the
    full distribution list):
    python main.py --report medline_pbo --mode test   # test_recipients only
    python main.py --report medline_pbo --mode prd    # full recipient lists

The shared runner (src/runner.py) provides the timeout watchdog, logging,
notification, ETL-health, and maintenance. Each report's own steps live in
reports/<report>/pipeline.py and are selected here by --report.
"""

import argparse
import os
import warnings

from src import runner
from reports.medline_pbo import pipeline as medline_pbo_pipeline
from reports.medline_allocation import pipeline as medline_allocation_pipeline

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# report name -> (default config path, pipeline module)
REGISTRY = {
    "medline_pbo": (
        os.path.join("reports", "medline_pbo", "config.yaml"),
        medline_pbo_pipeline,
    ),
    "medline_allocation": (
        os.path.join("reports", "medline_allocation", "config.yaml"),
        medline_allocation_pipeline,
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
    parser.add_argument(
        "--mode", choices=["test", "prd"], default="test",
        help="test (default): send notifications to test_recipients only; "
             "prd: use the full recipient lists.",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--log-path", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    default_config, pipeline_module = REGISTRY[args.report]
    config_path = args.config or default_config
    package_path = os.path.abspath(__file__)

    if args.worker:
        runner.run_worker(
            pipeline_module.run, config_path, args.log_path, package_path,
            pipeline_module.STEP_COUNT, mode=args.mode,
        )
    else:
        runner.run_parent(
            entry_path=package_path,
            config_path=config_path,
            package_path=package_path,
            forward_args=["--report", args.report, "--mode", args.mode],
            mode=args.mode,
        )


if __name__ == "__main__":
    main()
