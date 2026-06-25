"""Convenience entry point — equivalent to `python main.py --report medline_allocation`.

Kept so existing launchers/habits keep working; all dispatch logic lives in main.py.
Any extra arguments (e.g. --config) are forwarded through.
"""

import sys

from main import main

if __name__ == "__main__":
    main(["--report", "medline_allocation", *sys.argv[1:]])
