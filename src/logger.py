"""Tee-style logger — duplicates stdout/stderr to a timestamped log file."""

import os
import sys
from datetime import datetime


class _TeeStream:
    """Write to both the original stream and a log file."""

    def __init__(self, original, log_file):
        self._original = original
        self._log_file = log_file

    def write(self, text):
        self._original.write(text)
        self._log_file.write(text)
        self._log_file.flush()

    def flush(self):
        self._original.flush()
        self._log_file.flush()

    # forward any other attribute lookups to the original stream
    def __getattr__(self, name):
        return getattr(self._original, name)


class TeeLogger:
    """Replace sys.stdout/stderr with a tee that also writes to a log file.

    Usage
    -----
    >>> logger = TeeLogger(log_dir)
    >>> # ... pipeline work (all print() output is captured) ...
    >>> log_path = logger.close()
    """

    def __init__(self, log_dir, log_path=None):
        os.makedirs(log_dir, exist_ok=True)
        if log_path is None:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_path = os.path.join(log_dir, f"log_{stamp}.txt")
        self._log_path = log_path
        self._log_file = open(self._log_path, "w", encoding="utf-8")

        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr

        sys.stdout = _TeeStream(self._orig_stdout, self._log_file)
        sys.stderr = _TeeStream(self._orig_stderr, self._log_file)

    @property
    def log_path(self):
        return self._log_path

    def close(self):
        """Restore original streams, close log file, return log path."""
        sys.stdout = self._orig_stdout
        sys.stderr = self._orig_stderr
        self._log_file.close()
        return self._log_path
