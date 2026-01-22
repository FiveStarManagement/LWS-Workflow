# lws_workflow/logger.py

import os
import sys
import io
import glob
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

from config import LOG_DIR, LOG_FILE, LOG_LEVEL


class SizeAndTimeRotatingFileHandler(RotatingFileHandler):
    """
    Rotate log file when:
      - file exceeds maxBytes (size-based)
      - OR date changes (daily rotation)

    Keeps backupCount total rotated files.
    Rotated file names look like:
      lws_workflow.log.2025-12-31_235959
    """

    def __init__(self, filename, maxBytes, backupCount, encoding="utf-8", delay=True):
        # ✅ delay=True reduces time the file handle is held open (helps on Windows)
        super().__init__(
            filename,
            mode="a",
            maxBytes=maxBytes,
            backupCount=backupCount,
            encoding=encoding,
            delay=delay,
        )
        self._last_date = datetime.now().date()

    def shouldRollover(self, record):
        # ---- Daily rollover check ----
        today = datetime.now().date()
        if today != self._last_date:
            return True

        # ---- Size rollover check (RotatingFileHandler base logic) ----
        return super().shouldRollover(record)

    def doRollover(self):
        # Timestamp suffix for rotated file
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        rotated_name = f"{self.baseFilename}.{timestamp}"

        # Close the stream before moving
        if self.stream:
            try:
                self.stream.close()
            except Exception:
                pass
            self.stream = None

        # Rename current file -> rotated file
        if os.path.exists(self.baseFilename):
            try:
                os.rename(self.baseFilename, rotated_name)
            except PermissionError:
                # ✅ WINDOWS LOCK SAFE: another process has the file open.
                # Skip rollover this run, reopen stream, and keep logging.
                self.stream = self._open()
                return
            except OSError:
                # ✅ Same idea for other OS-level rename failures
                self.stream = self._open()
                return

        # ✅ Only update date marker AFTER successful rollover
        self._last_date = datetime.now().date()

        # Cleanup old rotated logs (keep only backupCount)
        pattern = self.baseFilename + ".*"
        files = sorted(glob.glob(pattern), reverse=True)  # newest first

        for old in files[self.backupCount:]:
            try:
                os.remove(old)
            except Exception:
                pass

        # Reopen stream
        self.stream = self._open()


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger:
      - File rotation: daily + 5MB size
      - Keeps 5 backups
      - Console handler enabled unless LWS_NO_CONSOLE=1

    ✅ Windows-safe mode:
      - If LWS_STDOUT_ONLY=1, do NOT attach file handler (BAT/PS captures + rotates logs).
    """
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)

    # Convert "INFO"/"DEBUG" into actual logging levels
    level = getattr(logging, str(LOG_LEVEL).upper(), logging.INFO)
    logger.setLevel(level)

    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    # ==========================
    # FILE HANDLER (optional)
    # ==========================
    if os.getenv("LWS_STDOUT_ONLY") != "1":
        logfile_path = os.path.join(LOG_DIR, LOG_FILE)

        file_handler = SizeAndTimeRotatingFileHandler(
            logfile_path,
            maxBytes=5 * 1024 * 1024,  # 5MB
            backupCount=5,
            delay=True,                # ✅ important on Windows
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    # ==========================
    # CONSOLE HANDLER (UTF-8 safe)
    # ==========================
    if os.getenv("LWS_NO_CONSOLE") != "1":
        utf8_stream = io.TextIOWrapper(
            sys.stdout.buffer,
            encoding="utf-8",
            errors="replace"
        )
        console_handler = logging.StreamHandler(utf8_stream)
        console_handler.setLevel(level)
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)

    logger.propagate = False
    return logger
