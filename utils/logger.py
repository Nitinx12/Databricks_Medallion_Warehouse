import logging
import os
from datetime import datetime, timedelta

# Anchor logs/ to the project root, not the current working directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_ROOT = os.path.join(BASE_DIR, "logs")

VALID_STAGES = ["extraction", "transformation", "loading"]


def get_logger(stage: str, name: str, retention_days: int = 14) -> logging.Logger:
    stage = stage.lower()
    if stage not in VALID_STAGES:
        raise ValueError(f"Invalid stage '{stage}'. Must be one of: {VALID_STAGES}")

    # logs/<stage>/ folder
    log_dir = os.path.join(LOGS_ROOT, stage)
    os.makedirs(log_dir, exist_ok=True)

    # Drop old logs so this doesn't grow forever under a scheduler
    _cleanup_old_logs(log_dir, retention_days)

    # Log file: logs/<stage>/<name>_2024-06-01_12-00-00.log
    run_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(log_dir, f"{name}_{run_time}.log")

    # Unique logger key per stage+name
    logger_key = f"{stage}.{name}"
    logger = logging.getLogger(logger_key)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # Avoid duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console: INFO and above
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(fmt)

    # File: DEBUG and above
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def _cleanup_old_logs(log_dir: str, retention_days: int) -> None:
    """Delete log files older than retention_days from log_dir."""
    cutoff = datetime.now() - timedelta(days=retention_days)
    for filename in os.listdir(log_dir):
        file_path = os.path.join(log_dir, filename)
        if not os.path.isfile(file_path):
            continue
        modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        if modified_time < cutoff:
            os.remove(file_path)