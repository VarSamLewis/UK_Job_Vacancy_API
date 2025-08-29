import logging
from pathlib import Path
import sys

log_file = Path.home() / ".Job_Vacancy_API/logs/app_log.json"
log_file.parent.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("API")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"
)

# File handler (UTF-8 for Unicode safety)
file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
file_handler.setFormatter(formatter)

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

# Avoid duplicate handlers if re-imported
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
else:
    # Remove all handlers and re-add (for interactive environments)
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)