import os
from pathlib import Path
from typing import List
from src.utils.logger import logger

def flush_xlsx_files(folder: Path, file_formats: List[str]):
    files = [file for file in os.listdir(folder)
    if (
        # file.endswith('.xlsx') or file.endswith('.xls'))
          [format for format in file_formats if file.endswith(format)]
         )
    ]


    for file in files:
        os.remove(os.path.join(folder, file))

def validate_folder_empty(folder: Path, file_formats: List[str]):
    files = [file for file in os.listdir(folder) if (
        # file.endswith('.xlsx') or file.endswith('.xls'))
          [format for format in file_formats if file.endswith(format)]
         )
    ]
    if files:
        logger.error(f"Folder {folder} is not empty")
        raise ValueError(f"Folder {folder} is not empty")
    else:
        logger.info(f"Folder {folder} is empty")
        return True

def main():
    logger.info("Flush script started")
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    DATA_DIR = BASE_DIR / "data"
    file_formats = ['.xlsx', '.xls']

    logger.info(f"Flushing {', '.join(file_formats)} files from {DATA_DIR}")

    flush_xlsx_files(DATA_DIR, file_formats)
    if validate_folder_empty(DATA_DIR, file_formats):
        logger.info(f"Folder {DATA_DIR} has been cleared of {', '.join(file_formats)} files")

    logger.info("Flush script completed")

if __name__ == "__main__":
    main()
