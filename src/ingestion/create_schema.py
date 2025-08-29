import pandas as pd
from pathlib import Path
import os

from src.utils.logger import logger

def read_cleaned_csv(file_path: Path) -> pd.DataFrame:
    """
    Read a cleaned CSV file into a Pandas DataFrame.
    """
    logger.info(f"Reading cleaned CSV file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded DataFrame shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV file {file_path}: {e}")
        return pd.DataFrame()

def batch_read_csv(folder_path: Path, prefixes: list[str], suffixes_to_exclude: list[str] = None) -> dict[str, list[pd.DataFrame]]:
    """
    For each prefix, read all cleaned CSV files in a folder, returning a dict of prefix -> list of DataFrames.
    No formatting or schema logic is applied here.
    """
    logger.info(f"Starting batch CSV read in folder: {folder_path} for prefixes: {prefixes}")
    results: dict[str, list[pd.DataFrame]] = {}

    for prefix in prefixes:
        logger.info(f"Searching for CSV files with prefix: {prefix}")
        dataframes = []
        files = [
            f for f in os.listdir(folder_path)
            if os.path.isfile(folder_path / f)
            and f.startswith(prefix)
            and f.endswith(".csv")
            and not any(f.endswith(suf + ".csv") for suf in (suffixes_to_exclude or []))
        ]
        logger.info(f"Found {len(files)} CSV files for prefix '{prefix}': {files}")

        for f in files:
            file_path = folder_path / f
            logger.info(f"Processing CSV file: {file_path}")
            df = read_cleaned_csv(file_path)
            if df.empty:
                logger.warning(f"DataFrame for file {file_path} is empty. Skipping.")
                continue
            dataframes.append(df)
            logger.info(f"Appended DataFrame for file: {file_path}")

        results[prefix] = dataframes
        logger.info(f"Completed processing for prefix '{prefix}'. Total DataFrames: {len(dataframes)}")

    logger.info("Batch CSV read complete.")
    return results

def main_ingestion(folder_path):
    prefixes = ["jobs3", "x06"]
    suffixes = ["_1"]
    logger.info("Starting main ingestion process for CSVs.")
    grouped_dataframes = batch_read_csv(folder_path, prefixes, suffixes)

    for prefix, dfs in grouped_dataframes.items():
        logger.info(f"Loaded {len(dfs)} CSV files for prefix '{prefix}'")
        print(f"✅ Loaded {len(dfs)} CSV files for prefix '{prefix}'")
        # DataFrame-specific logic goes here, e.g. formatting for DB
        for i, df in enumerate(dfs):
            logger.debug(f"Previewing DataFrame {i+1} for prefix '{prefix}':\n{df.head()}")
            print(f"\nPreview {prefix} file {i+1}:")
            print(df.head())
    logger.info("Main ingestion process complete.")

def create_schema(dfs) -> dict[str, list[pd.DataFrame]]:
    
    return result
    





if __name__ == "__main__":
    folder = Path(r"C:\Users\samle\Source\Repos\UK_Job_Vacancy_API\Data")
    dfs = main_ingestion(folder)
    create_schema(dfs)


