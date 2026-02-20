import pandas as pd
import numpy as np

from src.utils.logger import logger


def convert_xls_to_xlsx(folder: str, output_folder: str = None):
    """
    Convert all .xls files in a folder to .xlsx format.
    Args:
        folder: Source folder containing .xls files.
        output_folder: Destination folder for .xlsx files (defaults to source folder).
    """
    import os
    if output_folder is None:
        output_folder = folder

    for filename in os.listdir(folder):
        if filename.lower().endswith('.xls') and not filename.lower().endswith('.xlsx'):
            xls_path = os.path.join(folder, filename)
            xlsx_filename = os.path.splitext(filename)[0] + '.xlsx'
            xlsx_path = os.path.join(output_folder, xlsx_filename)
            try:
                xls = pd.ExcelFile(xls_path, engine='xlrd')
                with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
                    for sheet_name in xls.sheet_names:
                        df = xls.parse(sheet_name)
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"Converted {xls_path} -> {xlsx_path}")
            except Exception as e:
                print(f"Failed to convert {xls_path}: {e}")


def _clean_column_names(cols: list[str]) -> list[str]:
    """Standardize column names to lowercase, snake_case, no spaces."""
    logger.info(f"Cleaning columns: {cols}")
    cleaned = [
        c.strip().lower().replace(" ", "_").replace("-", "_")
        for c in cols
    ]
    logger.debug(f"Cleaned columns: {cleaned}")
    return cleaned


def _apply_common_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are completely empty or contain only placeholders.
    Rules Applied:
    1. Remove rows that are completely empty or contain only NaN values.
    2. Remove columns that are completely empty or contain only NaN values.
    3. Reset the DataFrame index after row removals.
    4. Clean column names to be lowercase and snake_case.
    5. Trim whitespace from string entries in the DataFrame.
    6. Replace common placeholders for missing values (e.g., '', 'n/a', '-', '--') with NaN.
    7. Remove duplicate rows.
    8. Convert columns with numeric data stored as strings to appropriate numeric types.
    """
    logger.debug(f"DF shape before common rules applied: {df.shape}")
    # Rule 1
    df.dropna(how='all', inplace=True)
    # Rule 2
    df.dropna(axis=1, how='all', inplace=True)
    # Rule 3
    df.reset_index(drop=True, inplace=True)
    # Rule 4
    df.columns = _clean_column_names(df.columns)
    # Rule 5
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    # Rule 6
    missing_values = ['', 'n/a', 'na', '-', '--']
    df.replace(missing_values, np.nan, inplace=True)
    # Rule 7
    df = df.drop_duplicates()
    # Rule 8
    for col in df.select_dtypes(include='object').columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            pass
    if 'unnamed: 0' in df.columns:
        df.drop(columns=['unnamed: 0'], inplace=True)
    logger.debug(f"DF shape after common rules applied: {df.shape}")

    return df
