import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from src.utils.logger import logger


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        dbname="UK_Job_Vacancy_API",
        user='postgres',
        password=os.getenv("PGSQL_SU_PW"),
        host='localhost',
        port=5432
    )


def calculate_quarter_from_date(start_date):
    """Calculate quarter from start date"""
    if pd.isna(start_date):
        return None
    return (start_date.month - 1) // 3 + 1


def insert_time_dimensions_vacs01(conn, df):
    """Insert time dimension data for VACS01 (3-month rolling periods as quarters)"""
    with conn.cursor() as cur:
        quarter_data = set()
        
        for _, row in df.iterrows():
            year = int(row['year']) if pd.notna(row['year']) else None
            start_date = pd.to_datetime(row['start_mon'], errors='coerce')
            end_date = pd.to_datetime(row['end_mon'], errors='coerce')
            
            if not all([year, pd.notna(start_date), pd.notna(end_date)]):
                continue
            
            # Calculate quarter from start date
            quarter = calculate_quarter_from_date(start_date)
            if quarter:
                quarter_data.add((year, quarter, start_date.date(), end_date.date()))
        
        # Insert quarters
        if quarter_data:
            execute_values(
                cur,
                """INSERT INTO quarters (year, quarter, start_date, end_date) 
                   VALUES %s ON CONFLICT (year, quarter) DO NOTHING""",
                list(quarter_data)
            )
            logger.info(f"Inserted {len(quarter_data)} quarter records")


def insert_vacs01_data(conn, df):
    """Insert VACS01 quarter data into vacs01_quarter table"""
    with conn.cursor() as cur:
        # Get quarter_ids lookup
        cur.execute("SELECT quarter_id, year, quarter FROM quarters")
        quarter_lookup = {(row[1], row[2]): row[0] for row in cur.fetchall()}
        
        data = []
        for _, row in df.iterrows():
            # Parse basic info
            year = int(row['year']) if pd.notna(row['year']) else None
            start_date = pd.to_datetime(row['start_mon'], errors='coerce')
            
            if not year or pd.isna(start_date):
                continue
                
            quarter = calculate_quarter_from_date(start_date)
            if not quarter:
                continue
                
            quarter_id = quarter_lookup.get((year, quarter))
            if not quarter_id:
                continue
                
            # Map CSV columns to database columns
            all_vacancies = int(row['All Vacancies1 (thousands)']) if pd.notna(row['All Vacancies1 (thousands)']) else None
            unemployment = int(row['Unemployment2 (thousands)']) if pd.notna(row['Unemployment2 (thousands)']) else None
            unemployed_per_vacancy = float(row['Number of unemployed people per vacancy']) if pd.notna(row['Number of unemployed people per vacancy']) else None
            
            year_quarter = f"{year}-Q{quarter}"
            
            data.append((quarter_id, year, quarter, year_quarter, all_vacancies, unemployment, unemployed_per_vacancy))
        
        if data:
            execute_values(
                cur,
                """INSERT INTO vacs01_quarter 
                   (quarter_id, year, quarter, year_quarter, all_vacancies, unemployment, unemployed_per_vacancy) 
                   VALUES %s ON CONFLICT (quarter_id) DO UPDATE SET
                   all_vacancies = EXCLUDED.all_vacancies,
                   unemployment = EXCLUDED.unemployment,
                   unemployed_per_vacancy = EXCLUDED.unemployed_per_vacancy""",
                data
            )
            logger.info(f"Inserted {len(data)} VACS01 records")


def ingest_vacs01_csv(file_path):
    """Main function to ingest VACS01 CSV file"""
    conn = get_db_connection()
    
    try:
        conn.autocommit = False  # Use transactions
        
        # Read CSV
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read {file_path} with {len(df)} rows")
        
        # Insert time dimensions first
        insert_time_dimensions_vacs01(conn, df)
        
        # Insert VACS01 data
        insert_vacs01_data(conn, df)
        
        conn.commit()
        logger.info(f"Successfully processed {file_path}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error processing {file_path}: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    # Test with your VACS01 file
    file_path = "Data/vacs01/vacs01aug2025_VACS01.csv"
    ingest_vacs01_csv(file_path)