import os
import re
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

from src.utils.logger import logger

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "Data"

# ---------------------------------------------------------------------------
# Industry name normalisation
# Raw CSV column headers vary across datasets (footnote digits, hyphenated
# line-breaks, trailing spaces). This maps them to the canonical names
# seeded into dim_industry.
# ---------------------------------------------------------------------------

_INDUSTRY_NAME_MAP = {
    'All vacancies1':                                                       'All vacancies',
    'All vacancies1 ':                                                      'All vacancies',
    'Manu-    facturing':                                                   'Manufacturing',
    'Construc-tion':                                                        'Construction',
    'Electricity, gas, steam & air conditioning supply2':                   'Electricity, gas, steam & air conditioning supply',
    'Water supply, sewerage, waste & remediation activities2':              'Water supply, sewerage, waste & remediation activities',
    'Wholesale & retail trade; repair of motor vehicles and motor cycles':  'Wholesale & retail trade; repair of motor vehicles',
    'Administra-tive & support service activities':                        'Administrative & support service activities',
    'Information & communica-tion':                                         'Information & communication',
    'Real estate activities2':                                              'Real estate activities',
    'Human health & social work activities2':                               'Human health & social work activities',
    'Arts, entertainment & recreation2':                                    'Arts, entertainment & recreation',
    'Total services ':                                                      'Total services',
}

def _normalise_industry(raw: str) -> str:
    raw = raw.strip()
    if raw in _INDUSTRY_NAME_MAP:
        return _INDUSTRY_NAME_MAP[raw]
    # Strip trailing footnote digit, fix hyphenated line-breaks
    cleaned = re.sub(r'\d+$', '', raw).strip()
    cleaned = re.sub(r'(\w)-\s+(\w)', r'\1\2', cleaned)
    return cleaned


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_conn():
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", 5432)),
    )


def _clean_val(val):
    """Convert NaN, NaT, and ONS placeholder strings to None."""
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, str) and val.strip() in ('..', '.', ''):
        return None
    return val


def _upsert_period(cur, period_type, start_date, end_date, year, month, quarter_label):
    """Insert or retrieve a dim_period row, always returning its period_id."""
    cur.execute("""
        INSERT INTO dim_period (period_type, start_date, end_date, year, month, quarter_label)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (period_type, start_date, end_date) DO UPDATE
            SET period_type = EXCLUDED.period_type
        RETURNING period_id
    """, (period_type, start_date, end_date, year, month, quarter_label))
    return cur.fetchone()[0]


def _upsert_fact(cur, period_id, dataset_id, industry_id, size_id,
                 vacancies, unemployment, unemployed_per_vacancy, vacancy_rate):
    cur.execute("""
        INSERT INTO fact_vacancies
            (period_id, dataset_id, industry_id, size_id,
             vacancies_000s, unemployment_000s, unemployed_per_vacancy, vacancy_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT uq_fact_vacancies DO UPDATE SET
            vacancies_000s         = EXCLUDED.vacancies_000s,
            unemployment_000s      = EXCLUDED.unemployment_000s,
            unemployed_per_vacancy = EXCLUDED.unemployed_per_vacancy,
            vacancy_rate           = EXCLUDED.vacancy_rate
    """, (period_id, dataset_id, industry_id, size_id,
          _clean_val(vacancies), _clean_val(unemployment),
          _clean_val(unemployed_per_vacancy), _clean_val(vacancy_rate)))


def _load_dim_lookups(cur):
    cur.execute("SELECT industry_name, industry_id FROM dim_industry")
    industry_ids = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT size_label, size_id FROM dim_business_size")
    size_ids = {row[0]: row[1] for row in cur.fetchall()}

    cur.execute("SELECT dataset_code, dataset_id FROM dim_dataset")
    dataset_ids = {row[0]: row[1] for row in cur.fetchall()}

    return industry_ids, size_ids, dataset_ids


# ---------------------------------------------------------------------------
# Per-dataset insert functions
# ---------------------------------------------------------------------------

def _insert_vacs01(cur, dataset_id, all_vac_id):
    for csv_path in sorted((DATA_DIR / 'vacs01').glob('*.csv')):
        df = pd.read_csv(csv_path).dropna(how='all')
        logger.info(f"vacs01: {csv_path.name} ({len(df)} rows)")

        vac_col   = next(c for c in df.columns if 'vacancies' in c.lower())
        unemp_col = next(c for c in df.columns if 'unemployment' in c.lower())
        ratio_col = next(c for c in df.columns if 'unemployed people per' in c.lower())

        for _, row in df.iterrows():
            if pd.isna(row.get('start_mon')):
                continue

            start_date = pd.to_datetime(row['start_mon']).date()
            end_date   = pd.to_datetime(row['end_mon']).date() if _clean_val(row.get('end_mon')) else start_date
            year       = int(row['year'])
            end_char   = row.get('end_mon_char', '')
            q_label    = f"{row['start_mon_char']}-{end_char} {year}".strip('-').strip()

            period_id = _upsert_period(cur, 'quarterly_rolling', start_date, end_date, year, None, q_label)
            _upsert_fact(cur, period_id, dataset_id, all_vac_id, None,
                         row[vac_col], row[unemp_col], row[ratio_col], None)


def _insert_vacs02(cur, dataset_ids, industry_ids):
    _META_COLS = {'year', 'start_mon_char', 'end_mon_char', 'start_mon', 'end_mon'}

    for csv_path in sorted((DATA_DIR / 'vacs02').glob('*.csv')):
        name = csv_path.name.lower()
        if 'levels' in name:
            dataset_id = dataset_ids['vacs02_levels']
            is_levels  = True
        elif 'ratios' in name:
            dataset_id = dataset_ids['vacs02_ratios']
            is_levels  = False
        else:
            logger.warning(f"Skipping unrecognised vacs02 file: {csv_path.name}")
            continue

        df = pd.read_csv(csv_path).dropna(how='all')
        logger.info(f"vacs02: {csv_path.name} ({len(df)} rows)")
        industry_cols = [c for c in df.columns if c not in _META_COLS]

        for _, row in df.iterrows():
            if pd.isna(row.get('start_mon')):
                continue

            start_date = pd.to_datetime(row['start_mon']).date()
            end_date   = pd.to_datetime(row['end_mon']).date() if _clean_val(row.get('end_mon')) else start_date
            year       = int(row['year'])
            end_char   = row.get('end_mon_char', '')
            q_label    = f"{row['start_mon_char']}-{end_char} {year}".strip('-').strip()

            period_id = _upsert_period(cur, 'quarterly_rolling', start_date, end_date, year, None, q_label)

            for col in industry_cols:
                val = _clean_val(row[col])
                if val is None:
                    continue
                canonical   = _normalise_industry(col)
                industry_id = industry_ids.get(canonical)
                if industry_id is None:
                    logger.warning(f"Unknown industry '{col}' → '{canonical}' — skipping")
                    continue
                if is_levels:
                    _upsert_fact(cur, period_id, dataset_id, industry_id, None, val, None, None, None)
                else:
                    _upsert_fact(cur, period_id, dataset_id, industry_id, None, None, None, None, val)


def _insert_x06_industry(cur, csv_path, dataset_id, industry_ids):
    df = pd.read_csv(csv_path).dropna(how='all')
    logger.info(f"x06 industry: {csv_path.name} ({len(df)} rows)")
    industry_cols = [c for c in df.columns if c != 'Mon']

    for _, row in df.iterrows():
        if _clean_val(row.get('Mon')) is None:
            continue

        date      = pd.to_datetime(row['Mon'], dayfirst=True).date()
        period_id = _upsert_period(cur, 'monthly', date, date, date.year, date.month, None)

        for col in industry_cols:
            val = _clean_val(row[col])
            if val is None:
                continue
            canonical   = _normalise_industry(col)
            industry_id = industry_ids.get(canonical)
            if industry_id is None:
                logger.warning(f"Unknown industry '{col}' → '{canonical}' — skipping")
                continue
            _upsert_fact(cur, period_id, dataset_id, industry_id, None, val, None, None, None)


def _insert_x06_size(cur, csv_path, dataset_id, size_ids):
    df = pd.read_csv(csv_path).dropna(how='all')
    logger.info(f"x06 size: {csv_path.name} ({len(df)} rows)")
    # Exclude the 'All vacancies' total — already covered by x06_industry
    size_cols = [c for c in df.columns if c != 'Mon' and 'all' not in c.lower()]

    for _, row in df.iterrows():
        if _clean_val(row.get('Mon')) is None:
            continue

        date      = pd.to_datetime(row['Mon'], dayfirst=True).date()
        period_id = _upsert_period(cur, 'monthly', date, date, date.year, date.month, None)

        for col in size_cols:
            val = _clean_val(row[col])
            if val is None:
                continue
            size_id = size_ids.get(col.strip())
            if size_id is None:
                logger.warning(f"Unknown size band '{col}' — skipping")
                continue
            _upsert_fact(cur, period_id, dataset_id, None, size_id, val, None, None, None)


def _insert_x06(cur, dataset_ids, industry_ids, size_ids):
    for csv_path in sorted((DATA_DIR / 'x06').glob('*.csv')):
        name = csv_path.name.lower()
        if 'introduction' in name:
            continue
        elif 'vacancies_by_industry' in name:
            _insert_x06_industry(cur, csv_path, dataset_ids['x06_industry'], industry_ids)
        elif 'size_of_business' in name:
            _insert_x06_size(cur, csv_path, dataset_ids['x06_size'], size_ids)
        else:
            logger.warning(f"Skipping unrecognised x06 file: {csv_path.name}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def insert_all():
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info("Loading dimension lookups...")
                industry_ids, size_ids, dataset_ids = _load_dim_lookups(cur)

                logger.info("Inserting vacs01...")
                _insert_vacs01(cur, dataset_ids['vacs01'], industry_ids['All vacancies'])

                logger.info("Inserting vacs02...")
                _insert_vacs02(cur, dataset_ids, industry_ids)

                logger.info("Inserting x06...")
                _insert_x06(cur, dataset_ids, industry_ids, size_ids)

        logger.info("All data inserted successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    insert_all()
