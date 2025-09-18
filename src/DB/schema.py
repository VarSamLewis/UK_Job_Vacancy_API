import os
import psycopg2

conn = psycopg2.connect(
	dbname="UK_Job_Vacancy_API",
	user='postgres',
	password=os.getenv("PGSQL_SU_PW"),
	host='localhost',
	port = 5432
)
with conn.cursor() as cur:
    # Simplified time dimensions with embedded quarter info in months
    cur.execute("""
    CREATE TABLE IF NOT EXISTS quarters (
        quarter_id  SERIAL PRIMARY KEY,
        year        INTEGER NOT NULL,
        quarter     INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
        start_date  DATE NOT NULL,
        end_date    DATE NOT NULL,
        year_quarter TEXT GENERATED ALWAYS AS (year || '-Q' || quarter) STORED,
        CONSTRAINT uq_quarters UNIQUE (year, quarter)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS months (
        month_id    SERIAL PRIMARY KEY,
        year        INTEGER NOT NULL,
        month       INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
        quarter     INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
        start_date  DATE NOT NULL,
        end_date    DATE NOT NULL,
        year_month  TEXT GENERATED ALWAYS AS (year || '-' || LPAD(month::TEXT, 2, '0')) STORED,
        year_quarter TEXT GENERATED ALWAYS AS (year || '-Q' || quarter) STORED,
        CONSTRAINT uq_months UNIQUE (year, month)
    );
    """)

    # DENORMALIZED: Embed industry/size info directly in fact tables
    
    # VACS01 (quarter-level) - keep simple, one row per quarter
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacs01_quarter (
        quarter_id                 INTEGER PRIMARY KEY REFERENCES quarters(quarter_id),
        year                       INTEGER NOT NULL,
        quarter                    INTEGER NOT NULL,
        year_quarter               TEXT NOT NULL,
        all_vacancies              INTEGER,
        unemployment               INTEGER,
        unemployed_per_vacancy     NUMERIC(5,2)
    );
    """)

    # VACS02 ratios - DENORMALIZED with industry names embedded
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacs02_ratios_quarter (
        quarter_id      INTEGER NOT NULL REFERENCES quarters(quarter_id),
        year            INTEGER NOT NULL,
        quarter         INTEGER NOT NULL,
        year_quarter    TEXT NOT NULL,
        industry_code   TEXT,
        industry_name   TEXT NOT NULL,
        is_total        BOOLEAN DEFAULT FALSE,
        ratio           NUMERIC(5,2),
        PRIMARY KEY (quarter_id, industry_name)
    );
    """)

    # VACS02 levels - DENORMALIZED with industry names embedded
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacs02_levels_quarter (
        quarter_id      INTEGER NOT NULL REFERENCES quarters(quarter_id),
        year            INTEGER NOT NULL,
        quarter         INTEGER NOT NULL,
        year_quarter    TEXT NOT NULL,
        industry_code   TEXT,
        industry_name   TEXT NOT NULL,
        is_total        BOOLEAN DEFAULT FALSE,
        level           NUMERIC(8,1),
        PRIMARY KEY (quarter_id, industry_name)
    );
    """)

    # VACS03 headline - with embedded time info
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacs03_quarter_headline (
        quarter_id           INTEGER PRIMARY KEY REFERENCES quarters(quarter_id),
        year                 INTEGER NOT NULL,
        quarter              INTEGER NOT NULL,
        year_quarter         TEXT NOT NULL,
        all_vacancies        INTEGER,
        change_on_quarter    INTEGER,
        percentage_change    NUMERIC(5,2)
    );
    """)

    # VACS03 by size - DENORMALIZED with size class info embedded
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vacs03_quarter_by_size (
        quarter_id      INTEGER NOT NULL REFERENCES quarters(quarter_id),
        year            INTEGER NOT NULL,
        quarter         INTEGER NOT NULL,
        year_quarter    TEXT NOT NULL,
        size_label      TEXT NOT NULL,
        min_employees   INTEGER,
        max_employees   INTEGER,
        sort_order      INTEGER NOT NULL,
        vacancies       INTEGER,
        PRIMARY KEY (quarter_id, size_label)
    );
    """)

    # x06 month by industry - DENORMALIZED
    cur.execute("""
    CREATE TABLE IF NOT EXISTS x06_month_by_industry (
        month_id        INTEGER NOT NULL REFERENCES months(month_id),
        year            INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        quarter         INTEGER NOT NULL,
        year_month      TEXT NOT NULL,
        year_quarter    TEXT NOT NULL,
        industry_code   TEXT,
        industry_name   TEXT NOT NULL,
        is_total        BOOLEAN DEFAULT FALSE,
        vacancies       INTEGER,
        PRIMARY KEY (month_id, industry_name)
    );
    """)

    # x06 month by size - DENORMALIZED
    cur.execute("""
    CREATE TABLE IF NOT EXISTS x06_month_by_size (
        month_id        INTEGER NOT NULL REFERENCES months(month_id),
        year            INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        quarter         INTEGER NOT NULL,
        year_month      TEXT NOT NULL,
        year_quarter    TEXT NOT NULL,
        size_label      TEXT NOT NULL,
        min_employees   INTEGER,
        max_employees   INTEGER,
        sort_order      INTEGER NOT NULL,
        vacancies       NUMERIC(8,1),
        PRIMARY KEY (month_id, size_label)
    );
    """)

    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quarters_year_desc ON quarters(year DESC, quarter DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_months_year_desc ON months(year DESC, month DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_months_year_quarter ON months(year_quarter);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_quarters_year_quarter ON quarters(year_quarter);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs02_ratios_industry ON vacs02_ratios_quarter(industry_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs02_levels_industry ON vacs02_levels_quarter(industry_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_x06_industry_name ON x06_month_by_industry(industry_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs03_size_order ON vacs03_quarter_by_size(sort_order);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_x06_size_order ON x06_month_by_size(sort_order);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs01_year ON vacs01_quarter(year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs02_ratios_year ON vacs02_ratios_quarter(year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs02_levels_year ON vacs02_levels_quarter(year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs03_headline_year ON vacs03_quarter_headline(year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vacs03_size_year ON vacs03_quarter_by_size(year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_x06_industry_year ON x06_month_by_industry(year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_x06_size_year ON x06_month_by_size(year);")

    conn.commit()

