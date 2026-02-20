import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_period (
    period_id     SERIAL PRIMARY KEY,
    period_type   VARCHAR(20)  NOT NULL CHECK (period_type IN ('monthly', 'quarterly_rolling')),
    start_date    DATE         NOT NULL,
    end_date      DATE         NOT NULL,
    year          INTEGER      NOT NULL,
    month         INTEGER      CHECK (month BETWEEN 1 AND 12),   -- NULL for quarterly rows
    quarter_label VARCHAR(20),                                    -- e.g. 'May-Jul 2021', NULL for monthly rows
    UNIQUE (period_type, start_date, end_date)
);

CREATE TABLE IF NOT EXISTS dim_industry (
    industry_id    SERIAL PRIMARY KEY,
    industry_name  VARCHAR(120) NOT NULL UNIQUE,
    industry_group VARCHAR(60),
    notes          TEXT         -- captures cross-dataset discrepancies
);

CREATE TABLE IF NOT EXISTS dim_business_size (
    size_id        SERIAL PRIMARY KEY,
    size_label     VARCHAR(40)  NOT NULL UNIQUE,
    min_employees  INTEGER      NOT NULL,
    max_employees  INTEGER                       -- NULL = open-ended top band
);

CREATE TABLE IF NOT EXISTS dim_dataset (
    dataset_id             SERIAL PRIMARY KEY,
    dataset_code           VARCHAR(30)  NOT NULL UNIQUE,
    description            TEXT,
    is_seasonally_adjusted BOOLEAN      NOT NULL,
    period_type            VARCHAR(20)  NOT NULL CHECK (period_type IN ('monthly', 'quarterly_rolling'))
);

-- Single fact table. industry_id and size_id are mutually exclusive per row
-- (one will always be NULL), captured via dim_dataset context.
-- Requires PostgreSQL 15+ for NULLS NOT DISTINCT.
CREATE TABLE IF NOT EXISTS fact_vacancies (
    id                     SERIAL PRIMARY KEY,
    period_id              INTEGER      NOT NULL REFERENCES dim_period(period_id),
    dataset_id             INTEGER      NOT NULL REFERENCES dim_dataset(dataset_id),
    industry_id            INTEGER               REFERENCES dim_industry(industry_id),
    size_id                INTEGER               REFERENCES dim_business_size(size_id),
    vacancies_000s         NUMERIC(8,1),
    unemployment_000s      NUMERIC(8,1),          -- vacs01 only
    unemployed_per_vacancy NUMERIC(5,2),           -- vacs01 only
    vacancy_rate           NUMERIC(5,2),           -- vacs02_ratios only
    CONSTRAINT uq_fact_vacancies UNIQUE NULLS NOT DISTINCT (period_id, dataset_id, industry_id, size_id)
);
"""

# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

SEED_SQL = """
INSERT INTO dim_dataset (dataset_code, description, is_seasonally_adjusted, period_type)
VALUES
    ('vacs01',        'Total UK vacancies and unemployment (VACS01)',         TRUE,  'quarterly_rolling'),
    ('vacs02_levels', 'Vacancies by industry - absolute levels (VACS02)',     TRUE,  'quarterly_rolling'),
    ('vacs02_ratios', 'Vacancies by industry - vacancy rates (VACS02)',       TRUE,  'quarterly_rolling'),
    ('x06_industry',  'Single-month vacancies by industry (X06)',             FALSE, 'monthly'),
    ('x06_size',      'Single-month vacancies by business size (X06)',        FALSE, 'monthly')
ON CONFLICT (dataset_code) DO NOTHING;

INSERT INTO dim_industry (industry_name, industry_group, notes)
VALUES
    ('All vacancies',                                             NULL,         NULL),
    ('Mining & quarrying',                                        'Production', NULL),
    ('Manufacturing',                                             'Production', NULL),
    ('Electricity, gas, steam & air conditioning supply',         'Production', 'Suppressed in some periods'),
    ('Water supply, sewerage, waste & remediation activities',    'Production', 'Suppressed in some periods'),
    ('Construction',                                              'Production', NULL),
    -- Quarterly datasets group these three together; x06 splits them out
    ('Wholesale & retail trade; repair of motor vehicles',        'Services',   'Quarterly (vacs02) only - see Motor Trades, Wholesale, Retail for monthly equivalent'),
    ('Motor Trades',                                              'Services',   'Monthly (x06) only - sub-category of Wholesale & retail trade'),
    ('Wholesale',                                                 'Services',   'Monthly (x06) only - sub-category of Wholesale & retail trade'),
    ('Retail',                                                    'Services',   'Monthly (x06) only - sub-category of Wholesale & retail trade'),
    ('Transport & storage',                                       'Services',   NULL),
    ('Accommodation & food service activities',                   'Services',   NULL),
    ('Information & communication',                               'Services',   NULL),
    ('Financial & insurance activities',                          'Services',   NULL),
    ('Real estate activities',                                    'Services',   'Suppressed in some periods'),
    ('Professional scientific & technical activities',            'Services',   NULL),
    ('Administrative & support service activities',               'Services',   NULL),
    ('Public admin & defence; compulsory social security',        'Services',   NULL),
    ('Education',                                                 'Services',   NULL),
    ('Human health & social work activities',                     'Services',   'Suppressed in some periods'),
    ('Arts, entertainment & recreation',                          'Services',   'Suppressed in some periods'),
    ('Other service activities',                                  'Services',   NULL),
    ('Total services',                                            'Services',   'Quarterly (vacs02) only - aggregate subtotal of all service industries')
ON CONFLICT (industry_name) DO NOTHING;

INSERT INTO dim_business_size (size_label, min_employees, max_employees)
VALUES
    ('1 - 9 employed',       1,    9),
    ('10 - 49 employed',     10,   49),
    ('50 - 249 employed',    50,   249),
    ('250 - 2,499 employed', 250,  2499),
    ('2,500 + employed',     2500, NULL)
ON CONFLICT (size_label) DO NOTHING;
"""

# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def create_schema():
    conn = psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", 5432)),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
                cur.execute(SEED_SQL)
        print("Schema created and seed data inserted.")
    finally:
        conn.close()


if __name__ == "__main__":
    create_schema()
