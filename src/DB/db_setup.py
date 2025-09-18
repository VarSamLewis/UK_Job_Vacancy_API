import psycopg2
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to PostgreSQL server (not to a specific database)
conn = psycopg2.connect(
    user='postgres',
    password=os.getenv("PGSQL_SU_PW"),
    host='localhost',
    port=5432
)

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'UK_Job_Vacancy_API'")
    exists = cur.fetchone()
    
    if not exists:
        cur.execute('CREATE DATABASE "UK_Job_Vacancy_API"')
        print("Database 'UK_Job_Vacancy_API' created successfully")
    else:
        print("Database 'UK_Job_Vacancy_API' already exists")


with open('src/DB/schema.py', 'r', encoding='utf-8-sig') as f:
    exec(f.read())

with conn.cursor() as cur:
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = cur.fetchall()
    print("Tables created:")
    for table in tables:
        print(f"  - {table[0]}")

conn.close()
