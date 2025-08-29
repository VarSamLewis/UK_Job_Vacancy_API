import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from src.ingestion.create_schema import create_schema

from src.utils.logger import logger

db_pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    dbname="cerbyd_triplogger",
    user="postgres",
    password="x836vzm7dI",
    host="localhost",
    port=5432
)  # TODO: Adjust parameters to env vars


def get_connection():
    conn = db_pool.getconn()
    if not conn:
        logger.error("Failed to get connection from pool.")
        raise Exception("No available database connections.")
    logger.info("Connected to the database.")
    return conn

def return_connection(conn):
    db_pool.putconn(conn)

