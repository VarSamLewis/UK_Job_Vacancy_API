import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv
from src.utils.logger import logger

load_dotenv()

db_pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    dbname=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", 5432)),
)


def get_connection():
    conn = db_pool.getconn()
    if not conn:
        logger.error("Failed to get connection from pool.")
        raise Exception("No available database connections.")
    logger.info("Connected to the database.")
    return conn

def return_connection(conn):
    db_pool.putconn(conn)

