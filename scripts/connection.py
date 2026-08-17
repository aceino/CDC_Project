import os 
import psycopg2 
from dotenv import load_dotenv

load_dotenv()  

DB_CONFIG = { 
    "host" : os.getenv("PG_HOST"),
    "port" : os.getenv("PG_PORT"),
    "dbname" : os.getenv("PG_DB"),
    "user" : os.getenv("PG_USER"),
    "password" : os.getenv("PG_PASSWORD"),
}

def get_connection() : 
    conn = psycopg2.connect(**DB_CONFIG) 
    conn.autocommit = True 
    return conn 