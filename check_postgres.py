"""Explicit local empty-password connection diagnostic; never probes passwords."""

import psycopg2
from psycopg2 import OperationalError

def check_local_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="",
            host="localhost",
            port="5432"
        )
        conn.close()
        return True
    except OperationalError:
        return False

if __name__ == "__main__":
    if check_local_connection():
        print("Local PostgreSQL connection succeeded with an empty password argument.")
    else:
        print("Local PostgreSQL connection failed; no other passwords were attempted.")
        raise SystemExit(1)
