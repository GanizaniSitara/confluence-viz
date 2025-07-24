import psycopg2
from psycopg2 import OperationalError

def try_password(pw):
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

# Try a list of passwords
for candidate in ["pass1", "pass2", "correct_password"]:
    if try_password(candidate):
        print(f"✅ Password OK: {candidate}")
        break