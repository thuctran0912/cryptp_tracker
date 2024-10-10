import os
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv('config/config.env')

def test_snowflake_connection():
    # Debug: Print environment variables (Be careful not to log the actual password)
    print(f"SNOWFLAKE_ACCOUNT: {'Set' if os.getenv('SNOWFLAKE_ACCOUNT') else 'Not Set'}")
    print(f"SNOWFLAKE_USER: {'Set' if os.getenv('SNOWFLAKE_USER') else 'Not Set'}")
    print(f"SNOWFLAKE_PASSWORD: {'Set' if os.getenv('SNOWFLAKE_PASSWORD') else 'Not Set'}")

    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
        )

        cur = conn.cursor()
        cur.execute("SELECT current_version()")
        one_row = cur.fetchone()
        print(f"Successfully connected to Snowflake. Version: {one_row[0]}")

    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    test_snowflake_connection()