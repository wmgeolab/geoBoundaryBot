import psycopg2
from psycopg2 import sql

# Connection parameters
DB_HOST = "geoboundaries-postgres-service"  # Service name resolves to the ClusterIP
DB_PORT = "5432"                           # Default PostgreSQL port
DB_NAME = "geoboundaries"                  # Database name
DB_USER = "geoboundaries"                  # Username
DB_PASSWORD = ""                           # Empty password since host auth is trust

def test_postgis_connection():
    try:
        # Establish connection
        print("Connecting to the PostGIS database...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connection successful!")

        # Create a cursor object
        cursor = conn.cursor()

        # Test if PostGIS extension is enabled
        cursor.execute("SELECT PostGIS_Version();")
        postgis_version = cursor.fetchone()
        if postgis_version:
            print(f"PostGIS Version: {postgis_version[0]}")
        else:
            print("Failed to fetch PostGIS version. Is PostGIS enabled?")

        # Test a basic query
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        print(f"Connected to database: {db_name}")

        # Close the cursor and connection
        cursor.close()
        conn.close()
        print("Connection closed successfully.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_postgis_connection()
