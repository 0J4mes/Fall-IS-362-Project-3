# setup_database.py
import sqlite3
import requests
import os


def download_chinook_database():
    """Download Chinook database directly from GitHub"""
    print("Downloading Chinook database...")

    # Direct link to the Chinook SQLite database
    chinook_url = "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
    local_db_path = "Chinook_Sqlite.sqlite"

    try:
        # Download the SQLite file directly
        response = requests.get(chinook_url, stream=True, timeout=60)
        response.raise_for_status()

        # Get total file size for progress tracking
        total_size = int(response.headers.get('content-length', 0))

        with open(local_db_path, 'wb') as f:
            if total_size == 0:
                # No content length header
                f.write(response.content)
            else:
                # Show progress for larger download
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    downloaded += len(chunk)
                    f.write(chunk)
                    # Show progress
                    done = int(50 * downloaded / total_size)
                    print(f"\r[{'=' * done}{' ' * (50 - done)}] {downloaded}/{total_size} bytes", end='', flush=True)

        print(f"\nDownload completed: {local_db_path}")
        return local_db_path

    except Exception as e:
        print(f"\nError downloading database: {e}")
        return None


def verify_chinook_database(db_path):
    """Verify that the database contains Chinook tables and data"""
    print(f"Verifying database: {db_path}")

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check for essential Chinook tables
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name IN ('Customer', 'Invoice', 'Track', 'Album', 'Artist', 'Employee', 'Genre', 'InvoiceLine')
        """)
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]

        print("Found tables:", ", ".join(table_names))

        # Verify we have the core tables needed for our analysis
        required_tables = ['Customer', 'Invoice', 'InvoiceLine', 'Track', 'Album']
        missing_tables = [table for table in required_tables if table not in table_names]

        if missing_tables:
            print(f"Missing required tables: {missing_tables}")
            conn.close()
            return False

        # Check if tables have data
        print("Checking table row counts:")
        for table in required_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  - {table}: {count} rows")

        conn.close()
        print("Database verification successful!")
        return True

    except Exception as e:
        print(f"Error verifying database: {e}")
        return False


def setup_database():
    """Main setup function"""
    print("=" * 60)
    print("CHINOOK DATABASE SETUP")
    print("=" * 60)

    db_path = "Chinook_Sqlite.sqlite"

    # Check if we already have a valid Chinook database
    if os.path.exists(db_path):
        print(f"Found existing database: {db_path}")
        if verify_chinook_database(db_path):
            print("Using existing Chinook database")
            return db_path
        else:
            print("Existing database is invalid. Downloading fresh copy...")
            os.remove(db_path)

    # Download fresh database
    print("Downloading Chinook database from:")
    print("https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite")

    db_path = download_chinook_database()

    if db_path and verify_chinook_database(db_path):
        print("Chinook database setup completed successfully!")
        return db_path
    else:
        print("Failed to setup Chinook database")
        print("\nTroubleshooting steps:")
        print("1. Check your internet connection")
        print("2. Manually download from the link above")
        print("3. Place the file in this directory as 'Chinook_Sqlite.sqlite'")
        return None


if __name__ == "__main__":
    result = setup_database()
    if result:
        print(f"\nSUCCESS: Database ready at '{result}'")
        print("\nYou can now run:")
        print("  python chinook_python_centric.py")
        print("  python main.py")
    else:
        print("\nFAILED: Database setup unsuccessful")