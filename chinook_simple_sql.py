# chinook_simple_sql.py
import pandas as pd
from sqlalchemy import create_engine, text
import os


def simple_sql_solution():
    """Simple solution using SQL query (for comparison)"""

    # Database connection
    database_path = 'Chinook_Sqlite.sqlite'

    if not os.path.exists(database_path):
        print(f"Database file not found: {database_path}")
        print("Please run setup_database.py first")
        return None

    engine = create_engine(f'sqlite:///{database_path}')

    try:
        # Use connection context manager
        with engine.connect() as connection:
            # SQL query that does the complex joining
            sql_query = """
            SELECT 
                c.LastName,
                c.FirstName,
                t.Name AS Name,
                a.Title AS Title
            FROM Customer c
            JOIN Invoice i ON c.CustomerId = i.CustomerId
            JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
            JOIN Track t ON il.TrackId = t.TrackId
            JOIN Album a ON t.AlbumId = a.AlbumId
            ORDER BY c.LastName, c.FirstName
            """

            # Execute query with connection
            df = pd.read_sql(text(sql_query), connection)

            print("Simple SQL Solution Results:")
            print("First 10 rows:")
            print(df.head(10))
            print(f"\nTotal rows: {len(df)}")

            # Save results
            df.to_csv('customer_tracks_simple.csv', index=False)
            print("Results saved to 'customer_tracks_simple.csv'")

            return df

    except Exception as e:
        print(f"Error: {e}")
        return None

    finally:
        # Ensure engine is disposed
        engine.dispose()


if __name__ == "__main__":
    simple_sql_solution()