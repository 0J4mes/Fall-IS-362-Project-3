# chinook_python_centric.py
import pandas as pd
from sqlalchemy import create_engine
import os


class ChinookAnalyzer:
    def __init__(self, database_path='Chinook_Sqlite.sqlite'):
        """Initialize with database connection"""
        self.database_path = database_path
        self.engine = None
        self.connection = None
        self.tables = {}

    def connect_to_database(self):
        """Establish database connection"""
        try:
            if not os.path.exists(self.database_path):
                print(f"Database file not found: {self.database_path}")
                print("Please run setup_database.py first")
                return False

            self.engine = create_engine(f'sqlite:///{self.database_path}')
            self.connection = self.engine.connect()
            print("Database connection established successfully!")
            return True

        except Exception as e:
            print(f"Error connecting to database: {e}")
            return False

    def load_all_tables(self):
        """Load all relevant tables into pandas DataFrames"""
        if not self.connection:
            print("No database connection available")
            return False

        try:
            print("Loading database tables...")

            # Load each table with connection (not engine)
            self.tables['customers'] = pd.read_sql('SELECT * FROM Customer', self.connection)
            self.tables['invoices'] = pd.read_sql('SELECT * FROM Invoice', self.connection)
            self.tables['invoice_items'] = pd.read_sql('SELECT * FROM InvoiceLine', self.connection)
            self.tables['tracks'] = pd.read_sql('SELECT * FROM Track', self.connection)
            self.tables['albums'] = pd.read_sql('SELECT * FROM Album', self.connection)

            print("All tables loaded successfully!")
            self.display_table_info()
            return True

        except Exception as e:
            print(f"Error loading tables: {e}")
            return False

    def display_table_info(self):
        """Display information about loaded tables"""
        print("\nTable Information:")
        for table_name, df in self.tables.items():
            print(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")

    def merge_data_python(self):
        """
        Perform complex data merging using Python/pandas operations
        This replaces the complex SQL join with Python logic
        """
        print("\nPerforming data merging with Python...")

        # Start with customers and invoices
        print("Step 1: Merging customers with invoices...")
        customer_invoices = pd.merge(
            self.tables['customers'][['CustomerId', 'FirstName', 'LastName']],
            self.tables['invoices'][['InvoiceId', 'CustomerId']],
            on='CustomerId',
            how='inner'
        )
        print(f"  Customer-Invoice relationships: {len(customer_invoices)}")

        # Add invoice items
        print("Step 2: Adding invoice items...")
        customer_invoice_items = pd.merge(
            customer_invoices,
            self.tables['invoice_items'][['InvoiceLineId', 'InvoiceId', 'TrackId']],
            on='InvoiceId',
            how='inner'
        )
        print(f"  Customer-Invoice-Track relationships: {len(customer_invoice_items)}")

        # Add track information
        print("Step 3: Adding track information...")
        tracks_info = self.tables['tracks'][['TrackId', 'Name', 'AlbumId']]
        customer_tracks = pd.merge(
            customer_invoice_items,
            tracks_info,
            on='TrackId',
            how='inner'
        )
        print(f"  Customer-Track relationships: {len(customer_tracks)}")

        # Add album information
        print("Step 4: Adding album information...")
        albums_info = self.tables['albums'][['AlbumId', 'Title']]
        final_data = pd.merge(
            customer_tracks,
            albums_info,
            on='AlbumId',
            how='inner'
        )
        print(f"  Final dataset size: {len(final_data)} rows")

        return final_data

    def clean_and_format_data(self, df):
        """
        Clean and format the final DataFrame according to requirements
        """
        print("\nFormatting final output...")

        # Select and rename columns to match required output
        formatted_df = df[[
            'LastName', 'FirstName', 'Name', 'Title'
        ]].copy()

        # Sort by LastName then FirstName
        formatted_df = formatted_df.sort_values(['LastName', 'FirstName'])

        # Reset index
        formatted_df = formatted_df.reset_index(drop=True)

        return formatted_df

    def analyze_customer_behavior(self, df):
        """
        Additional Python analysis on the data
        """
        print("\n" + "=" * 50)
        print("PYTHON DATA ANALYSIS RESULTS")
        print("=" * 50)

        # Customer purchase statistics
        customer_stats = df.groupby(['LastName', 'FirstName']).agg({
            'Name': 'count',  # Number of tracks purchased
            'Title': 'nunique'  # Number of unique albums
        }).rename(columns={'Name': 'TotalTracks', 'Title': 'UniqueAlbums'})

        print("\nTop 10 customers by tracks purchased:")
        print(customer_stats.nlargest(10, 'TotalTracks'))

        # Most popular tracks
        popular_tracks = df['Name'].value_counts().head(10)
        print("\nTop 10 most purchased tracks:")
        print(popular_tracks)

        # Most popular albums
        popular_albums = df['Title'].value_counts().head(10)
        print("\nTop 10 most purchased albums:")
        print(popular_albums)

        return customer_stats, popular_tracks, popular_albums

    def create_visualizations(self, df):
        """
        Create visualizations using Python with robust style handling
        """
        try:
            import matplotlib.pyplot as plt

            print("\nCreating visualizations...")

            # Get available styles and use a safe option
            available_styles = plt.style.available
            if 'seaborn' in available_styles:
                plt.style.use('seaborn')
            elif 'seaborn-v0_8' in available_styles:
                plt.style.use('seaborn-v0_8')
            else:
                plt.style.use('default')  # Fallback to default style

            fig, axes = plt.subplots(2, 2, figsize=(15, 12))

            # Plot 1: Top 10 customers by tracks purchased
            top_customers = df.groupby(['LastName', 'FirstName']).size().nlargest(10)
            top_customers.plot(kind='bar', ax=axes[0, 0], title='Top 10 Customers by Tracks Purchased',
                               color='skyblue', edgecolor='black')
            axes[0, 0].tick_params(axis='x', rotation=45)
            axes[0, 0].set_ylabel('Number of Tracks')

            # Plot 2: Top 10 most popular tracks
            popular_tracks = df['Name'].value_counts().head(10)
            axes[0, 1].bar(range(len(popular_tracks)), popular_tracks.values, color='lightgreen', edgecolor='black')
            axes[0, 1].set_title('Top 10 Most Popular Tracks')
            axes[0, 1].set_xticks(range(len(popular_tracks)))
            axes[0, 1].set_xticklabels(popular_tracks.index, rotation=45, ha='right')
            axes[0, 1].set_ylabel('Purchase Count')

            # Plot 3: Top 10 most popular albums
            popular_albums = df['Title'].value_counts().head(10)
            axes[1, 0].bar(range(len(popular_albums)), popular_albums.values, color='lightcoral', edgecolor='black')
            axes[1, 0].set_title('Top 10 Most Popular Albums')
            axes[1, 0].set_xticks(range(len(popular_albums)))
            axes[1, 0].set_xticklabels(popular_albums.index, rotation=45, ha='right')
            axes[1, 0].set_ylabel('Purchase Count')

            # Plot 4: Distribution of tracks per customer
            tracks_per_customer = df.groupby(['LastName', 'FirstName']).size()
            axes[1, 1].hist(tracks_per_customer, bins=20, alpha=0.7, color='purple', edgecolor='black')
            axes[1, 1].set_title('Distribution of Tracks per Customer')
            axes[1, 1].set_xlabel('Number of Tracks')
            axes[1, 1].set_ylabel('Number of Customers')

            plt.tight_layout()
            plt.savefig('customer_tracks_analysis.png', dpi=300, bbox_inches='tight')
            plt.show()
            print("Visualizations saved as 'customer_tracks_analysis.png'")

        except ImportError:
            print("Matplotlib not available for visualizations")
        except Exception as e:
            print(f"Error creating visualizations: {e}")
            print("Continuing without visualizations...")

    def run_complete_analysis(self):
        """
        Run the complete analysis pipeline
        """
        print("Starting Chinook Database Analysis...")

        # Step 0: Connect to database
        if not self.connect_to_database():
            return None

        # Step 1: Load all tables
        if not self.load_all_tables():
            self.close_connection()
            return None

        # Step 2: Merge data using Python
        merged_data = self.merge_data_python()

        # Step 3: Clean and format data
        final_df = self.clean_and_format_data(merged_data)

        # Step 4: Display required output
        print("\n" + "=" * 50)
        print("REQUIRED OUTPUT - First 5 Rows")
        print("=" * 50)
        print(final_df.head())

        # Step 5: Additional Python analysis
        self.analyze_customer_behavior(final_df)

        # Step 6: Create visualizations
        self.create_visualizations(final_df)

        # Step 7: Close connection
        self.close_connection()

        return final_df

    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
        if self.engine:
            self.engine.dispose()


def main():
    """Main execution function"""
    analyzer = None
    try:
        # Initialize analyzer
        analyzer = ChinookAnalyzer('Chinook_Sqlite.sqlite')

        # Run complete analysis
        result_df = analyzer.run_complete_analysis()

        # Save results
        if result_df is not None:
            result_df.to_csv('customer_tracks_python.csv', index=False)
            print(f"\nResults saved to 'customer_tracks_python.csv'")
            print(f"Final DataFrame shape: {result_df.shape}")

    except Exception as e:
        print(f"Error during analysis: {e}")

    finally:
        # Ensure connection is closed
        if analyzer:
            analyzer.close_connection()


if __name__ == "__main__":
    main()