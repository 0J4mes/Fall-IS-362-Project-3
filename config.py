# config.py
import os
from sqlalchemy import create_engine


class DatabaseConfig:
    """Database configuration and connection management"""

    def __init__(self, database_path='Chinook_Sqlite.sqlite'):
        self.database_path = database_path
        self.engine = None
        self.connection = None

    def get_connection(self):
        """Get database connection"""
        if not self.connection:
            self.engine = create_engine(f'sqlite:///{self.database_path}')
            self.connection = self.engine.connect()
        return self.connection

    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()
        self.connection = None
        self.engine = None


# Project settings
OUTPUT_DIR = 'output'
CSV_FILENAME = 'customer_tracks_analysis.csv'
IMAGE_FILENAME = 'customer_analysis_plots.png'


def ensure_output_dir():
    """Create output directory if it doesn't exist"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)


def get_database_path():
    """Get the path to the database file"""
    possible_paths = [
        'Chinook_Sqlite.sqlite',
        'ChinookDatabase/Chinook_Sqlite.sqlite',
        'chinook.db',
        '../Chinook_Sqlite.sqlite'
    ]

    for path in possible_paths:
        if os.path.exists(path):
            return path

    return None