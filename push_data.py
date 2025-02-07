import pypyodbc
import pandas as pd
import os
import sys

from dotenv import load_dotenv
load_dotenv(override=True)

from src.exception.exception import Exceptionhandle
from src.logging.logger import logging

class DatabaseManager:
    """Handles all interactions with the MS SQL Server database."""

    def __init__(self, server, database):
        """Initialize connection to the SQL Server."""
        self.connection_string = f"""DRIVER={{SQL SERVER}};
                                    SERVER={server};
                                    DATABASE={database};
                                    Trust_Connection=yes;
                                """
        self.conn = None
        self.cursor = None

    def connect(self):
        """Establish connection to SQL Server."""
        try:
            self.conn = pypyodbc.connect(self.connection_string)
            self.cursor = self.conn.cursor()
            logging.info("Database connection established successfully!")
            return self.conn
        except Exception as e:
            raise Exceptionhandle(e,sys)

    def close_connection(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.info("Database connection closed.")

    def create_table(self):
        """Create the StudentPlacement table if it does not exist."""
        create_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'StudentPlacement')
        CREATE TABLE StudentPlacement (
            StudentID INT PRIMARY KEY,
            CGPA FLOAT,
            Internships INT,
            Projects INT,
            Certifications INT,
            AptitudeTestScore FLOAT,
            SoftSkillsRating FLOAT,
            ExtracurricularActivities VARCHAR(10),
            PlacementTraining VARCHAR(10),
            SSC_Marks FLOAT,
            HSC_Marks FLOAT,
            PlacementStatus VARCHAR(20)
        );
        """
        try:
            self.cursor.execute(create_table_query)
            self.conn.commit()
            logging.info("✅ Table 'StudentPlacement' created successfully (if not already exists).")
        except Exception as e:
            raise Exceptionhandle(e,sys)

    def insert_data_from_csv(self, csv_file):
        """Insert data from a CSV file into the StudentPlacement table."""
        try:
            df = pd.read_csv(csv_file)
            insert_query = """
            INSERT INTO StudentPlacement (
                StudentID, CGPA, Internships, Projects, Certifications, 
                AptitudeTestScore, SoftSkillsRating, ExtracurricularActivities, 
                PlacementTraining, SSC_Marks, HSC_Marks, PlacementStatus
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            for _, row in df.iterrows():
                self.cursor.execute(insert_query, tuple(row))
            self.conn.commit()
            logging.info(f"✅ {len(df)} records inserted successfully.")
        except Exception as e:
            raise Exceptionhandle(e,sys)



# Main execution
if __name__ == "__main__":
    # Database details
    server=os.getenv("server")
    database=os.getenv("database")
    print(server)
    print(database)
    csv_file = "Data/placementdata.csv"  # Update with your actual file path

    # Initialize database manager and connect
    db_manager = DatabaseManager(server, database)
    db_manager.connect()

    # Create table if not exists
    db_manager.create_table()

    db_manager.insert_data_from_csv(csv_file)

    # Close connection
    db_manager.close_connection()
