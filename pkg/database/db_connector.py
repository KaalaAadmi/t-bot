import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import logging
from typing import Dict, Any, List

logger = logging.getLogger('root')

class DBConnector:
    """
    A class to manage the PostgreSQL connection and database operations.
    Handles table creation and data insertion for ORB ranges.
    """
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None
        logger.info("DBConnector initialized with configuration.")

    def connect(self):
        """Establishes the connection to the PostgreSQL database."""
        if self.conn and not self.conn.closed:
            return self.conn
            
        try:
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                database=self.db_config['db'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                # Set client encoding and other standard parameters
                options="-c search_path=public"
            )
            self.conn.autocommit = True # Useful for simple insert/create operations
            logger.info("Successfully connected to PostgreSQL database.")
            return self.conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.conn = None
            return None

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")

    def initialize_tables(self):
        """Creates all necessary tables if they do not already exist."""
        if not self.conn:
            logger.error("Cannot initialize tables: Database connection is not established.")
            return

        # 1. ORB Ranges Table Definition (The table you requested)
        orb_table_creation_query = """
        CREATE TABLE IF NOT EXISTS orb_ranges (
            timestamp TIMESTAMPTZ NOT NULL,
            ticker VARCHAR(20) NOT NULL,
            token INTEGER NOT NULL,
            orb_high NUMERIC(10, 2) NOT NULL,
            orb_low NUMERIC(10, 2) NOT NULL,
            orb_range_size NUMERIC(10, 2) NOT NULL,
            orb_volume BIGINT NOT NULL,
            
            -- Primary key ensures we don't insert duplicate ORBs for the same ticker on the same day
            PRIMARY KEY (timestamp::date, ticker)
        );
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(orb_table_creation_query)
                logger.info("Table 'orb_ranges' checked/created successfully.")
        except Exception as e:
            logger.error(f"Error creating 'orb_ranges' table: {e}")
            
    def insert_orb_range(self, orb_data: Dict[str, Any]):
        """Inserts a single ORB range record into the database."""
        if not self.conn:
            logger.error("Cannot insert ORB range: Database connection is not established.")
            return

        query = sql.SQL("""
            INSERT INTO orb_ranges (
                timestamp, ticker, token, orb_high, orb_low, orb_range_size, orb_volume
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (timestamp::date, ticker) DO NOTHING;
        """)

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    orb_data['timestamp'], 
                    orb_data['ticker'],
                    orb_data['token'],
                    orb_data['orb_high'], 
                    orb_data['orb_low'], 
                    orb_data['orb_range_size'], 
                    orb_data['orb_volume']
                ))
            # Log at debug level to avoid spamming for every insert
            # logger.debug(f"Inserted ORB for {orb_data['ticker']} on {orb_data['timestamp'].date()}")
        except Exception as e:
            logger.error(f"Error inserting ORB range for {orb_data['ticker']}: {e}")
