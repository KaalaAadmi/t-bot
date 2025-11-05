import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

class DBConnector:
    """
    A class to manage the PostgreSQL connection and database operations.
    Handles table creation and data insertion for candles and ORB ranges.
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
            # Use autocommit for simple DDL and non-transactional inserts
            self.conn.autocommit = True 
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
            logger.info("Database connection closed.")

    def initialize_tables(self):
        """Initializes all necessary tables, including creating indexes and hypertables."""
        if not self.conn:
            logger.error("Cannot initialize tables: Database connection is not established.")
            return

        try:
            # 1. Check/Create the base tables
            self._create_one_min_candles_table()
            self._create_orb_ranges_table()
            
            # 2. Convert to TimeScaleDB Hypertable
            self._convert_to_hypertable('one_min_candles', 'timestamp')
            
            # Since ORB ranges also contain a timestamp, they should probably be a hypertable too
            # self._convert_to_hypertable('orb_ranges', 'timestamp') 

        except Exception as e:
            # This catch block will now likely be hit if a table creation error occurs
            logger.error(f"Error during database initialization: {e}")

    def _execute_query(self, query: sql.SQL, params: tuple = None, log_success: str = None):
        """Helper to execute a query and handle common exceptions."""
        if not self.conn:
            logger.error("Query failed: Database connection is not established.")
            return
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, params)
            if log_success:
                logger.info(log_success)
        except Exception as e:
            # Log the full query causing the issue for better debugging
            logger.error(f"Error executing SQL query: {e}\nQuery: {query.as_string(self.conn)}")
            raise # Re-raise to be caught by initialize_tables

    def _check_and_enable_timescale_extension(self):
        """Ensures the TimeScaleDB extension is enabled."""
        try:
            self._execute_query(
                sql.SQL("CREATE EXTENSION IF NOT EXISTS timescaledb;"),
                log_success="TimeScaleDB extension checked/enabled."
            )
        except Exception as e:
            logger.error(f"Failed to enable TimeScaleDB extension: {e}")
            
    def _create_one_min_candles_table(self):
        """Creates the one_min_candles table and the necessary functional unique index."""
        # This table stores 1-minute OHLCV data.
        # --- FIX APPLIED HERE: Removed invalid UNIQUE constraint from inline table definition ---
        one_min_candles_table_creation_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS one_min_candles (
                timestamp TIMESTAMPTZ NOT NULL,
                ticker VARCHAR(10) NOT NULL,
                token INTEGER NOT NULL,
                open NUMERIC(10, 2) NOT NULL,
                high NUMERIC(10, 2) NOT NULL,
                low NUMERIC(10, 2) NOT NULL,
                close NUMERIC(10, 2) NOT NULL,
                volume INTEGER NOT NULL,
                interval VARCHAR(5) NOT NULL,
                PRIMARY KEY (timestamp, ticker)
                -- REMOVED: UNIQUE (DATE(timestamp), ticker) <-- This was the error
            );
        """)

        # --- FIX: Create the functional unique index as a separate statement ---
        daily_unique_index_query = sql.SQL("""
            -- This index enforces that we only have one daily record per ticker. 
            -- Note: We use IF NOT EXISTS to prevent errors if it already exists.
            CREATE UNIQUE INDEX IF NOT EXISTS one_min_candles_date_ticker_idx
            ON one_min_candles (DATE(timestamp), ticker);
        """)

        self._execute_query(
            one_min_candles_table_creation_query, 
            log_success="Table 'one_min_candles' checked/created successfully."
        )
        # Execute the separate, corrected index creation query
        self._execute_query(
            daily_unique_index_query, 
            log_success="Functional unique index created/verified on 'one_min_candles'."
        )

    def _create_orb_ranges_table(self):
        """Creates the orb_ranges table."""
        # This table stores daily Open Range Breakout (ORB) data.
        # The unique constraint is enforced on the primary key (date-ticker combination).
        orb_table_creation_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS orb_ranges (
                timestamp TIMESTAMPTZ NOT NULL,
                ticker VARCHAR(10) NOT NULL,
                token INTEGER NOT NULL,
                orb_high NUMERIC(10, 2) NOT NULL,
                orb_low NUMERIC(10, 2) NOT NULL,
                orb_range_size NUMERIC(10, 2) NOT NULL,
                orb_volume INTEGER NOT NULL,
                -- Primary key is a composite key on the date part of timestamp and ticker
                PRIMARY KEY (DATE(timestamp), ticker)
            );
        """)
        self._execute_query(
            orb_table_creation_query, 
            log_success="Table 'orb_ranges' checked/created successfully."
        )
            
    def _convert_to_hypertable(self, table_name: str, time_column: str):
        """Converts a standard PostgreSQL table into a TimeScaleDB hypertable."""
        # Ensure the extension is enabled first
        self._check_and_enable_timescale_extension()
        
        hypertable_conversion_query = sql.SQL(
            "SELECT create_hypertable({table_name}, {time_column}, if_not_exists => TRUE);"
        ).format(
            table_name=sql.Literal(table_name), 
            time_column=sql.Literal(time_column)
        )
        
        self._execute_query(
            hypertable_conversion_query, 
            log_success=f"Table '{table_name}' converted to TimeScaleDB Hypertable."
        )

    def insert_one_min_candle(self, candle_data: Dict[str, Any]):
        """Inserts a single 1-minute candle record into the database."""
        if not self.conn:
            logger.error("Cannot insert candle: Database connection is not established.")
            return

        # Use ON CONFLICT on the PRIMARY KEY to handle updates or re-sends
        query = sql.SQL("""
            INSERT INTO one_min_candles (
                timestamp, ticker, token, open, high, low, close, volume, interval
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (timestamp, ticker) DO UPDATE SET
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """)

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    candle_data['timestamp'], 
                    candle_data['ticker'],
                    candle_data['token'],
                    candle_data['open'], 
                    candle_data['high'], 
                    candle_data['low'], 
                    candle_data['close'], 
                    candle_data['volume'],
                    candle_data['interval']
                ))
        except Exception as e:
            # Log the error without raising to prevent a single bad candle from crashing the stream
            logger.error(f"Error inserting 1-min candle for {candle_data.get('ticker', 'N/A')}: {e}")

    def insert_orb_range(self, orb_data: Dict[str, Any]):
        """Inserts a single ORB range record into the database."""
        if not self.conn:
            logger.error("Cannot insert ORB range: Database connection is not established.")
            return

        # Uses ON CONFLICT on the functional index (DATE(timestamp), ticker)
        query = sql.SQL("""
            INSERT INTO orb_ranges (
                timestamp, ticker, token, orb_high, orb_low, orb_range_size, orb_volume
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (DATE(timestamp), ticker) DO NOTHING;
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
        except Exception as e:
            logger.error(f"Error inserting ORB range for {orb_data.get('ticker', 'N/A')} on {orb_data.get('timestamp', datetime.now()).date()}: {e}")

    def get_orb_range(self, ticker: str, date: datetime.date) -> Dict[str, Any] or None:
        """Retrieves the ORB range for a specific ticker and date."""
        if not self.conn:
            logger.error("Cannot retrieve ORB range: Database connection is not established.")
            return None

        query = sql.SQL("""
            SELECT 
                orb_high, orb_low, orb_range_size, orb_volume 
            FROM 
                orb_ranges 
            WHERE 
                ticker = %s AND DATE(timestamp) = %s;
        """)

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query, (ticker, date))
                result = cur.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error retrieving ORB range for {ticker} on {date}: {e}")
            return None
