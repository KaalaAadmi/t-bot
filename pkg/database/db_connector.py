import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
import pytz

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
        print("Establishing new database connection...")
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
            # --- FIX: Move TimeScaleDB Extension check here (ensures it's enabled early) ---
            self._check_and_enable_timescale_extension()
            
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
        """Creates the one_min_candles table."""
        one_min_candles_table_creation_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS one_min_candles (
                timestamp TIMESTAMPTZ NOT NULL,
                ticker TEXT NOT NULL,  -- FIX 4: Changed VARCHAR(10) to TEXT
                token INTEGER NOT NULL,
                open NUMERIC(10, 2) NOT NULL,
                high NUMERIC(10, 2) NOT NULL,
                low NUMERIC(10, 2) NOT NULL,
                close NUMERIC(10, 2) NOT NULL,
                volume INTEGER NOT NULL,
                interval TEXT NOT NULL, -- FIX 4: Changed VARCHAR(5) to TEXT
                PRIMARY KEY (timestamp, ticker)
            );
        """)

        # --- FIX: Create the functional unique index as a separate statement ---
        # daily_unique_index_query = sql.SQL("""
        #     CREATE UNIQUE INDEX IF NOT EXISTS one_min_candles_date_ticker_idx
        #     ON one_min_candles (date_trunc('day', timestamp AT TIME ZONE 'UTC'), ticker);
        # """)
        try:
            self._execute_query(
                one_min_candles_table_creation_query, 
                log_success="Table 'one_min_candles' checked/created successfully."
            )
            logger.info("1-Minute candles table created successfully.")
            # Execute the separate, corrected index creation query
            # self._execute_query(
            #     daily_unique_index_query, 
            #     log_success="Functional unique index created/verified on 'one_min_candles'."
            # )
            # logger.info("Functional unique index created successfully.")
        except Exception as e:
            logger.info(f"Error creating functional unique index on 'one_min_candles': {e}")

    def _create_orb_ranges_table(self):
        """Creates the orb_ranges table."""
        orb_table_creation_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS orb_ranges (
                timestamp TIMESTAMPTZ NOT NULL,
                ticker TEXT NOT NULL,  -- FIX 4: Changed VARCHAR(10) to TEXT
                token INTEGER NOT NULL,
                orb_high NUMERIC(10, 2) NOT NULL,
                orb_low NUMERIC(10, 2) NOT NULL,
                orb_range_size NUMERIC(10, 2) NOT NULL,
                orb_volume INTEGER NOT NULL,
                PRIMARY KEY (timestamp, ticker)
            );
        """)

        # FIX 3b: Create the unique functional index separately for daily uniqueness
        daily_unique_index_query = sql.SQL("""
            CREATE UNIQUE INDEX IF NOT EXISTS orb_ranges_date_ticker_idx
            ON orb_ranges (DATE(timestamp AT TIME ZONE 'UTC'), ticker);
        """)
        
        try:
            self._execute_query(
                orb_table_creation_query, 
                log_success="Table 'orb_ranges' checked/created successfully."
            )
            logger.info("ORB ranges table created successfully.")
            # Execute the separate functional unique index creation query
            self._execute_query(
                daily_unique_index_query, 
                log_success="Functional unique index created/verified on 'orb_ranges'."
            )
            logger.info("Functional unique index created successfully.")
        except Exception as e:
            logger.info(f"Error creating 'orb_ranges' table or index: {e}")

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
        # ... (omitted connection check)

        # FIX 3c: Change ON CONFLICT to use the new simple primary key
        # We rely on the unique functional index to prevent duplicate dates.
        query = sql.SQL("""
            INSERT INTO orb_ranges (
                timestamp, ticker, token, orb_high, orb_low, orb_range_size, orb_volume
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (timestamp, ticker) DO NOTHING;
        """)

        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    orb_data['timestamp'], 
                    orb_data['ticker'],
                    orb_data['token'],
                    orb_data['high'], 
                    orb_data['low'], 
                    orb_data['range'], 
                    orb_data['volume']
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

    def _insert_dummy_data(self):
        """Generates and inserts sample data."""
        logger.info("Generating and inserting dummy data...")
        
        # Define a timezone
        tz = pytz.timezone('Asia/Kolkata')
        
        # --- 1-Minute Candles Dummy Data ---
        candles = []
        ticker_tokens = {'TCS': 115393, 'INFY': 4963}
        # Anchor the calculation to a specific date for consistency. 
        # We'll use today's date at 9:15 AM IST and then subtract 1 day.
        now_in_tz = datetime.now(tz).date() # Get today's date in IST timezone
        # Combine the fixed time (9:15) with the calculated date
        start_date_ist = tz.localize(datetime(now_in_tz.year, now_in_tz.month, now_in_tz.day, 9, 15, 0, 0)) - timedelta(days=1)
        start_time = start_date_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        
        for i in range(5): # Generate 5 minutes of data
            difference = timedelta(minutes=i)
            timestamp = start_time + difference

            # Simple linear data for TCS and INFY
            for ticker, token in ticker_tokens.items():
                base_price = 3000 if ticker == 'TCS' else 1400
                close_change = 1.0 * i
                volume = 1000 + (i * 100)
                
                candle_record = {
                    'timestamp': timestamp,
                    'ticker': ticker,
                    'token': token,
                    'open': base_price + i,
                    'high': base_price + i + 1,
                    'low': base_price + i - 0.5,
                    'close': base_price + i + close_change,
                    'volume': volume,
                    'interval': '1min'
                }
                candles.append(candle_record)

        # Insert the generated candle data
        for candle in candles:
            # *** MODIFICATION FOR one_min_candles ***
            # The live agent calls insert_data with the record wrapped in a list: [record]
            self.insert_data('one_min_candles', [candle]) 
            logger.info(f"Inserted dummy candle for {candle['ticker']} at {candle['timestamp']}")
        logger.info(f"Successfully inserted {len(candles)} dummy 1-min candles.")

        # --- ORB Ranges Dummy Data ---
        
        # Use the start time of the ORB range (9:15 AM)
        orb_time = tz.localize(datetime.now().replace(hour=9, minute=20, second=0, microsecond=0) - timedelta(days=1))

        orb_data = [
            {
                'timestamp': orb_time, 
                'ticker': 'TCS', 
                'token': 115393, 
                'high': 3550.0, 
                'low': 3400.0, 
                'range': 150.0, 
                'volume': 30000
            },
            {
                'timestamp': orb_time, 
                'ticker': 'INFY', 
                'token': 4963, 
                'high': 1500.0, 
                'low': 1400.0, 
                'range': 100.0, 
                'volume': 20000
            },
        ]
        
        # Insert ORB ranges using the generic insert_data method
        for record in orb_data:
            # *** MODIFICATION FOR orb_ranges ***
            # The live agent calls insert_data with the record wrapped in a list: [record]
            self.insert_data('orb_ranges', [record])
            logger.debug(f"Inserted dummy ORB for {record['ticker']} at {record['timestamp']}")

        logger.info(f"Successfully inserted {len(orb_data)} dummy ORB ranges.")

    def _delete_dummy_data(self):
        """Deletes all dummy data inserted for testing."""
        logger.info("Deleting dummy data...")
        try:
            # Delete records specifically tagged as DUMMYTEST
            self._execute_query(sql.SQL("DELETE FROM one_min_candles WHERE ticker = 'DUMMYTEST';"), log_success="Deleted DUMMYTEST candles.")
            self._execute_query(sql.SQL("DELETE FROM orb_ranges WHERE ticker = 'DUMMYTEST';"), log_success="Deleted DUMMYTEST ORB ranges.")
            logger.info("Dummy data deletion complete.")
        except Exception as e:
            logger.error(f"Error deleting dummy data: {e}")
        
    def insert_data(self, table_name: str, data: Any):
        """
        Generic method to insert data. 
        
        *** MODIFIED LOGIC ***
        Handles the case where the input 'data' is a list of records 
        (matching the live agent's call) or a single record (Dict).
        """
        # 1. Determine the list of records to process
        # This handles the live agent passing [record] or a list of multiple records
        records_to_insert = data if isinstance(data, list) else [data]

        # 2. Iterate and insert each record individually
        for record in records_to_insert:
            print(record)
            if table_name == 'minute_candles' or table_name == 'one_min_candles':
                # Note: 'minute_candles' is the name used by the live agent in its call
                # 'one_min_candles' is the canonical table name here.
                self.insert_one_min_candle(record)
            elif table_name == 'orb_ranges':
                self.insert_orb_range(record)
            else:
                logger.warning(f"Unknown table name '{table_name}'. Data not inserted.")
