import logging
import asyncio
import copy
from datetime import datetime, timedelta
import pytz
from typing import Dict, Any, List, Optional

# External dependencies
from kiteconnect import KiteTicker
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Initialize logger (using 'root' for consistency with main.py)
logger = logging.getLogger(__name__)


class DataCollectorAgent:
    """
    Manages real-time data flow by:
    1. Scheduling ORB calculation at 9:20 AM IST.
    2. Converting live ticks into 1-minute OHLCV candles.
    3. Publishing all prepared data to Redis Streams.
    4. Persisting ORB ranges to the PostgreSQL database.
    """
    def __init__(self, kc_client, redis_client, db_connector, config):
        self.kc = kc_client
        self.r = redis_client
        self.db = db_connector
        self.config = config

        # Timezone settings (critical for scheduled jobs and candle generation)
        self.tz = pytz.timezone(config.get('trading', {}).get('time_zone', 'Asia/Kolkata'))
        
        # Redis Stream names
        self.ORB_STREAM = config['streams'].get('orb_range_data', 'orb_range_data_stream') 
        self.CANDLE_STREAM = config['streams']['one_min_candle_data']

        # KITE Ticker setup
        self.ticker: Optional[KiteTicker] = None
        self.subscribed_tokens: Dict[str, int] = {}  # {ticker: token}
        self.active_tokens: List[int] = []     # List of tokens (int) to subscribe

        # In-memory dictionary to aggregate Ticks -> 1-Minute Candles
        # {token: {timestamp: {open, high, low, close, volume, start_volume}}}
        self.candles_1min: Dict[int, Dict[datetime, Dict[str, Any]]] = {}

        # New dictionary to store 1-minute candles specifically for ORB calculation
        # {token: {timestamp: {candle_data}}}
        self.orb_candles: Dict[int, Dict[datetime, Dict[str, Any]]] = {}
        
        # Scheduler for market-specific jobs
        self.scheduler = AsyncIOScheduler(timezone=self.tz)

    async def run(self, ticker_token_map: Dict[str, int]):
        """Starts the data collection loop and sets up scheduled jobs."""
        
        # Invert the map for easier lookup: {token: ticker}
        self.subscribed_tokens = {v: k for k, v in ticker_token_map.items()}
        self.active_tokens = list(self.subscribed_tokens.keys())
        
        # 1. Start the Ticker Connection
        await self._start_ticker_connection()

        # 2. Setup ORB Scheduled Job
        self._setup_orb_scheduler()

        # 3. Keep the agent alive
        while True:
            await asyncio.sleep(60)
            
    # -----------------------------------------------
    # --- KITE TICKER HANDLERS AND CONNECTION ---
    # -----------------------------------------------

    def _on_connect(self, ws, response):
        """Callback when the ticker connects successfully."""
        logger.info("Kite Ticker connected. Subscribing to instruments...")
        
        # Subscribe to all active tokens (mode 1: full quotes including volume and depth)
        ws.subscribe(self.active_tokens)
        ws.set_mode(ws.MODE_FULL, self.active_tokens)
        logger.info(f"Subscribed to {len(self.active_tokens)} instruments in FULL mode.")
        
    def _on_close(self, ws, code, reason):
        """Callback when the ticker connection closes."""
        logger.warning(f"Kite Ticker connection closed. Code: {code}, Reason: {reason}")

    def _on_error(self, ws, code, reason):
        """Callback on connection error."""
        logger.error(f"Kite Ticker error. Code: {code}, Reason: {reason}")

    def _on_message(self, ws, data):
        """Callback on receiving a message (rarely used for tick data)."""
        logger.debug(f"Received message: {data}")

    def _on_tick(self, ws, ticks):
        """Processes incoming ticks to build 1-minute candles."""
        for tick in ticks:
            instrument_token = tick['instrument_token']
            last_price = tick['last_price']
            tick_time_ist = tick['exchange_timestamp'].astimezone(self.tz)
            volume_traded = tick.get('volume_traded', 0)
            
            # Candles are indexed by their *start* time (e.g., 9:15:00 for the candle closing at 9:16:00)
            current_minute_start = tick_time_ist.replace(second=0, microsecond=0)
            
            if instrument_token not in self.candles_1min:
                self.candles_1min[instrument_token] = {}

            if current_minute_start not in self.candles_1min[instrument_token]:
                # A new minute has started: the previous minute's candle is COMPLETE.
                
                # Check for the previous minute's candle (the one that just closed)
                prev_minute_start = current_minute_start - timedelta(minutes=1)
                
                if prev_minute_start in self.candles_1min[instrument_token]:
                    # Publish the fully closed candle before creating the new one
                    self._publish_closed_candle(instrument_token, prev_minute_start)
                    # Clean up the closed candle to save memory
                    del self.candles_1min[instrument_token][prev_minute_start]
                    self.db.insert_data('minute_candles', [self.candles_1min[instrument_token][prev_minute_start]])


                # Initialize the new 1-minute candle
                self.candles_1min[instrument_token][current_minute_start] = {
                    'open': last_price,
                    'high': last_price,
                    'low': last_price,
                    'close': last_price, # Starts as the last price
                    'start_volume': volume_traded, # Total traded volume when the candle started
                    'volume': 0 # Incremental volume for this candle
                }
            
            # Update the current candle with the latest tick data
            candle = self.candles_1min[instrument_token][current_minute_start]
            
            candle['high'] = max(candle['high'], last_price)
            candle['low'] = min(candle['low'], last_price)
            candle['close'] = last_price # Always updated with the last tick price
            
            # Calculate volume traded in this minute
            candle['volume'] = volume_traded - candle['start_volume']
            
            # Special handling for the very last minute of trading (9:15 to 3:30 PM is 375 minutes)
            # The candle starting at 15:29:00 will close at 15:30:00.
            if current_minute_start.hour == 15 and current_minute_start.minute >= 29:
                # Force close the last candle at 15:30:00 (which is tick time 15:29:59.xxx)
                if current_minute_start.minute == 29 and tick_time_ist.second >= 58:
                    self._publish_closed_candle(instrument_token, current_minute_start)
                    del self.candles_1min[instrument_token][current_minute_start]


    def _publish_closed_candle(self, token: int, timestamp: datetime):
        """
        Helper to format and publish a closed candle.
        This is where we now also capture candles for the ORB calculation.
        """
        # Get the ticker name from the inverted subscribed_tokens map
        ticker_name = self.subscribed_tokens.get(token, str(token))

        # We must use a copy here as the candle might be deleted right after
        closed_candle_data = copy.deepcopy(self.candles_1min[token][timestamp])

        candle_data = {
            'timestamp': timestamp.isoformat(), # Use isoformat for JSON/Redis
            'ticker': ticker_name,
            'token': token,
            'open': closed_candle_data['open'],
            'high': closed_candle_data['high'],
            'low': closed_candle_data['low'],
            'close': closed_candle_data['close'],
            'volume': closed_candle_data['volume'],
            'interval': '1min'
        }
        
        # --- ORB Capture Logic ---
        # Capture candles that *start* between 9:15:00 and 9:19:00 IST
        # These are the 5 candles that close at 9:16, 9:17, 9:18, 9:19, and 9:20.
        if timestamp.hour == 9 and 15 <= timestamp.minute <= 19:
            if token not in self.orb_candles:
                self.orb_candles[token] = {}
            
            # Store a copy of the final, closed candle OHLC data
            # The key is the start time of the candle (9:15, 9:16, etc.)
            self.orb_candles[token][timestamp] = {
                'open': closed_candle_data['open'],
                'high': closed_candle_data['high'],
                'low': closed_candle_data['low'],
                'close': closed_candle_data['close'],
            }
            logger.debug(f"ORB Capture: Stored {ticker_name} 1-min candle starting at {timestamp.strftime('%H:%M')}")
        # --- END ORB Capture Logic ---

        # Publish the 1-minute candle to the stream
        self.r.publish_to_stream(self.CANDLE_STREAM, f"candle_{ticker_name}", candle_data)
        
        # Log the published candle (optional, for debugging)
        logger.debug(f"Published 1min candle for {ticker_name} @ {timestamp.strftime('%H:%M')}: O={candle_data['open']}, C={candle_data['close']}")


    async def _start_ticker_connection(self):
        """Initializes and connects the KiteTicker."""
        try:
            # Note: KiteTicker takes the API Key and Access Token directly
            self.ticker = KiteTicker(self.config['zerodha']['api_key'], self.kc.access_token)
            
            # Setup all necessary callbacks
            self.ticker.on_connect = self._on_connect
            self.ticker.on_close = self._on_close
            self.ticker.on_error = self._on_error
            self.ticker.on_message = self._on_message
            self.ticker.on_ticks = self._on_tick
            
            logger.info("Starting Kite Ticker connection in a separate thread...")
            # Blocking call, runs in a dedicated thread
            self.ticker.connect(daemon=True)
            
        except Exception as e:
            logger.critical(f"Failed to initialize or connect Kite Ticker: {e}")
            
    # -----------------------------------------------
    # --- ORB SCHEDULING AND CALCULATION LOGIC ---
    # -----------------------------------------------

    def _setup_orb_scheduler(self):
        """Schedules the ORB calculation function to run at 9:20 AM IST."""
        if self.scheduler.running:
            return

        # Schedule the ORB calculation job for every weekday at 9:20:00 IST.
        # This runs AFTER the candle starting at 9:19:00 closes (which is 9:20:00)
        self.scheduler.add_job(
            self._calculate_orb_range,
            'cron',
            day_of_week='mon-fri',
            hour=9,
            minute=20,
            second=5, # Adding a 5 second buffer just in case
            name='Open_Range_Breakout_Calculator'
        )
        
        self.scheduler.start()
        logger.info("ORB Range Calculator scheduled to run weekdays at 09:20:05 IST.")
        
    def _calculate_orb_range(self):
        """
        Aggregates the 5 collected 1-minute candles (9:15 to 9:20) to find 
        the ORB high/low and publishes the result.
        """
        logger.info("Starting ORB (9:15-9:20) calculation for all active instruments.")
        
        orb_results = []
        
        # Iterate over tokens that have collected ORB data
        for token, token_orb_candles in self.orb_candles.items():
            
            ticker_name = self.subscribed_tokens.get(token, str(token))
            
            # We expect exactly 5 candles (9:15, 9:16, 9:17, 9:18, 9:19 start times)
            if len(token_orb_candles) < 5:
                logger.warning(f"Skipping ORB for {ticker_name} ({token}): Found only {len(token_orb_candles)} candles (expected 5).")
                continue
                
            # Aggregate the 5 candles to find the extremes
            all_highs = [c['high'] for c in token_orb_candles.values()]
            all_lows = [c['low'] for c in token_orb_candles.values()]
            
            orb_high = max(all_highs)
            orb_low = min(all_lows)
            
            # The official timestamp of the ORB range is 9:20:00 IST
            timestamp = datetime.now(self.tz).replace(hour=9, minute=20, second=0, microsecond=0)
            
            orb_data = {
                'timestamp': timestamp.isoformat(),
                'ticker': ticker_name,
                'token': token,
                'high': orb_high,
                'low': orb_low,
                'range': orb_high - orb_low,
                'interval': '5min_ORB'
            }
            
            orb_results.append(orb_data)
            
            # 1. Publish to Redis Stream for downstream agents
            self.r.publish_to_stream(self.ORB_STREAM, f"orb_{ticker_name}", orb_data)
            logger.debug(f"Published ORB for {ticker_name}: High={orb_high}, Low={orb_low}")
            
            # 2. Persist to Database (Assuming the 'orb_ranges' table exists)
            try:
                # We need to ensure the timestamp is a datetime object for the DB insertion helper
                db_record = {**orb_data, 'timestamp': timestamp} 
                self.db.insert_data('orb_ranges', [db_record])
            except Exception as e:
                logger.error(f"Failed to insert ORB data for {ticker_name} into DB: {e}")
            logger.info(f"ORB for {ticker_name}: High={orb_high}, Low={orb_low}, Range={orb_high - orb_low}")
        # Clear the ORB cache after calculation is complete for the day
        self.orb_candles.clear()
        logger.info(f"Completed ORB calculation for {len(orb_results)} instruments. ORB cache cleared.")
