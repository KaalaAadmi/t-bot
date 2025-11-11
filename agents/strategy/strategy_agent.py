import logging
import asyncio
from datetime import datetime, time
import pytz
from typing import Dict, Any, Optional, List

# Application imports
from pkg.redis.redis_client import RedisClient 

logger = logging.getLogger(__name__)

# --- State Class ---
class TickerState:
    """Manages the trading state for a single instrument."""
    def __init__(self, token: int, ticker: str):
        self.token = token
        self.ticker = ticker
        self.orb_high: Optional[float] = None
        self.orb_low: Optional[float] = None
        # WAITING_FOR_ORB, WAITING_FOR_BREAKOUT, BREAKOUT_DETECTED, IN_TRADE, TRADE_CLOSED
        self.state: str = 'WAITING_FOR_ORB' 
        self.position: Optional[str] = None  # 'BUY', 'SELL', None
        # Stores last 2 candles for FVG check: [C_{-1}, C_B]. C_1 is the current processed candle.
        self.one_min_cache: List[Dict[str, Any]] = [] 
        self.breaker_candle: Optional[Dict[str, Any]] = None # The first valid breaker candle
        self.trade_params: Optional[Dict[str, Any]] = None # SL, TP, Entry etc.
        self.lock = asyncio.Lock() # For concurrent stream processing

    def __repr__(self):
        return (f"State(Ticker={self.ticker}, ORB={self.orb_low}-{self.orb_high}, "
                f"Status={self.state}, Pos={self.position})")

# --- Agent Class ---
class StrategyAgent:
    """
    Implements the ORB + FVG trading strategy by consuming real-time ORB and 1-minute candle data
    from Redis Streams and generating trade signals.
    """
    def __init__(self, redis_client: RedisClient, config: Dict):
        self.r = redis_client
        self.config = config
        self.tz = pytz.timezone(config.get('trading', {}).get('time_zone', 'Asia/Kolkata'))
        self.instruments_state: Dict[int, TickerState] = {} # Key: token, Value: TickerState

        # Redis Stream names from settings.yaml
        self.ORB_STREAM = config['streams'].get('orb_range_data', 'orb_range_data_stream')
        self.CANDLE_STREAM = config['streams']['one_min_candle_data']
        self.SIGNAL_STREAM = config['streams']['trade_signal']
        self.JOURNAL_STREAM = config['streams']['journaling']
        self.TRADE_CLOSED_STREAM = config['streams']['trade_closed']
        
        # Stream processing configuration
        self.CONSUMER_GROUP = 'strategy_agent_group'
        self.CONSUMER_NAME = 'strategy_instance_1'
        self.STREAM_POLL_INTERVAL = 1 # seconds

    async def run(self, ticker_token_map: Dict[str, int]):
        """Starts the main stream consumption loops."""
        logger.info("Starting Strategy Agent. Initializing instrument states.")
        
        # Initialize state objects for all instruments
        for ticker, token in ticker_token_map.items():
            self.instruments_state[token] = TickerState(token, ticker)
            
        # Initialize Redis streams for consumption (group creation)
        try:
            self.r.create_consumer_group(self.ORB_STREAM, self.CONSUMER_GROUP)
            self.r.create_consumer_group(self.CANDLE_STREAM, self.CONSUMER_GROUP)
            logger.info(f"Redis consumer group '{self.CONSUMER_GROUP}' initialized for ORB and CANDLE streams.")
        except Exception as e:
            logger.error(f"Failed to create Redis consumer group: {e}. Attempting to proceed without initial check.")
            
        # Start separate tasks for each stream consumer
        orb_task = asyncio.create_task(self._orb_consumer_loop())
        candle_task = asyncio.create_task(self._candle_consumer_loop())
        
        await asyncio.gather(orb_task, candle_task)

    # --- Redis Stream Consumers ---

    async def _orb_consumer_loop(self):
        """Continuously reads and processes ORB range data."""
        while True:
            try:
                # Note: This calls a synchronous method on the underlying Redis client (self.r.r.xreadgroup)
                messages = self._read_stream_messages(self.ORB_STREAM)
                for message in messages:
                    await self._process_orb_range(message)
            except Exception as e:
                logger.error(f"Error in ORB consumer loop: {e}")
            await asyncio.sleep(self.STREAM_POLL_INTERVAL)

    async def _candle_consumer_loop(self):
        """Continuously reads and processes 1-minute candle data."""
        while True:
            try:
                messages = self._read_stream_messages(self.CANDLE_STREAM)
                for message in messages:
                    await self._process_one_min_candle(message)
            except Exception as e:
                logger.error(f"Error in Candle consumer loop: {e}")
            await asyncio.sleep(self.STREAM_POLL_INTERVAL / 2.0)

    def _read_stream_messages(self, stream_name: str) -> List[Dict[str, str]]:
        """Reads messages from a Redis stream using XREADGROUP and returns a list of dictionaries."""
        messages = []
        try:
            results = self.r.r.xreadgroup(
                groupname=self.CONSUMER_GROUP,
                consumername=self.CONSUMER_NAME,
                streams={stream_name: '>'}, # Read new messages
                count=50,
                block=int(self.STREAM_POLL_INTERVAL * 500) # milliseconds block time
            )
            
            if results:
                # The structure is [[stream_name, [[id, {field: value}]]]]
                stream_result = results[0]
                for msg_id, fields in stream_result[1]:
                    processed_message = {'stream_id': msg_id}
                    for k, v in fields.items():
                        # Decode bytes to strings if necessary
                        processed_message[k.decode('utf-8') if isinstance(k, bytes) else k] = \
                        v.decode('utf-8') if isinstance(v, bytes) else v
                    messages.append(processed_message)
            
        except Exception as e:
            # Handle NOGROUP error if init failed to avoid crashing the loop
            if 'NOGROUP' not in str(e):
                 logger.error(f"Redis XREADGROUP failed for {stream_name}: {e}")
        return messages
            
    # --- Data Processors ---

    async def _process_orb_range(self, orb_message: Dict[str, Any]):
        """Processes a new ORB range message and updates the instrument state."""
        token = int(orb_message['token'])
        stream_id = orb_message['stream_id']
        state = self.instruments_state.get(token)

        try:
            if not state:
                logger.warning(f"Received ORB for unknown token: {token}. Skipping.")
                return
            
            async with state.lock:
                # Only set ORB once per day
                if state.orb_high is None or state.state == 'TRADE_CLOSED': 
                    state.orb_high = float(orb_message['high'])
                    state.orb_low = float(orb_message['low'])
                    state.state = 'WAITING_FOR_BREAKOUT'
                    logger.info(f"{state.ticker}: ORB set - High: {state.orb_high}, Low: {state.orb_low}. State -> WAITING_FOR_BREAKOUT")

        except Exception as e:
            logger.error(f"Error processing ORB message: {e} | Message: {orb_message}")
        finally:
            self.r.r.xack(self.ORB_STREAM, self.CONSUMER_GROUP, stream_id)


    async def _process_one_min_candle(self, candle_message: Dict[str, Any]):
        """Processes a new 1-minute candle and applies the strategy logic."""
        token = None
        stream_id = None
        try:
            token = int(candle_message['token'])
            stream_id = candle_message['stream_id']
            
            state = self.instruments_state.get(token)
            if not state or state.state in ['WAITING_FOR_ORB', 'TRADE_CLOSED']:
                return # Skip processing if not ready or trade is closed

            candle = self._parse_candle(candle_message)
            
            # Trading window check (entry cut-off at 3:00 PM IST)
            trading_end_time = time(15, 0, 0, tzinfo=self.tz)
            if candle['timestamp'].time() >= trading_end_time:
                async with state.lock:
                    if state.state != 'IN_TRADE':
                        state.state = 'TRADE_CLOSED'
                        logger.info(f"{state.ticker}: End of entry window reached. State -> TRADE_CLOSED.")
                # Important: check_exit still runs below if IN_TRADE, but new signals are blocked.

            async with state.lock:
                if state.state == 'IN_TRADE':
                    self._check_exit(state, candle)
                
                elif state.state in ['WAITING_FOR_BREAKOUT', 'BREAKOUT_DETECTED']:
                    signal = self._detect_setup(state, candle)
                    if signal:
                        await self._execute_trade(state, signal)
                        
                # Update cache after processing the candle
                self._update_candle_cache(state, candle)
            
        except Exception as e:
            logger.error(f"Error processing 1-min candle for token {token}: {e} | Message: {candle_message}")
        finally:
             if token and stream_id:
                # Acknowledge the message regardless of error
                self.r.r.xack(self.CANDLE_STREAM, self.CONSUMER_GROUP, stream_id)


    # --- Strategy Core Logic Helpers ---

    def _parse_candle(self, message: Dict[str, str]) -> Dict[str, Any]:
        """Converts string stream values to appropriate types for a candle."""
        ts = datetime.fromisoformat(message['timestamp']).replace(tzinfo=self.tz)
        return {
            'timestamp': ts,
            'open': float(message['open']),
            'high': float(message['high']),
            'low': float(message['low']),
            'close': float(message['close']),
            'volume': int(message['volume']),
            'token': int(message['token']),
            'ticker': message['ticker']
        }

    def _update_candle_cache(self, state: TickerState, candle: Dict[str, Any]):
        """Maintains the 2-candle history for FVG detection ([C_{-1}, C_{B}])."""
        state.one_min_cache.append(candle)
        # Keep only the last 2 candles 
        if len(state.one_min_cache) > 2:
            state.one_min_cache.pop(0)

    def _is_valid_breaker(self, state: TickerState, candle: Dict[str, Any]) -> Optional[str]:
        """
        Checks for a valid ORB Breaker Candle based on all constraints.
        """
        orb_h = state.orb_high
        orb_l = state.orb_low
        c_open = candle['open']
        c_close = candle['close']
        
        # Constraint 2: Candle body must have some part inside the ORB range.
        body_start = min(c_open, c_close)
        body_end = max(c_open, c_close)
        body_partially_in_range = (body_end > orb_l and body_start < orb_h)
        if not body_partially_in_range:
             return None 

        # Check for Constraint 1 & 3: Breakout Direction and Color
        if c_close > orb_h:
            # Bullish Breakout (Buy Signal)
            is_green_candle = c_close > c_open
            if is_green_candle:
                return 'BUY'
        
        elif c_close < orb_l:
            # Bearish Breakout (Sell Signal)
            is_red_candle = c_close < c_open
            if is_red_candle:
                return 'SELL'

        return None

    def _check_fvg(self, state: TickerState, breaker_direction: str, current_candle: Dict[str, Any]) -> bool:
        """
        Checks for a 3-candle FVG setup using C_{-1} (from cache) and C_{1} (current_candle).
        """
        # Need C-1 and CB in cache (length 2) to check FVG with C1 (current_candle)
        if len(state.one_min_cache) < 2:
            return False

        c_minus_1 = state.one_min_cache[0] # Candle before Breaker (C_{-1})
        c_one = current_candle              # Candle after Breaker (C_{1})
        
        if breaker_direction == 'BUY':
            # Bullish FVG: High of C_{-1} < Low of C_{1}
            return c_minus_1['high'] < c_one['low']
        
        if breaker_direction == 'SELL':
            # Bearish FVG: Low of C_{-1} > High of C_{1}
            return c_minus_1['low'] > c_one['high']

        return False

    def _detect_setup(self, state: TickerState, candle: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Main logic for detecting the Breaker + FVG setup."""
        
        # --- Stage 1: WAITING_FOR_BREAKOUT ---
        if state.state == 'WAITING_FOR_BREAKOUT':
            breaker_direction = self._is_valid_breaker(state, candle)
            
            if breaker_direction:
                # First valid breaker found (this is C_B)
                state.breaker_candle = candle
                state.position = breaker_direction
                state.state = 'BREAKOUT_DETECTED'
                logger.info(f"{state.ticker}: Breaker detected ({breaker_direction}). Waiting for FVG on next candle (C1).")
                return None 

        # --- Stage 2: BREAKOUT_DETECTED (Waiting for C1 to form FVG) ---
        elif state.state == 'BREAKOUT_DETECTED' and state.breaker_candle:
            
            # The current candle is C_1
            
            # --- Handle Edge Case / Constraint Check (Opposite Breakout) ---
            new_breaker_direction = self._is_valid_breaker(state, candle)
            
            if new_breaker_direction and new_breaker_direction != state.position:
                # Edge Case: Breakout opposite way before FVG confirmation. Reset setup to new direction.
                logger.warning(f"{state.ticker}: Opposite breakout detected on C1. Resetting setup. New Dir: {new_breaker_direction}")
                state.breaker_candle = candle # C1 becomes the new C_B
                state.position = new_breaker_direction
                # FVG check for this NEW breaker will happen on the next candle (C_2)
                return None 
            
            # --- Check FVG for current setup (State.position) ---
            is_fvg = self._check_fvg(state, state.position, candle)
            
            if is_fvg:
                # Valid Entry: FVG found after the breaker. Entry at C1's close.
                signal = self._calculate_trade_parameters(state, candle)
                
                # Signal is generated. Strategy Agent stops looking for new signals until trade is closed.
                state.state = 'IN_TRADE' # Strategy Agent changes to IN_TRADE, Order Agent will fulfill
                state.trade_params = signal # Store params for exit check
                logger.info(f"{state.ticker}: Valid Entry Setup! Direction: {state.position}. Generating Signal.")
                return signal
            else:
                # Constraint: No FVG, check if price came back into the range
                if state.orb_low <= candle['close'] <= state.orb_high:
                    logger.info(f"{state.ticker}: No FVG detected on C1 and price is back in range. Resetting to WAITING_FOR_BREAKOUT.")
                    # Reset all setup data to wait for a new breaker
                    state.state = 'WAITING_FOR_BREAKOUT'
                    state.breaker_candle = None
                    state.position = None
                    state.one_min_cache.clear() # Clear cache to restart FVG chain
                # Note: If price is still outside and no FVG, the state remains 'BREAKOUT_DETECTED' to respect 
                # the constraint that the first candle is still the breaker. The price must return to ORB to reset.
                    
        return None

    def _calculate_trade_parameters(self, state: TickerState, entry_candle: Dict[str, Any]) -> Dict[str, Any]:
        """Calculates SL, TP, and Position Size (simple dev size) based on the strategy rules."""
        
        breaker = state.breaker_candle
        entry_price = entry_candle['close']
        
        if state.position == 'BUY':
            stop_loss = breaker['low']
            risk_per_share = entry_price - stop_loss
            profit_target = entry_price + (risk_per_share * 2.0)
            
        elif state.position == 'SELL':
            stop_loss = breaker['high']
            risk_per_share = stop_loss - entry_price
            profit_target = entry_price - (risk_per_share * 2.0)
        else:
            raise ValueError("Invalid trade position for calculation.")

        # --- Position Size Calculation (Using dev fixed USD size) ---
        dev_position_size_usd = self.config['portfolio'].get('dev_position_size_usd', 100.0) # Default to 100
        
        try:
            position_size = int(dev_position_size_usd / entry_price)
            if position_size == 0:
                position_size = 1
        except Exception:
            position_size = 1
            
        
        return {
            'timestamp': entry_candle['timestamp'].isoformat(),
            'ticker': state.ticker,
            'token': state.token,
            'direction': state.position,
            'entry_price': round(entry_price, 2),
            'stop_loss': round(stop_loss, 2),
            'take_profit': round(profit_target, 2),
            'rr_ratio': 2.0,
            'breaker_high': breaker['high'], # For journaling agent
            'breaker_low': breaker['low'],   # For journaling agent
            'position_size': position_size, 
            'status': 'NEW_SIGNAL'
        }

    async def _execute_trade(self, state: TickerState, signal: Dict[str, Any]):
        """Publishes the final trade signal and journaling entry."""
        
        # 1. Publish to Trade Signal Stream
        self.r.publish_message(self.SIGNAL_STREAM, signal)
        logger.info(f"{state.ticker}: !!! TRADE SIGNAL GENERATED ({state.position}) !!! Entry: {signal['entry_price']}, SL: {signal['stop_loss']}, TP: {signal['take_profit']}")
        
        # 2. Publish initial journaling update
        journal_data = {
            'timestamp': signal['timestamp'],
            'ticker': state.ticker,
            'token': state.token,
            'type': 'ENTRY_SIGNAL',
            'details': f"ORB Breakout {state.position} signal generated. Entry: {signal['entry_price']}",
            'metadata': signal
        }
        self.r.publish_message(self.JOURNAL_STREAM, journal_data)


    # --- Exit/Trade Management Logic ---
    
    def _check_exit(self, state: TickerState, candle: Dict[str, Any]):
        """Checks for SL (candle close) or TP (wick included) hit while in a trade."""
        if not state.trade_params:
            return

        sl = state.trade_params['stop_loss']
        tp = state.trade_params['take_profit']
        
        exit_type = None
        exit_price = None

        if state.position == 'BUY':
            # 1. Check TP (wicks included)
            if candle['high'] >= tp:
                exit_type = 'TP_HIT'
                exit_price = tp 
            # 2. Check SL (candle close below SL)
            elif candle['close'] < sl:
                exit_type = 'SL_HIT'
                exit_price = sl 

        elif state.position == 'SELL':
            # 1. Check TP (wicks included)
            if candle['low'] <= tp:
                exit_type = 'TP_HIT'
                exit_price = tp
            # 2. Check SL (candle close above SL)
            elif candle['close'] > sl:
                exit_type = 'SL_HIT'
                exit_price = sl

        if exit_type:
            # Calculate final PnL for the signal
            exit_signal = {
                'timestamp': candle['timestamp'].isoformat(),
                'ticker': state.ticker,
                'token': state.token,
                'position': state.position,
                'entry_price': state.trade_params['entry_price'],
                'exit_price': round(exit_price, 2),
                'exit_type': exit_type,
                # PnL logic: (Exit - Entry) * Size * Multiplier (+1 for Buy, -1 for Sell)
                'pnl': round(
                    (exit_price - state.trade_params['entry_price']) * (state.trade_params['position_size'] if state.position == 'BUY' else -state.trade_params['position_size']), 2
                )
            }
            
            # Publish Trade Closed signal for Portfolio Manager
            self.r.publish_message(self.TRADE_CLOSED_STREAM, exit_signal)
            logger.info(f"{state.ticker}: Trade CLOSED. Type: {exit_type}, PnL: {exit_signal['pnl']}")
            
            # Publish journaling update
            journal_data = {
                'timestamp': exit_signal['timestamp'],
                'ticker': state.ticker,
                'token': state.token,
                'type': exit_type,
                'details': f"Trade closed by {exit_type}. Final PnL: {exit_signal['pnl']}",
                'metadata': exit_signal
            }
            self.r.publish_message(self.JOURNAL_STREAM, journal_data)
            
            # Final state update for the day
            state.state = 'TRADE_CLOSED'
            state.position = None
            state.trade_params = None
            state.breaker_candle = None
            state.one_min_cache.clear()