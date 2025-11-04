import asyncio
import logging
import logging.config
import yaml
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler # <--- NEW IMPORT
import pytz
from typing import Dict, Any

# Application imports
from pkg.config.config_loader import load_settings
from pkg.zerodha.client import login
from pkg.redis.redis_client import RedisClient
from pkg.database.db_connector import DBConnector 
from agents.datacollector.data_collector import DataCollectorAgent
from agents.universe.universe_agent import UniverseAgent # <--- NEW IMPORT

logger = logging.getLogger(__name__) 

# Initialize logger (needs to be done before agent initialization)
def setup_logging(config_path="config/logging_config.yaml"):
    """Setup logging configuration."""
    if not os.path.exists(config_path):
        # Fallback if running from a different directory structure
        config_path = os.path.join(os.path.dirname(__file__), config_path)
    
    # Create the logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    logging.config.dictConfig(config)
    # Get the root logger after config is loaded
    return logging.getLogger('root') 


def get_initial_universe_map(kc_client: Any, settings: Dict) -> Dict[str, int]:
    """
    Handles the synchronous fetching of the ticker-token map at startup.
    It first tries to load from cache, then runs the full UniverseAgent process if needed.
    """
    logger.info("--- Phase 1: Fetching Trading Universe Map ---")
    
    # Initialize the Universe Agent
    universe_agent = UniverseAgent(kc_client=kc_client, settings=settings)
    
    # 1. Try to load from cache first for fast startup
    universe_map = universe_agent.load_universe_map()
    
    if universe_map is not None:
        return universe_map
    
    # 2. If cache failed or is stale, run the full process (scrape + tokenize)
    # This must be run synchronously before we start the Data Collector
    universe_map = universe_agent.run()
    
    if not universe_map:
        logger.critical("FAILED TO DETERMINE TRADING UNIVERSE. Shutting down.")
        raise RuntimeError("Failed to get initial trading universe.")
        
    return universe_map


async def main():
    """Main function to orchestrate the bot and start all agents."""
    
    # 1. Setup Logging
    logger = setup_logging()
    logger.info("--- T-Bot Application Starting ---")
    
    # 2. Load Configuration
    settings = load_settings()
    logger.info(f"Settings loaded for environment: {settings['environment']}")
    
    # 3. Initialize Zerodha Client and Login
    kc = login(settings) 
    if not kc:
        logger.error("Zerodha login failed. Shutting down application.")
        return
    logger.info("Zerodha login successful. KiteConnect client initialized.")
    
    # 4. Initialize and Verify Redis Client
    redis_settings = settings['redis']
    stream_names = list(settings['streams'].values())

    r = RedisClient(
        host=redis_settings['host'],
        port=redis_settings['port'],
        db=int(redis_settings['db'])
    )
    
    if not r.check_connection():
        logger.error("Failed to connect to Redis. Shutting down application.")
        return
        
    # 5. Initialize Redis Streams (Declare channels)
    r.initialize_streams(stream_names)
    logger.info(f"All {len(stream_names)} Redis streams initialized/declared.")
    
    # 6. Initialize and Verify Database Connector
    db_connector = DBConnector(settings['database'])
    if not db_connector.connect():
        logger.error("Database connection failed. Shutting down application.")
        return
    
    # Ensure all tables are created
    db_connector.initialize_tables()
    
    # 7. Get the initial Ticker-Token Universe Map <--- CRITICAL CHANGE
    try:
        ticker_token_map = get_initial_universe_map(kc, settings)
    except RuntimeError:
        db_connector.close()
        return

    # 8. Setup Scheduler for Periodic Universe Updates
    tz = pytz.timezone('Asia/Kolkata')
    scheduler = AsyncIOScheduler(timezone=tz)
    
    # Instantiate the Universe Agent once for scheduling
    universe_agent_scheduled = UniverseAgent(kc_client=kc, settings=settings)
    
    # Schedule the UniverseAgent to run every weekday at 8:00 AM IST
    scheduler.add_job(
        universe_agent_scheduled.run, 
        'cron', 
        day_of_week='mon-fri', 
        hour=8, 
        minute=0, 
        second=0,
        name='Daily_Universe_Update'
    )
    scheduler.start()
    logger.info("Universe Agent scheduled to run weekdays at 8:00 AM IST.")
    
    # 9. Initialize and Run Data Collector Agent
    data_collector_agent = DataCollectorAgent(
        kc_client=kc, 
        redis_client=r, 
        db_connector=db_connector, 
        config=settings
    )
    
    # Start the Data Collector Agent, passing the newly acquired map
    collector_task = asyncio.create_task(data_collector_agent.run(ticker_token_map)) # <--- PASS DYNAMIC MAP
    
    logger.info(f"Data Collector Agent started with {len(ticker_token_map)} instruments.")
    
    # Keep the main loop running until all tasks are complete (or interrupted)
    try:
        # Wait indefinitely for background tasks
        await collector_task
    except asyncio.CancelledError:
        logger.info("Main application tasks cancelled.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in the main loop: {e}")
    finally:
        # Clean up resources
        if db_connector:
            db_connector.close()
        scheduler.shutdown()
        logger.info("--- T-Bot Application Shut Down ---")


if __name__ == "__main__":
    try:
        # Use asyncio.run for starting the asynchronous main function
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nApplication manually interrupted. Exiting.")
