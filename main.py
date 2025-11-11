import asyncio
import logging
import logging.config
import yaml
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import pytz
from typing import Dict, Any
from pathlib import Path 

# Application imports
from pkg.config.config_loader import load_settings
from pkg.zerodha.client import login
from pkg.redis.redis_client import RedisClient
from pkg.database.db_connector import DBConnector 
from agents.datacollector.data_collector import DataCollectorAgent
from agents.universe.universe_agent import UniverseAgent 
from agents.strategy.strategy_agent import StrategyAgent # <--- NEW IMPORT

logger = logging.getLogger(__name__) 

# Get the root directory for path resolution
ROOT_DIR = Path(__file__).parent

# Initialize logger (needs to be done before agent initialization)
def setup_logging(config_path="pkg/config/logging_config.yaml"):
    """Setup logging configuration."""
    # Resolve path relative to the script's location
    config_path = ROOT_DIR / config_path
    
    # Create the logs directory if it doesn't exist
    log_dir = ROOT_DIR / 'logs'
    if not log_dir.exists():
        log_dir.mkdir()

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logging.config.dictConfig(config)
        return logging.getLogger('root') 
    except Exception as e:
        print(f"Failed to set up logging: {e}")
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger('root') 

def get_initial_universe_map(kc_client: Any, settings: Dict) -> Dict[str, int]:
    """Loads the stored universe map or generates a new one if missing."""
    universe_agent = UniverseAgent(kc_client=kc_client, settings=settings)
    # The load_universe_map method should be available on UniverseAgent
    # (Assuming it was implemented in the full version of universe_agent.py)
    return universe_agent.load_universe_map() 

async def main():
    """Main asynchronous entry point for the T-Bot application."""
    
    # 1. Setup Logging
    setup_logging()
    logger.info("--- T-Bot Application Initializing ---")

    # 2. Load Configuration and Secrets
    settings = load_settings()
    
    # 3. Initialize Database and Redis Connections
    db_connector = DBConnector(settings['database'])
    if not db_connector.connect():
        logger.error("Database connection failed. Shutting down.")
        return

    r = RedisClient(settings['redis']['host'], int(settings['redis']['port']), int(settings['redis']['db']))
    if not r.check_connection():
        logger.error("Redis connection failed. Shutting down.")
        return

    # 4. Initialize KiteConnect Client (Kite Client)
    kc = login(settings=settings)
    if not kc:
        logger.error("KiteConnect client login failed. Shutting down.")
        db_connector.close()
        return

    # 5. Get Initial Universe Map (from file/DB)
    # Note: I'm assuming load_universe_map is implemented in universe_agent.py
    universe_agent_loader = UniverseAgent(kc_client=kc, settings=settings)
    ticker_token_map = universe_agent_loader.load_universe_map() 
    
    if not ticker_token_map:
        logger.warning("No initial universe map loaded. Running Universe Agent to create one.")
        ticker_token_map = universe_agent_loader.run()
    
    if not ticker_token_map:
        logger.error("Cannot proceed without a universe map. Shutting down.")
        db_connector.close()
        return

    # 6. Initialize APScheduler
    scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Kolkata'))
    
    # 7. Initialize Universe Agent (for scheduled daily updates)
    scheduler.add_job(
        universe_agent_loader.run, 
        trigger='cron',
        day_of_week='mon-fri', 
        hour=8, 
        minute=5, 
        second=0,
        name='Daily_Universe_Update'
    )
    logger.info("Universe Agent scheduled to run weekdays at 8:05 AM IST.")

    # 8. Initialize and Run Data Collector Agent
    data_collector_agent = DataCollectorAgent(
        kc_client=kc, 
        redis_client=r, 
        db_connector=db_connector, 
        config=settings
    )
    
    collector_task = asyncio.create_task(data_collector_agent.run(ticker_token_map))
    logger.info(f"Data Collector Agent started with {len(ticker_token_map)} instruments.")
    
    # 9. Initialize and Run Strategy Agent <--- START NEW AGENT
    strategy_agent = StrategyAgent(
        redis_client=r,
        config=settings
    )
    strategy_task = asyncio.create_task(strategy_agent.run(ticker_token_map))
    logger.info(f"Strategy Agent started.")

    # 10. Start Scheduler
    scheduler.start()
    
    # Keep the main loop running until all tasks are complete (or interrupted)
    try:
        # Wait indefinitely for background tasks
        await asyncio.gather(collector_task, strategy_task)
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
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shut down by user.")