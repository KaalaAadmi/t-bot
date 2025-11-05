import json
import logging
import os
import pandas as pd
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Ensure project root on sys.path so 'pkg' imports work when running this file directly
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
    
# External dependencies
from kiteconnect import KiteConnect
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
# You may need to import Service or ChromeDriverManager if not using standard PATH
# from selenium.webdriver.chrome.service import Service
# from webdriver_manager.chrome import ChromeDriverManager 

from pkg.config.config_loader import load_settings
from dotenv import dotenv_values
from pkg.zerodha.client import login

# Initialize logger (using 'root' for consistency with main.py)
logger = logging.getLogger(__name__)

class UniverseAgent:
    """
    Manages the bot's trading universe: scraping current index components and 
    mapping them to Zerodha instrument tokens.
    """
    def __init__(self, kc_client: KiteConnect, settings: Dict):
        self.kc = kc_client
        self.settings = settings
        # The path where the final map (ticker: token) will be stored
        self.output_path = settings.get('universe', {}).get('file_path', 'data/universe_map.json')
        # Tickers to scrape (e.g., NIFTY, BANKNIFTY)
        self.indices_to_scrape = ['NIFTY', 'BANKNIFTY']
        # TradingView URL template for scraping
        # self.scrape_url_template = f"https://in.tradingview.com/symbols/NSE-{index_name}/components/"

        # Constants for cleaning tickers
        # self.NSE_SUFFIX = ".NS"
        # self.BSE_SUFFIX = ".BO"
        logger.info("Initialized UniverseAgent.")

    # def clean_ticker(self, ticker_full: str) -> str:
    #     """Removes the exchange suffix (.NS, .BO, etc.) from the ticker name."""
    #     if ticker_full.endswith(self.NSE_SUFFIX):
    #         return ticker_full[:-len(self.NSE_SUFFIX)]
    #     if ticker_full.endswith(self.BSE_SUFFIX):
    #         return ticker_full[:-len(self.BSE_SUFFIX)]
    #     return ticker_full

    def get_tickers(self, index_name: str) -> List[str]:
        """
        Scrapes a specific index component list from TradingView using Selenium.
        Args:
            index_name (str): The name of the index (e.g., 'NIFTY').
        Returns:
            list: A list of scraped and cleaned ticker symbols.
        """
        options = Options()
        options.add_argument("--headless")  # Run in headless mode (CRITICAL for servers)
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        # Add a user agent to avoid bot detection
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        
        # Construct the URL
        url = f"https://in.tradingview.com/symbols/NSE-{index_name}/components/"
        
        # Initialize the driver
        driver = None
        try:
            # Assumes chromedriver is available in the system PATH or container environment
            driver = webdriver.Chrome(options=options) 
            logger.info(f"Navigating to {url} to scrape {index_name} tickers.")
            driver.get(url)

            # Wait for the main data table to load (look for the ticker link elements)
            # The XPATH is from the original file, targeting the anchor tags for symbols
            wait_selector = ".screener-container-is-scrolled-to-end"
            wait = WebDriverWait(driver, 120)  # Wait up to 120 seconds
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, wait_selector)))
            # The .tickerName-GrtoTeat selector from the Go code finds the ticker elements
            ticker_selector = ".tickerName-GrtoTeat"
            ticker_elements = driver.find_elements(By.CSS_SELECTOR, ticker_selector)
            
            tickers = [el.text.strip() for el in ticker_elements]
            logger.info(f"Scraped {len(tickers)} tickers for {index_name}")
            
            # Clean and filter the tickers, similar to the Go code's post-processing
            cleaned_tickers = [ticker for ticker in tickers if ticker]

            # Apply specific replacements
            for i, ticker in enumerate(cleaned_tickers):
                if ticker == "BAJAJ_AUTO":
                    cleaned_tickers[i] = "BAJAJ-AUTO"
                # Handle other cases if needed, e.g., M&M
                elif ticker == "M&M":
                    cleaned_tickers[i] = "M&M"
            
            return cleaned_tickers
            
        except TimeoutException:
            logger.error(f"Timed out waiting for page elements on {url}")
            return []
        except Exception as e:
            logger.error(f"An error occurred while scraping {url}: {e}")
            return []
        finally:
            if driver:
                driver.quit()

    # --- Other Methods (No changes needed, but included for context) ---
    def get_instrument_tokens(self, tickers: List[str]) -> Dict[str, int]:
        """
        Takes a list of base tickers and fetches their instrument token map 
        from the Zerodha Kite API.
        """
        logger.info(f"Fetching instrument tokens for {len(tickers)} tickers...")
        try:
            instrument_list = self.kc.instruments('NSE')  # Filtered to NSE
        except Exception as e:
            logger.error(f"Failed to fetch instrument list from Kite: {e}")
            return {}
        
        # Build lookup: only NSE cash-equity (instrument_type == 'EQ')
        instrument_map = {
            ins['tradingsymbol'].upper(): int(ins['instrument_token'])
            for ins in instrument_list
            if ins.get('exchange') == 'NSE' and ins.get('instrument_type') == 'EQ'
        }

        ticker_token_map: Dict[str, int] = {}
        missing: List[str] = []
        for ticker in tickers:
            key = ticker.strip().upper().replace('_', '-')  # normalize common variants
            token = instrument_map.get(key)
            if token:
                ticker_token_map[key] = token
            else:
                missing.append(ticker)

        if missing:
            logger.warning(f"{len(missing)} tickers not found on NSE EQ: {missing[:10]}{' ...' if len(missing) > 10 else ''}")
            
        logger.info(f"Successfully mapped {len(ticker_token_map)} tickers to tokens.")
        return ticker_token_map

    def deduplicate_tickers(self, tickers):
        """
        Deduplicates a list of tickers.
        Args:
            tickers (list): List of ticker symbols.
        Returns:
            list: Deduplicated list of ticker symbols.
        """
        return list(set(tickers))
    
    def save_universe_map(self, ticker_token_map: Dict[str, int]):
        """
        Saves the final ticker-token map to a JSON file.
        """
        directory = os.path.dirname(self.output_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Created directory: {directory}")

        with open(self.output_path, 'w') as f:
            json.dump(ticker_token_map, f, indent=4)
            logger.info(f"Saved {len(ticker_token_map)} instruments to {self.output_path}")

    def load_universe_map(self) -> Optional[Dict[str, int]]:
        """
        Load the cached ticker-token map from disk.
        Returns:
            Dict[str, int] if available and valid; otherwise None.
        """
        path = self.output_path
        try:
            if not path:
                logger.error("Universe map path is not configured in settings.")
                return None

            if not os.path.isfile(path):
                logger.info(f"Universe map not found at {path}.")
                return None

            with open(path, "r") as f:
                data = json.load(f)

            if not isinstance(data, dict):
                logger.warning(f"Universe map at {path} is not a dict. Ignoring.")
                return None

            normalized: Dict[str, int] = {}
            for k, v in data.items():
                if k is None:
                    continue
                key = str(k).strip().upper().replace("_", "-")
                try:
                    normalized[key] = int(v)
                except (TypeError, ValueError):
                    logger.warning(f"Invalid token for {k}: {v}. Skipping.")

            if not normalized:
                logger.info("Universe map file is empty after normalization.")
                return None

            logger.info(f"Loaded {len(normalized)} instruments from {path}")
            return normalized

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse universe map JSON at {path}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading universe map from {path}: {e}")
            return None
        
    def run(self) -> Dict[str, int]:
        """
        Orchestrates the scraping, token mapping, and saving of the trading universe.
        """
        logger.info("Starting Universe Update: Scraping component tickers...")
        
        # 1. Scrape Tickers
        all_tickers = []
        for index in self.indices_to_scrape:
            # This is now the actual web scraping call
            tickers = self.get_tickers(index)
            if tickers:
                all_tickers.extend(tickers)
                
        if not all_tickers:
            logger.error("Failed to scrape any tickers from any index. Cannot proceed with token mapping.")
            return {}
        
        # Deduplicate and clean (simple set conversion is enough for deduplication)
        unique_tickers = self.deduplicate_tickers(all_tickers)
        logger.info(f"Total unique tickers scraped: {len(unique_tickers)}")

        # 2. Get Instrument Tokens from Kite API
        ticker_token_map = self.get_instrument_tokens(unique_tickers)
        
        # 3. Save the result
        if ticker_token_map:
            self.save_universe_map(ticker_token_map)
            
        return ticker_token_map

if __name__=="__main__":
    # Example usage (this would normally be in main.py)
    from kiteconnect import KiteConnect
    
    # Mock settings and KiteConnect client for demonstration
    settings = load_settings()  # Assume this function loads your settings
    config = dotenv_values('.env')
    kc_client=login(settings=settings)

    universe_agent = UniverseAgent(kc_client=kc_client, settings=settings)
    universe_map = universe_agent.run()
    print(universe_map)