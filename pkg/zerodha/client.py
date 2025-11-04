import os
import time
import logging
import json
from urllib.parse import urlparse, parse_qs
from dotenv import dotenv_values

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

import pyotp
from kiteconnect import KiteConnect

# --- Configuration (Load .env values for secrets) ---
# NOTE: Instead of relying on a global config, secrets should be managed by the main orchestrator 
# via the `settings` dictionary, but secrets like ZERODHA_API_KEY still need to come from the environment.
# We will keep this dotenv_values load for the secrets the client needs.
config = dotenv_values(".env")

# --- Logging Setup ---
# The main.py will handle the logging setup, so we only get the logger here
logger = logging.getLogger(__name__) 

# --- Helper Functions ---
def get_kite_client(api_key, access_token=None):
    """Initializes and returns a KiteConnect client."""
    kc = KiteConnect(api_key=api_key)
    if access_token:
        kc.set_access_token(access_token)
    return kc

def save_access_token(token, path):
    """Saves the access token to a JSON file."""
    try:
        with open(path, "w") as f:
            json.dump({"access_token": token}, f)
        logger.info("New access token successfully saved to file.")
    except Exception as e:
        logger.error(f"Failed to write access token to file: {e}")

def load_access_token(path):
    """Loads a saved access token from a JSON file."""
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                data = json.load(f)
                return data.get("access_token")
    except Exception as e:
        logger.error(f"Failed to load access token from file: {e}")
    return None

def login(settings: dict):
    """
    Main function to handle the entire login flow.
    It checks for a valid access token first before performing a new login.
    
    Args:
        settings (dict): The loaded application settings dictionary.
    """
    # 1. Get required secrets from the loaded environment variables via `config` or `settings`
    # NOTE: Assuming secrets like ZERODHA_API_KEY, USERNAME, PASSWORD, TOTP_SECRET, and ACCESS_TOKEN_PATH 
    # are loaded via .env and available in the global `config` object initialized above.
    # In a more robust system, all secrets would be explicitly passed via `settings`.
    API_KEY = config.get("ZERODHA_API_KEY")
    API_SECRET = config.get("ZERODHA_API_SECRET")
    ACCESS_TOKEN_PATH = config.get("ACCESS_TOKEN_PATH", "access_token.json")

    if not API_KEY or not API_SECRET:
        logger.error("ZERODHA_API_KEY or ZERODHA_API_SECRET not found in environment.")
        return None

    # 2. Check for an existing, valid access token
    access_token = load_access_token(ACCESS_TOKEN_PATH)
    if access_token:
        logger.info("Access token file found, attempting to validate...")
        kc = get_kite_client(API_KEY, access_token)
        try:
            profile = kc.profile()
            if profile:
                logger.info("Existing access token is valid.")
                return kc
        except Exception as e:
            logger.warning(f"Existing access token is invalid or expired. Proceeding with new login. Error: {e}")

    # 3. If no valid token, perform a new login
    logger.info("Performing new login to obtain access token.")
    return execute_manual_login(API_KEY, API_SECRET, ACCESS_TOKEN_PATH)

def execute_manual_login(api_key, api_secret, access_token_path):
    """
    Orchestrates the manual login process via a headless browser.
    """
    kc = get_kite_client(api_key)
    login_url = kc.login_url()

    try:
        request_token = perform_selenium_login(login_url)
        logger.info("Successfully obtained request token.")
        
        # Now use the request token to generate a session
        data = kc.generate_session(request_token, api_secret=api_secret)

        # Save the new access token to a file for future use
        save_access_token(data["access_token"], access_token_path)

        # Set the new access token and return the client object
        kc.set_access_token(data["access_token"])
        logger.info("Session and access token generated successfully.")
        return kc
        
    except Exception as e:
        logger.error(f"Failed to complete the login flow: {e}")
        return None

def perform_selenium_login(target_url):
    """
    Automates the Zerodha login process using a headless Selenium browser.
    
    Args:
        target_url (str): The Zerodha login URL.
    
    Returns:
        str: The request_token from the final redirect URL.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    # Use webdriver_manager to automatically handle the ChromeDriver
    # service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(options=options)
    
    try:
        logger.info("Launching headless browser for manual login...")
        driver.get(target_url)
        
        # Wait for the username field to be visible and enter credentials
        wait = WebDriverWait(driver, 60)
        wait.until(EC.visibility_of_element_located((By.ID, "userid")))
        
        logger.info("Submitting username and password.")
        # NOTE: Using global 'config' for secrets like username/password/totp secret
        driver.find_element(By.ID, "userid").send_keys(str(config["ZERODHA_USERNAME"]))
        driver.find_element(By.ID, "password").send_keys(str(config["ZERODHA_PASSWORD"]))
        driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        
        # Generate the TOTP code
        totp = pyotp.TOTP(str(config["ZERODHA_TOTP_SECRET"]))
        passcode = totp.now()
        logger.info("Generated TOTP passcode.")

        # Wait for the TOTP field (same ID as username) and submit
        wait.until(EC.visibility_of_element_located((By.ID, "userid")))
        driver.find_element(By.ID, "userid").send_keys(passcode)
        
        # We don't have a button to click here. The form submits automatically.
        # Wait for the redirect to happen, and get the final URL
        time.sleep(2) # A brief pause to ensure the redirect has started

        final_url = driver.current_url
        logger.info(f"Redirected to final URL: {final_url}")

        # Parse the final URL to extract the request_token
        parsed_url = urlparse(final_url)
        query_params = parse_qs(parsed_url.query)
        
        request_token = query_params.get("request_token", [None])[0]
        
        if not request_token:
            raise ValueError(f"Request token not found in final URL. Login likely failed. URL: {final_url}")
            
        return request_token
        
    except (TimeoutException, NoSuchElementException) as e:
        logger.error(f"Selenium automation failed: {e}")
        raise RuntimeError("Selenium failed to find required elements or timed out.") from e
    finally:
        driver.quit()

# Example usage (removed the direct run to prevent confusion with main.py's orchestration)
# if __name__ == "__main__":
#     ...
