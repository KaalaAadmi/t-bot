import os
from dotenv import load_dotenv
import yaml

# Load environment variables from .env file
# Ensure the path is correct relative to config_loader.py (pkg/config) -> (..) -> (..) -> .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../.env"))

def load_settings(settings_path=None):
    """Load settings from settings.yaml and replace placeholders with environment variables."""
    if not settings_path:
        settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
    with open(settings_path, "r") as file:
        settings = yaml.safe_load(file)
        
    # Recursive replacement for other settings (e.g., ports, generic vars)
    for key, value in os.environ.items():
        settings = replace_placeholders(settings, key, value)
        
    # --- CRITICAL FIX: Direct assignment for Database configuration ---
    # This bypasses any potential issues with the recursive replacement
    if 'database' in settings:
        # Use .get() on the dictionary for safety, but rely on os.getenv for the actual value
        settings['database']['user'] = os.getenv('POSTGRES_USER', settings['database'].get('user', ''))
        settings['database']['password'] = os.getenv('POSTGRES_PASSWORD', settings['database'].get('password', ''))
        # This line must set the 'db' key using POSTGRES_DB (which should be 'tbot_db')
        settings['database']['db'] = os.getenv('POSTGRES_DB', settings['database'].get('db', ''))
        settings['database']['host'] = os.getenv('POSTGRES_HOST', settings['database'].get('host', ''))
        settings['database']['port'] = os.getenv('POSTGRES_PORT', settings['database'].get('port', ''))
    # ------------------------------------------------------------------
    
    return settings

def replace_placeholders(config, key, value):
    """Recursively replace placeholders in the config dictionary."""
    if isinstance(config, dict):
        return {k: replace_placeholders(v, key, value) for k, v in config.items()}
    elif isinstance(config, list):
        return [replace_placeholders(v, key, value) for v in config]
    elif isinstance(config, str) and f"${{{key}}}" in config:
        # Check for the default value fallback syntax in YAML (e.g. ${VAR:-default})
        if ":-" in config:
            # Simple replacement, relying on the environment to provide the full value including fallback
            return config.replace(f"${{{key}}}", value)
        else:
            return config.replace(f"${{{key}}}", value)
    return config

# Example usage
if __name__ == "__main__":
    settings = load_settings()
    print(settings)