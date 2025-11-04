import os
from dotenv import load_dotenv
import yaml

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../.env"))

def load_settings(settings_path=None):
    """Load settings from settings.yaml and replace placeholders with environment variables."""
    if not settings_path:
        settings_path = os.path.join(os.path.dirname(__file__), "settings.yaml")
    with open(settings_path, "r") as file:
        settings = yaml.safe_load(file)
    # Replace placeholders with environment variables
    for key, value in os.environ.items():
        settings = replace_placeholders(settings, key, value)
    return settings

def replace_placeholders(config, key, value):
    """Recursively replace placeholders in the config dictionary."""
    if isinstance(config, dict):
        return {k: replace_placeholders(v, key, value) for k, v in config.items()}
    elif isinstance(config, list):
        return [replace_placeholders(v, key, value) for v in config]
    elif isinstance(config, str) and f"${{{key}}}" in config:
        return config.replace(f"${{{key}}}", value)
    return config

# Example usage
if __name__ == "__main__":
    settings = load_settings()
    print(settings)