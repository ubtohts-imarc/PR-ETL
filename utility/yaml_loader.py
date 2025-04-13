import yaml
from utility.logger import get_logger

logger = get_logger()

def load_yaml_config(file_path):
    """
    Reads a YAML configuration file and returns its contents as a dictionary.
    Handles errors gracefully and returns None if file is missing or invalid.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {file_path}")
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {file_path}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error while loading YAML file {file_path}: {e}")
    return None