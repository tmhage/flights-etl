import os
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel("INFO")

def create_folder_if_not_exists(path: str):
    """
    Create a new folder corresponding to todays date if it doesn't exist
    and return the path
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)