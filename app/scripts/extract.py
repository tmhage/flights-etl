import requests
import json
from config import PATH, API
from scripts.common.utils import create_folder_if_not_exists, logger
from collections import defaultdict

headers = {
    'accept': 'application/json',
    'resourceversion': 'v4',
    'app_id': API.ID,
    'app_key': API.KEY
}


def get_data(url: str, data: dict = None) -> dict:
    """ Recursive function to get all data in case of pagination """
    data = defaultdict(list) if data is None else data

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.ConnectionError as error:
        raise SystemExit(error)

    if response.status_code == 200:
        json_data = response.json()
        key = list(json_data.keys())[0]
        data[key].extend(json_data[key])

        if 'next' in response.links and len(data[key]) < API.MAX_RECORDS:
            return get_data(response.links['next']['url'], dict(data))
        else:
            logger.info(f"[Extract] Retrieved {len(data[key])} {key} from API")
            return dict(data)
    else:
        logger.warning("Something went wrong. Http response code:\
        {} {}".format(response.status_code, response.text))


def save_source_as_json(data: dict, path: str):
    """ Saves json file from data to path """
    create_folder_if_not_exists(path)
    with open(path, "w") as file:
        json.dump(data, file)
    logger.info(f"[Extract] Saved file to {path}")


def main():
    logger.info("[Extract] Request flights data from api..")
    flights = get_data(API.FLIGHTS_URL)
    logger.info("[Extract] Request airlines data from api..")
    airlines = get_data(API.AIRLINES_URL)
    logger.info("[Extract] Saving flights to json..")
    save_source_as_json(flights, PATH.FLIGHTS_SOURCE)
    logger.info("[Extract] Saving airlines to json..")
    save_source_as_json(airlines, PATH.AIRLINES_SOURCE)
