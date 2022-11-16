import os
import datetime


class PATH:
    today = str(datetime.datetime.now().date())
    BASE = os.path.abspath(__file__ + "/../")
    FLIGHTS_SOURCE = "{}/data/source/flights/downloaded_at={}/flights.json".format(BASE, today)
    AIRLINES_SOURCE = "{}/data/source/airlines/downloaded_at={}/airlines.json".format(BASE, today)
    RAW = "{}/data/raw/downloaded_at={}/flights.csv".format(BASE, today)
    CLEAN = f"{BASE}/data/clean/"


class API:
    FLIGHTS_URL = "https://api.schiphol.nl/public-flights/flights?flightDirection=D&includedelays=false&page=0&sort=%2BscheduleTime"
    AIRLINES_URL = "https://api.schiphol.nl/public-flights/airlines?page=0&sort=%2BpublicName"
    KEY = "08af4531933b71a6c0c32a999d7f1340"
    ID = "1f3faee8"
    MAX_RECORDS = int(os.getenv("MAX_RECORDS"))


class DB:
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
