from pandas import DataFrame
import pandas as pd
import json
import csv
from config import PATH
from scripts.common.database import session
from scripts.common.utils import create_folder_if_not_exists
from scripts.common.models import FlightsRaw
from sqlalchemy import text
from etl import logger


flight_columns = ["flightName", 'flightNumber', "scheduleDate", "scheduleTime", "aircraftType", "id"]


def select_and_rename_columns(df: DataFrame, selected: dict) -> DataFrame:
    """ Only select given column names and rename them """
    df = df[selected.keys()].rename(columns=selected)
    return df


def read_json_file(file_path: str) -> dict:
    """ read json file and return dictionary """
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def transform_flights_to_dataframe(flights: dict) -> DataFrame:
    """ Convert dictionary to Dataframe and transform """
    df = pd.json_normalize(flights['flights'], sep="_")
    # select only needed columns and rename them
    selected_columns = {
        "flightName": "flight_name",
        "flightNumber": "flight_number",
        "scheduleDate": "schedule_date",
        "scheduleTime": "schedule_time",
        "prefixIATA": "iata",
        "aircraftType_iataMain": "iata_main",
        "aircraftType_iataSub": "iata_sub",
        "id": "flight_id"
    }
    df = select_and_rename_columns(df, selected_columns)

    # merge scheduleDate and scheduleTime into scheduled_datetime column
    df["schedule_datetime"] = df["schedule_date"].map(str) + " " + df["schedule_time"].map(str)
    df.drop(["schedule_date", "schedule_time"], axis=1, inplace=True)

    return df


def transform_airlines_to_dataframe(destinations: dict) -> DataFrame:
    """ Convert dictionary to Dataframe and transform """
    df = pd.json_normalize(destinations['airlines'])
    df.drop(["nvls", "icao"], axis=1, inplace=True)
    df.rename(columns={"publicName": "airline"}, inplace=True)
    df.dropna(inplace=True, subset=["iata"])
    df.set_index("iata", inplace=True)
    return df


def join_flights_and_airlines(flights: DataFrame, airlines: DataFrame) -> DataFrame:
    """ Merge the dataframes on the column 'iata' """
    df = flights.join(airlines, on="iata")
    df.fillna(inplace=True, value={"airline": "unknown"})
    return df


def save_raw_to_csv(data: DataFrame):
    """ Save joined raw data to csv """
    create_folder_if_not_exists(PATH.RAW)
    data.to_csv(PATH.RAW)
    logger.info(f"[Transform] Saved raw joined data to csv")
    logger.info(f"[Transform] Path {PATH.RAW}")


def commit_raw_to_database():
    with open(PATH.RAW, mode="r") as csv_file:
        reader = csv.DictReader(csv_file)
        flights_raw_objects = []
        for row in reader:
            # Apply rows to FlightsRaw object and append to list
            flights_raw_objects.append(
                FlightsRaw(
                    flight_name=row["flight_name"],
                    flight_number=row["flight_number"],
                    schedule_datetime=row["schedule_datetime"],
                    airline=row["airline"],
                    flight_id=int(row["flight_id"])
                )
            )
        logger.info(f'[Transform] Bulk saving {len(flights_raw_objects)} flight_raw objects')
        # Save all new processed objects and commit
        session.bulk_save_objects(flights_raw_objects)
        session.commit()


def truncate_table():
    """ Drop table before adding the new transformations """
    logger.info(f"[Transform] Dropping raw table with {len(session.query(FlightsRaw).all())} flights")
    session.execute(text("TRUNCATE TABLE flights_raw RESTART IDENTITY;"))
    session.commit()


def main():
    truncate_table()
    logger.info("[Transform] Prepare flights dataframa..")
    flights = read_json_file(PATH.FLIGHTS_SOURCE)
    flights_df = transform_flights_to_dataframe(flights)
    logger.info("[Transform] Prepare airlines dataframa..")
    airlines = read_json_file(PATH.AIRLINES_SOURCE)
    airlines_df = transform_airlines_to_dataframe(airlines)
    logger.info("[Transform] Joining the dataframes..")
    joined_df = join_flights_and_airlines(flights_df, airlines_df)
    logger.info("[Transform] Saving joined data to csv..")
    save_raw_to_csv(joined_df)
    logger.info("[Transform] Commit to raw staging table..")
    commit_raw_to_database()
