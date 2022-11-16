from scripts.common.database import session
from scripts.common.models import FlightsRaw, FlightsClean
from sqlalchemy import cast, Date, BigInteger
from sqlalchemy.dialects.postgresql import insert
from scripts.common.utils import logger


def insert_flights():
    """
    Adding new flight data
    """
    # Get all flight_id from the clean table
    clean_flight_ids = session.query(FlightsClean.flight_id)
    logger.info(f"[Load] Found {clean_flight_ids.count()} flights in FlightClean table")
    # Query for flights NOT IN FlightsClean by their unique flight_id
    # flight_id and schedule_datetime need to be casted to FlightsClean datatypes
    raw_flight_id = cast(FlightsRaw.flight_id, BigInteger)
    new_flights = session.query(
        FlightsRaw.flight_name,
        FlightsRaw.flight_number,
        cast(FlightsRaw.schedule_datetime, Date),
        FlightsRaw.airline,
        raw_flight_id,
    ).filter(~raw_flight_id.in_(clean_flight_ids))

    logger.info(f"[Load] New flights to insert: {new_flights.count()}")

    # Insert found new flights
    insert_statement = insert(FlightsClean).from_select(
        ["flight_name", "flight_number", "schedule_datetime", "airline", "flight_id"],
        new_flights,
    )

    logger.info("[Load] Executing insert statement for clean data")

    session.execute(insert_statement)
    session.commit()

    logger.info("[Load] Commit complete.")
    logger.info(f"[Load] Currently {session.query(FlightsClean).count()} flights in clean table")


def main():
    logger.info("[Load] Start inserting process..")
    insert_flights()
