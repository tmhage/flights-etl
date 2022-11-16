from sqlalchemy import Table, Column, Integer, String, Date, BigInteger
from scripts.common.database import Base

flights_raw = Table(
    "flights_raw",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column("flight_name", String),
    Column("flight_number", String),
    Column("schedule_datetime", String),
    Column("airline", String),
    Column("flight_id", String)
)

flights_clean = Table(
    "flights_clean",
    Base.metadata,
    Column("id", Integer, primary_key=True),
    Column("flight_name", String(55)),
    Column("flight_number", String(55)),
    Column("schedule_datetime", Date),
    Column("airline", String(255)),
    Column("flight_id", BigInteger)
)


class FlightsRaw(Base):
    __table__ = flights_raw

    def __repr__(self):
        return "<Flight Raw {}>".format(self.id)


class FlightsClean(Base):
    __table__ = flights_clean

    def __repr__(self):
        return "<Flight Clean {}>".format(self.id)




