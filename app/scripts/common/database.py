from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from scripts.common.utils import logger
from config import DB
import psycopg2

URI = f"postgresql+psycopg2://{DB.POSTGRES_USER}:{DB.POSTGRES_PASSWORD}@db:5432/postgres"

logger.info(f"[INIT] connecting to {URI}..")

engine = create_engine(URI)

logger.info(f"[INIT] engine is {engine}")

Session = sessionmaker(bind=engine)

Base = declarative_base()

from scripts.common.models import FlightsRaw, FlightsClean

Base.metadata.create_all(engine, checkfirst=True)

session = Session()
