from google.cloud.sql.connector import Connector, IPTypes
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import sessionmaker

# initialize Connector object
connector = Connector()


# Database models
class WeatherStation(Base):
    __tablename__ = "weather_station"
    __table_args__ = {"schema": "db"}

    station_id = Column(Integer, primary_key=True)
    station_name = Column(String(255))
    location = Column(String(255))


class WeatherDataRecords(Base):
    __tablename__ = "weather_data_records"
    __table_args__ = {"schema": "db"}
    record_id = Column(Integer, primary_key=True)
    station_id = Column(Integer, ForeignKey("db.weather_station.station_id"))
    record_date = Column(Date)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)
    UniqueConstraint("station_id", "record_date", name="uni_records")


class YearlyStats(Base):
    __tablename__ = "weather_data_stats"
    __table_args__ = {"schema": "db"}
    stats_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    station_id = Column(Integer, ForeignKey("db.weather_station.station_id"))
    avg_max_temp = Column(Float)
    avg_min_temp = Column(Float)
    total_precipitation = Column(Float)
    UniqueConstraint("year", "station_id", name="uni_stats")


class YieldStats(Base):
    __tablename__ = "year_yield_stats"
    __table_args__ = {"schema": "db"}
    yield_id = Column(Integer, primary_key=True)
    yield_year = Column(Integer)
    yield_amt = Column(Integer)


# function to return the database connection
def get_conn():
    """
    Get an connection object
    :return: connector object
    """
    conn = connector.connect(
        "interview-434306:us-central1:corteva-agriscience",
        "pg8000",
        user="postgres",
        password="test@123",
        db="postgres",
        ip_type=IPTypes.PUBLIC,
    )
    return conn


# create connection pool
engine = create_engine(
    "postgresql+pg8000://",
    creator=get_conn,
)
# Create database tables if they don't exist
Base.metadata.create_all(engine)


# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()
