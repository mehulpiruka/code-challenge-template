from google.cloud.sql.connector import Connector,IPTypes
from jsonschema.benchmarks.subcomponents import schema
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import sessionmaker
import psycopg2

# initialize Connector object
connector = Connector()

# function to return the database connection
def getconn():
    conn = connector.connect(
        "interview-434306:us-central1:corteva-agriscience",
        "pg8000",
        user="postgres",
        password="test@123",
        db="postgres",
        ip_type=IPTypes.PUBLIC
    )
    return conn

# create connection pool
engine = create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)
print(engine.connect())
# Define your database models here (example):
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
    # station_id = Column(Integer, ForeignKey("weather_station.id"))
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)
    UniqueConstraint( 'station_id','record_date', name="uni_records")

class YearlyStats(Base):
    __tablename__ = "weather_data_stats"
    __table_args__ = {"schema": "db"}
    stats_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    station_id = Column(Integer, ForeignKey("db.weather_station.station_id"))
    avg_max_temp = Column(Float)
    avg_min_temp = Column(Float)
    total_precipitation = Column(Float)
    UniqueConstraint('year','station_id',name="uni_stats")
# Create database tables if they don't exist
Base.metadata.create_all(engine)

# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()