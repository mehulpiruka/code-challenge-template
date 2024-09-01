# import pandas as pd
import os
from datetime import datetime
from sqlalchemy.sql import func
from sqlalchemy import extract


from wheel.db_connection import WeatherDataRecords,session, WeatherStation, YearlyStats

def insert_station(station_id,station_name):
    import random
    print(station_id,station_name)
    location = random.choice(["Nebraska", "Iowa", "Illinois", "Indiana", "Ohio"])
    weather_station = WeatherStation(station_id=station_id,
                                     station_name=station_name,
                                     location=location)
    session.add(weather_station)


def insert_record(f,station_id,record_id,data_loaded=None):
    row_number = 1
    for line in f:
        data = line.strip().split('\t')
        try:
            record_id = record_id + 1
            date = datetime.strptime(data[0], "%Y%m%d").date()
            if data_loaded:
                if station_id in data_loaded.keys():
                    if date in data_loaded[station_id]:
                        print('Skipping')
                        continue
            max_temp = float(data[1]) if data[1] != '-9999' else None
            min_temp = float(data[2]) if data[2] != '-9999' else None
            precipitation = float(data[3]) if data[3] != '-9999' else None
            weather_data = WeatherDataRecords(
                record_id=record_id,
                record_date=date,
                station_id=int(station_id),
                max_temp=max_temp,
                min_temp=min_temp,
                precipitation=precipitation
            )
            session.add(weather_data)
            row_number = row_number + 1
            if row_number % 100 == 0:
                # session.commit()
                session.flush()
                session.commit()
            # print(session)
        except ValueError:
            # Handle invalid data records (optional)
            pass
    print(row_number)


def get_record_id(table_obj,column_obj):
    # return session.query(WeatherDataRecords).order_by(WeatherDataRecords.record_date.desc()).first()
    return session.query(table_obj).order_by(column_obj).first()

def calculate_stats():
    query = session.query(WeatherDataRecords)
    stats = query.with_entities(
        WeatherDataRecords.station_id,
        # WeatherStation.station_name,
        # WeatherStation.location,
        extract('year',WeatherDataRecords.record_date).label('year'),
        func.avg(WeatherDataRecords.max_temp).label('avg_max_temp'),
        func.avg(WeatherDataRecords.min_temp).label('avg_min_temp'),
        (func.sum(WeatherDataRecords.precipitation)/10).label('total_precipitation')
    ).group_by(WeatherDataRecords.station_id,extract('year',WeatherDataRecords.record_date)).order_by('year').all()
    stats_id  =  1 if not get_record_id(YearlyStats,YearlyStats.stats_id.desc()) else get_record_id(YearlyStats,YearlyStats.stats_id.desc()).stats_id
    print(stats_id,stats)
    for stat in stats:
        yearly_stats = YearlyStats(stats_id=stats_id,
                    station_id=stat[0],
                    year=stat[1],
                    avg_max_temp=stat[2],
                    avg_min_temp=stat[3],
                    total_precipitation=stat[4])

        session.add(yearly_stats)
        stats_id = stats_id + 1
    # print(session.query(YearlyStats).all())
    session.commit()

def check_station_id(station_id):
    check_result = session.query(WeatherStation).filter_by(station_id=station_id).first()
    return True if check_result else False

def get_loaded_station_data():
    loaded_data = session.query(WeatherStation).all()
    return list(set(d.station_id for d in loaded_data))
    # return True if check_result else False

def get_loaded_data():
    loaded_data = {}
    records = session.query(WeatherDataRecords).all()
    for rec in records:
        if rec.station_id not in loaded_data:
            loaded_data[rec.station_id] = []
        loaded_data[rec.station_id].append(rec.record_date)
    return loaded_data

def read_files():
    data_dir = os.path.join('C:\\', 'Users', 'Lenovo', 'Documents', 'GitHub', "code-challenge-template", 'test_data')
    start_time= datetime.now()
    for filename in list(os.listdir(data_dir)):
        filepath = os.path.join(data_dir, filename)
        station_id = int(filename[5:-4])
        data_loaded = get_loaded_data()
        stations_loaded = get_loaded_station_data()
        # print(stations_loaded)
        # if not check_station_id(station_id):
        # print(int(station_id) not in stations_loaded)
        if station_id not in stations_loaded:
            insert_station(station_id=station_id,
                           station_name=filename[:-4],
                        )
            session.commit()
        with open(filepath, "r") as f:
            # print(get_record_id().__dict__)
            record_id = 1 if not get_record_id(WeatherDataRecords,WeatherDataRecords.record_id.desc())  else get_record_id(WeatherDataRecords,WeatherDataRecords.record_id.desc()).record_id
            insert_record(f,station_id,record_id,data_loaded=data_loaded)
        session.commit()
        print(F"Completed ingesting {filename}")
    end_time = datetime.now()
    print(f"Start time: {start_time}, End time: {end_time}")
    print(f"Total time: {end_time - start_time}")
# calculate_stats()

# print(get_loaded_data())
# print(get_loaded_station_data())
# print(check_station_id(1044))
# print(check_station_id(110072))
read_files()