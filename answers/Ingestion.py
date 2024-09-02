import os
from datetime import datetime
from sqlalchemy.sql import func
from sqlalchemy import extract


from wheel.db_connection import (
    WeatherDataRecords,
    session,
    WeatherStation,
    YearlyStats,
    YieldStats,
)


def get_latest_record_id(table_obj, column_obj):
    """
    Currently used to get the last primary ids entered in the tables
    :param table_obj: Table from which we need to query
    :param column_obj: the column we want to query
    :return: the first row from the query
    """
    return session.query(table_obj).order_by(column_obj).first()


def insert_station(station_id, station_name):
    """
    The function to insert station id , name & location into WeatherStation table
    :param station_id: Station ID , should be unique
    :param station_name: Station Name
    :return:
    """
    ## Used Random here just to create data
    import random

    location = random.choice(["Nebraska", "Iowa", "Illinois", "Indiana", "Ohio"])
    weather_station = WeatherStation(
        station_id=station_id, station_name=station_name, location=location
    )
    session.add(weather_station)
    session.commit()


def insert_record(f, station_id, record_id, data_loaded=None):
    """

    :param f: File object
    :param station_id: Station ID
    :param record_id: the last Record ID that should be inserted
    :param data_loaded:
    :return:
    """
    row_number = 1
    for line in f:
        data = line.strip().split("\t")
        try:
            record_id = record_id + 1
            date = datetime.strptime(data[0], "%Y%m%d").date()
            if data_loaded:
                if station_id in data_loaded.keys():
                    if date in data_loaded[station_id]:
                        print("Skipping")
                        continue
            max_temp = float(data[1]) if data[1] != "-9999" else None
            min_temp = float(data[2]) if data[2] != "-9999" else None
            precipitation = float(data[3]) if data[3] != "-9999" else None
            weather_data = WeatherDataRecords(
                record_id=record_id,
                record_date=date,
                station_id=int(station_id),
                max_temp=max_temp,
                min_temp=min_temp,
                precipitation=precipitation,
            )
            session.add(weather_data)
            row_number = row_number + 1
            if row_number % 100 == 0:
                session.commit()
        except ValueError:
            # Handle invalid data records (optional)
            pass
    print(f"Total {row_number} rows inserted..!")


def check_station_id(station_id):
    """

    :param station_id: Station ID to be checked
    :return: Boolean : True if Station ID exists, else False
    """
    check_result = (
        session.query(WeatherStation).filter_by(station_id=station_id).first()
    )
    return True if check_result else False


def get_loaded_station_data():
    """
    Get the loaded station ids
    :return: List: Unique loaded station ids
    """
    loaded_data = session.query(WeatherStation).all()
    return list(set(d.station_id for d in loaded_data))
    # return True if check_result else False


def get_loaded_data():
    """
    Get the loaded dates in respective to station ids
    :return: Dict: Station id with list of the loaded date
                    {'station_id': list(record_date)}
    """
    loaded_data = {}
    records = session.query(WeatherDataRecords).all()
    for rec in records:
        if rec.station_id not in loaded_data:
            loaded_data[rec.station_id] = []
        loaded_data[rec.station_id].append(rec.record_date)
    return loaded_data


def calculate_stats():
    """
    Calculate following weather statistics for each year and station
    Average max temperature (°C)
    Average min temperature (°C)
    Total precipitation (cm)
    :return:
    """
    query = session.query(WeatherDataRecords)
    stats = (
        query.with_entities(
            WeatherDataRecords.station_id,
            extract("year", WeatherDataRecords.record_date).label("year"),
            func.avg(WeatherDataRecords.max_temp).label("avg_max_temp"),
            func.avg(WeatherDataRecords.min_temp).label("avg_min_temp"),
            (func.sum(WeatherDataRecords.precipitation) / 10).label(
                "total_precipitation"
            ),
        )
        .group_by(
            WeatherDataRecords.station_id,
            extract("year", WeatherDataRecords.record_date),
        )
        .order_by("year")
        .all()
    )
    stats_id = (
        1
        if not get_latest_record_id(YearlyStats, YearlyStats.stats_id.desc())
        else get_latest_record_id(YearlyStats, YearlyStats.stats_id.desc()).stats_id
    )
    # print(stats_id, stats)
    for stat in stats:
        yearly_stats = YearlyStats(
            stats_id=stats_id,
            station_id=stat[0],
            year=stat[1],
            avg_max_temp=stat[2],
            avg_min_temp=stat[3],
            total_precipitation=stat[4],
        )

        session.add(yearly_stats)
        stats_id = stats_id + 1
    session.commit()


def load_station_records(data_dir):
    """
    It reads & ingests all station records to the tables with any new station too
    :param data_dir: path object of dir with files
    :return:
    """
    start_time = datetime.now()
    try:
        for filename in list(os.listdir(data_dir)):
            filepath = os.path.join(data_dir, filename)
            station_id = int(
                filename[5:-4]
            )  # Assumaption each station is a part of file name
            data_loaded = get_loaded_data()
            stations_loaded = get_loaded_station_data()
            # print(stations_loaded)
            # if not check_station_id(station_id):
            # print(int(station_id) not in stations_loaded)
            if station_id not in stations_loaded:
                insert_station(
                    station_id=station_id,
                    station_name=filename[:-4],
                )
            with open(filepath, "r") as f:
                record_id = (
                    1
                    if not get_latest_record_id(
                        WeatherDataRecords, WeatherDataRecords.record_id.desc()
                    )
                    else get_latest_record_id(
                        WeatherDataRecords, WeatherDataRecords.record_id.desc()
                    ).record_id
                )
                insert_record(f, station_id, record_id, data_loaded=data_loaded)
            session.commit()
            print(f"Completed ingesting {filename}")

    except Exception as e:
        print(e)
    finally:
        end_time = datetime.now()
        print(f"Start time: {start_time}, End time: {end_time}")
        print(f"Total time: {end_time - start_time}")
        session.close()


def load_yield_data(data_dir):
    """
    It reads & ingests yearly yield data
    :param data_dir: the path of dir with files
    :return:
    """
    start_time = datetime.now()
    row_number = 0
    try:
        for filename in list(os.listdir(data_dir)):
            filepath = os.path.join(data_dir, filename)

            with open(filepath, "r") as f:
                yield_id = (
                    1
                    if not get_latest_record_id(YieldStats, YieldStats.yield_id.desc())
                    else get_latest_record_id(
                        YieldStats, YieldStats.yield_id.desc()
                    ).yield_id
                )
                for line in f:
                    data = line.strip().split("\t")
                    try:
                        yield_data = YieldStats(
                            yield_id=yield_id,
                            yield_year=int(data[0]),
                            yield_amt=int(data[1]),
                        )
                        session.add(yield_data)
                        session.commit()
                        row_number = row_number + 1
                    except ValueError:
                        pass
            print(f"Completed ingesting {filename}")

    except Exception as e:
        print(e)
    finally:
        print(f"Ingested  {row_number} rows.!!")
        end_time = datetime.now()
        print(f"Start time: {start_time}, End time: {end_time}")
        print(f"Total time: {end_time - start_time}")
        session.close()


# calculate_stats()

# print(get_loaded_data())
# print(get_loaded_station_data())
# print(check_station_id(1044))
# print(check_station_id(110072))
# data_dir_wx = os.path.join('C:\\', 'Users', 'Lenovo', 'Documents', 'GitHub', "code-challenge-template", 'wx_data')
# load_station_records(data_dir_wx)
# data_dir_yld = os.path.join('C:\\', 'Users', 'Lenovo', 'Documents', 'GitHub', "code-challenge-template", 'yld_data')
# load_yield_data(data_dir_yld)
