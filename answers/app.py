from flask import Flask, jsonify, request, render_template_string, render_template

from wheel.db_connection import WeatherDataRecords, WeatherStation, session, YearlyStats
from Ingestion import calculate_stats
from datetime import datetime

app = Flask(__name__,template_folder='.')

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/aws")
def aws():
    return render_template("aws.html")


@app.route("/api/weather/", methods=["GET"])
def get_weather_data():
    """
     Accepts 3 Args:
    - date_from : the start of the date from which we need data
    - date_to : the end of the date from which we need data
    - station_id : the station id for which data is needed
    :return: JSON-ified data from Weather Records table
    """
    query = session.query(WeatherDataRecords).join(WeatherStation)

    # Apply filters based on query parameters
    date_from = datetime.strptime(request.args.get("date_from"), "%Y-%m-%d").date()
    date_to = datetime.strptime(request.args.get("date_to"), "%Y-%m-%d").date()
    station_id = int(request.args.get("station_id"))
    print(date_from, date_to, station_id)

    if date_from:
        query = query.filter(WeatherDataRecords.record_date >= date_from)
    if date_to:
        query = query.filter(WeatherDataRecords.record_date <= date_to)
    if station_id:
        query = query.filter(WeatherDataRecords.station_id == station_id)

    # Paginate results
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 10))
    query = query.offset((page - 1) * per_page).limit(per_page)

    results = query.all()
    # print(query)
    # print(len(results))
    data = [
        {
            "station_id": station.station_id,
            "station_name": station.station_name,
            "station_location": station.location,
            "date": data.record_date.strftime("%Y-%m-%d"),
            "max_temp": data.max_temp,
            "min_temp": data.min_temp,
            "precipitation": data.precipitation,
        }
        for data in results
        for station in session.query(WeatherStation).filter_by(
            station_id=data.station_id
        )
    ]

    session.close()
    return jsonify(data)


@app.route("/api/weather/stats", methods=["GET"])
def get_weather_stats():
    """
    Accepts 3 Args:
    - year_from : the start of the year from which we need data
    - year_to : the end of the year from which we need data
    - station_id : the station id for which data is needed
    :return: JSON-ified data from Weather Stat table
    """

    # Apply filters based on query parameters
    year_from = int(request.args.get("year_from"))
    year_to = int(request.args.get("year_to"))
    station_id = int(request.args.get("station_id"))

    query = session.query(YearlyStats).join(WeatherStation)

    if year_from:
        query = query.filter(YearlyStats.year >= year_from)
    if year_to:
        query = query.filter(YearlyStats.year <= year_to)
    if station_id:
        query = query.filter(YearlyStats.station_id == station_id)

    # Calculate statistics
    stats = query.with_entities(
        WeatherStation.station_id,
        WeatherStation.station_name,
        WeatherStation.location,
        YearlyStats.year,
        YearlyStats.avg_max_temp,
        YearlyStats.avg_min_temp,
        YearlyStats.total_precipitation,
    ).all()
    stats_dict = [
        {
            "station_id": data.station_id,
            "station_name": data.station_name,
            "station_location": data.location,
            "year": data.year,
            "avg_max_temp": data.avg_max_temp,
            "avg_min_temp": data.avg_min_temp,
            "total_precipitation": data.total_precipitation,
        }
        for data in stats
    ]
    session.close()
    return jsonify(stats_dict)


@app.route("/api/weather/stats_now", methods=["GET"])
def put_weather_stats():
    """
    Calls calculate_stats() to update Weather stats table
    :return:
    """

    # Apply filters based on query parameters
    calculate_stats()
    return render_template_string("Stats updated.!!")



if __name__ == "__main__":
    app.run(debug=True)
