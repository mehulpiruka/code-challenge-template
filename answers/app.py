from flask import Flask, jsonify, request

from wheel.db_connection import WeatherDataRecords,WeatherStation,session, YearlyStats
from injestion import calculate_stats
import json

app = Flask(__name__)

@app.route('/api/weather/', methods=['GET'])
def get_weather_data():
    # session = Session()
    query = session.query(WeatherDataRecords).join(WeatherStation)

    # Apply filters based on query parameters
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    station_id = request.args.get('station_id')

    if date_from:
        query = query.filter(WeatherDataRecords.record_date >= date_from)
    if date_to:
        query = query.filter(WeatherDataRecords.record_date <= date_to)
    if station_id:
        query = query.filter(WeatherDataRecords.station_id == station_id)

    # Paginate results
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    query = query.offset((page - 1) * per_page).limit(per_page)

    results = query.all()
    data = [
        {
            "station_id": station.station_id,
            "station_name": station.station_name,
            "station_location": station.location,
            "date": data.record_date.strftime('%Y-%m-%d'),
            "max_temp": data.max_temp,
            "min_temp": data.min_temp,
            "precipitation": data.precipitation
        }
        for data in results
        for station in session.query(WeatherStation).filter_by(station_id=data.station_id)
    ]

    session.close()
    return jsonify(data)

@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    # session = Session()

    # Apply filters based on query parameters
    year_from = request.args.get('year_from')
    year_to = request.args.get('year_to')
    station_id = request.args.get('station_id')

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
        YearlyStats.total_precipitation
    ).all()
    stats_dict = [{
        "station_id": data.station_id,
        "station_name": data.station_name,
        "station_location": data.location,
        "year": data.year,
        "avg_max_temp": data.avg_max_temp,
        "avg_min_temp": data.avg_min_temp,
        "total_precipitation": data.total_precipitation
    } for data in stats ]
    session.close()
    return jsonify(stats_dict)

@app.route('/api/weather/stats_now', methods=['PUT'])
def put_weather_stats():
    # session = Session()

    # Apply filters based on query parameters
    calculate_stats()
#
# from flask_swagger_ui import get_swaggerui_blueprint
# # from swagger_spec import app as swagger_spec
#
# # Create the Swagger UI blueprint
# swaggerui_blueprint = get_swaggerui_blueprint(
#     "",
#     "http://127.0.0.1:5000/answers/swagger.json",
#     config={
#         'app_name': "Sample API"
#     }
# )
#
# # Register the Swagger UI blueprint with the app
# app.register_blueprint(swaggerui_blueprint)

if __name__ == '__main__':
    app.run(debug=True)