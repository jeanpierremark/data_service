from flask import Blueprint, Flask, jsonify
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxclient import client
from sql_server import engine
from sqlalchemy import text 
from dotenv import load_dotenv
import os

load_dotenv()

bucket_weather = "climate_data_weather"
bucket_visual = "climate_data_visual"
bucket_open = "climate_data_openmeteo"
org = os.getenv("org")

chercheur_routes = Blueprint('chercheur_routes', __name__)


@chercheur_routes.route('/chercheur/meteo_weather/<ville>', methods=['GET'])
def get_meteo_data_weather(ville):
    query = f'''
    from(bucket: "{bucket_weather}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "meteo" and r.ville == "{ville}")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
      |> keep(columns: ["_time","ville", "temperature","temperature_min","temperature_max", "humidite", "precipitation","pression","vitesse_vent","chance_pluie","condition","icon","uv_index","nebulosite"])
    '''
    result = client.query_api().query(org=org, query=query)

    data = []
    for table in result:
        for record in table.records:
            data.append({
                "ville":record["ville"],
                "time": record.get_time().isoformat(),
                "temperature": record["temperature"],
                "temperature_min": record["temperature_min"],
                "temperature_max": record["temperature_max"],
                "humidite": record["humidite"],
                "precipitation": record["precipitation"],
                "pression": record["pression"],
                "vitesse_vent": record["vitesse_vent"],
                "chance_pluie": record["chance_pluie"],
                "condition": record["condition"],
                "icon": record["icon"],
                "uv_index": record["uv_index"],
                "nebulosite": record["nebulosite"]
            })
            
    return jsonify({
        "message": "Success",
        "data_weather":data
        }),200



@chercheur_routes.route('/chercheur/meteo_visual/<ville>', methods=['GET'])
def get_meteo_data_visual(ville):
    query = f'''
    from(bucket: "{bucket_visual}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "meteo" and r.ville == "{ville}")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
      |> keep(columns: ["_time","ville", "temperature","temperature_min","temperature_max", "humidite", "precipitation","rayonnement_solaire","vitesse_vent","chance_pluie","condition","uv_index","nebulosite"])
    '''
    result = client.query_api().query(org=org, query=query)

    data = []
    for table in result:
        for record in table.records:
            data.append({
                "ville":record["ville"],
                "time": record.get_time().isoformat(),
                "temperature": record["temperature"],
                "temperature_min": record["temperature_min"],
                "temperature_max": record["temperature_max"],
                "humidite": record["humidite"],
                "precipitation": record["precipitation"],
                "rayonnement_solaire": record["rayonnement_solaire"],
                "vitesse_vent": record["vitesse_vent"],
                "chance_pluie": record["chance_pluie"],
                "condition": record["condition"],
                "uv_index": record["uv_index"],
                "nebulosite": record["nebulosite"]
            })
            
    return jsonify({
        "message": "Success",
        "data_visual":data
        }),200



@chercheur_routes.route('/chercheur/meteo_open/<ville>', methods=['GET'])
def get_meteo_data_open(ville):
    query = f'''
    from(bucket: "{bucket_open}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "meteo" and r.ville == "{ville}")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
      |> keep(columns: ["_time","ville", "temperature","temperature_min","temperature_max", "humidite", "precipitation","pression","vitesse_vent","chance_pluie","ensoleillement","rayonnement_solaire","uv_index","nebulosite"])
    '''
    result = client.query_api().query(org=org, query=query)

    data = []
    for table in result:
        for record in table.records:
            data.append({
                "ville":record["ville"],
                "time": record.get_time().isoformat(),
                "temperature": record["temperature"],
                "temperature_min": record["temperature_min"],
                "temperature_max": record["temperature_max"],
                "humidite": record["humidite"],
                "precipitation": record["precipitation"],
                "pression": record["pression"],
                "vitesse_vent": record["vitesse_vent"],
                "ensoleillement": record["ensoleillement"],
                "rayonnement_solaire": record["rayonnement_solaire"],
                "uv_index": record["uv_index"],
                "nebulosite": record["nebulosite"]
            })
            
    return jsonify({
        "message": "Success",
        "data_open":data
        }),200


@chercheur_routes.route('/chercheur/historique', methods=['GET'])
def get_historique_data():
    result = engine.connect().execute(text("SELECT TOP(100) * FROM FaitClimat"))
    rows = result.fetchall()
    columns = result.keys()
    data = [dict(zip(columns, row)) for row in rows]
    return jsonify({"data":data}),200
   