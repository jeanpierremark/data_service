from flask import Blueprint, Flask, jsonify
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxclient import client
from sql_server import engine
from sqlalchemy import text 
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta


from auth_middleware import token_required, chercheur_required , get_current_user


load_dotenv()

bucket_weather = "climate_data_weather"
bucket_visual = "climate_data_visual"
bucket_open = "climate_data_openmeteo"
bucket_openweather = "climate_data_openweather"
org = os.getenv("org")

chercheur_routes = Blueprint('chercheur_routes', __name__)


#Test
@chercheur_routes.route('/chercheur/test-auth', methods=['GET'])
@token_required
def test_chercheur_auth():
    """Route de test pour vérifier l'authentification chercheur"""
    user = get_current_user()
    return jsonify({
        'success': True,
        'message': 'Authentification chercheur réussie',
        'user_id': user['id'],
        'role': user['role'],
    }), 200

#Get all parameters from weather API
@chercheur_routes.route('/chercheur/meteo_weather/<ville>', methods=['GET'])
@token_required
@chercheur_required
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


#Get all parameters from Open Weather 
@chercheur_routes.route('/chercheur/meteo_openweather/<ville>', methods=['GET'])
@token_required
@chercheur_required
def get_meteo_data_openweather(ville):
    query = f'''
    from(bucket: "{bucket_openweather}")
      |> range(start: -1h)
      |> filter(fn: (r) => r._measurement == "meteo" and r.ville == "{ville}")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
      |> keep(columns: ["_time","ville", "temperature","humidite", "precipitation","pression","vitesse_vent","chance_pluie","condition","icon","nebulosite"])
    '''
    result = client.query_api().query(org=org, query=query)

    data = []
    for table in result:
        for record in table.records:
            data.append({
                "ville":record["ville"],
                "time": record.get_time().isoformat(),
                "temperature": record["temperature"],
                "humidite": record["humidite"],
                "precipitation": record["precipitation"],
                "pression": record["pression"],
                "vitesse_vent": record["vitesse_vent"],
                "chance_pluie": record["chance_pluie"],
                "condition": record["condition"],
                "icon": record["icon"],
                "nebulosite": record["nebulosite"]
            })
            
    return jsonify({
        "message": "Success",
        "data_openweather":data
        }),200



#Get all parameters from Open Meteo
@chercheur_routes.route('/chercheur/meteo_open/<ville>', methods=['GET'])
@token_required
@chercheur_required
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

#Données historique
@chercheur_routes.route('/chercheur/historique', methods=['GET'])
def get_historique_data():
    result = engine.connect().execute(text("SELECT TOP(100) * FROM FaitClimat"))
    rows = result.fetchall()
    columns = result.keys()
    data = [dict(zip(columns, row)) for row in rows]
    return jsonify({"data":data}),200




#Last 24h  avg 
from collections import defaultdict
from flask import jsonify

@chercheur_routes.route('/chercheur/last_24_avg/<bucket>/<ville>/<param>', methods=['GET'])
def get_day_avg(bucket, ville, param):
    message =""
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: -24h)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r)=> r._field=="{param}")
        |> filter(fn: (r)=> r.ville == "{ville}")
        |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
    '''

    result = client.query_api().query(org=org, query=query)

    # Grouper par heure et faire la moyenne
    hourly_data = defaultdict(list)
    for table in result:
        for record in table.records:
            hour_key = record.get_time().strftime("%Y-%m-%d %H")
            hourly_data[hour_key].append(record.get_value())

    # Moyenne par heure
    last24_avg = []
    for hour, values in sorted(hourly_data.items()):
        moyenne = sum(values) / len(values)
        last24_avg.append({
            "date": hour,
            "valeur": round(moyenne)
        })
    if last24_avg : 
        message ="success" 
        return jsonify({ 
        "last24_avg": last24_avg,
        "message":message
        }), 200

    else :
        message="empty"
        return jsonify({ 
        "message":message
        }), 200
    



#Seven Daily avg 
@chercheur_routes.route('/chercheur/daily_avg_7/<bucket>/<ville>/<param>',methods=['GET'])
def get_daily_avg(bucket,ville,param):
    message=""
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: -7d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r)=> r._field=="{param}")
        |> filter(fn: (r)=> r.ville == "{ville}")
        |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
        |> yield(name: "mean")


    '''

    result = client.query_api().query(org=org, query=query)

    # Extraction des moyennes journalières
    daily7_avg = []
    for table in result:
        for record in table.records:
            date_str = record.get_time().strftime("%Y-%m-%d")
            daily7_avg.append({
                "date": date_str,
                "moyenne": round(record.get_value())
            })

    if daily7_avg : 
        message ="success" 
        return jsonify({ 
        "last7d_avg": daily7_avg,
        "message":message
        }), 200

    else :
        message="empty"
        return jsonify({ 
        "message":message
        }), 204

#Thirty day avg 
@chercheur_routes.route('/chercheur/daily_avg_30/<bucket>/<ville>/<param>',methods=['GET'])
def get_monthly_avg(bucket,ville,param):
    message=""
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: -30d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r)=> r._field=="{param}")
        |> filter(fn: (r)=> r.ville == "{ville}")
        |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
    '''

    result = client.query_api().query(org=org, query=query)

    # Extraction des moyennes mensuelles 
    monthly_avg = []
    for table in result:
        for record in table.records:
            date_str = record.get_time().strftime("%Y-%m-%d")
            monthly_avg.append({
                "date": date_str,
                "moyenne": round(record.get_value())
            })

    if monthly_avg : 
        message ="success" 
        return jsonify({ 
        "monthly_avg": monthly_avg,
        "message":message
        }), 200

    else :
        message="empty"
        return jsonify({ 
        "message":message
        }), 200





#Get last hour data
@chercheur_routes.route('/chercheur/last_hour_data/<bucket>/<ville>', methods=['GET'])
def get_data_source(bucket, ville):
    # Heure UTC actuelle
    maintenant = datetime.now()

    # Heure précédente complète (par ex. si maintenant = 00h15, alors cible = 23h00)
    heure_cible_debut = maintenant.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    heure_cible_fin = heure_cible_debut.replace(minute=59, second=59)

    # Format RFC3339 pour InfluxDB
    start_time = heure_cible_debut.isoformat() + "Z"
    stop_time = heure_cible_fin.isoformat() + "Z"
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: {start_time}, stop:{stop_time})
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> last()
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    result = client.query_api().query(org=org, query=query)

    # Récupérer les dernières valeurs
    latest_data = []
    for table in result:
        for record in table.records:
            data = record.values
            # On supprime les champs internes inutiles (_start, _stop, result, etc.)
            info = {
                "date": record.get_time().strftime("%Y-%m-%d %H"),
                "ville": data.get("ville"),
                "temperature": data.get("temperature"),
                "humidite": data.get("humidite"),
                "pression": data.get("pression"),
                "vitesse_vent" : data.get('vitesse_vent')
            }
            latest_data.append(info)

    if latest_data : 
        message ="success" 
        return jsonify({ 
        "latest_data": latest_data,
        "message":message
        }), 200

    else :
        message="empty"
        return jsonify({ 
        "message":message
        }), 200



#Get last hour data
@chercheur_routes.route('/chercheur/current_data/<bucket>/<ville>', methods=['GET'])
def get_current_data(bucket, ville):
   
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: -1h)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> last()
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    result = client.query_api().query(org=org, query=query)

    # Récupérer les dernières valeurs
    current_data = []
    for table in result:
        for record in table.records:
            data = record.values
            # On supprime les champs internes inutiles (_start, _stop, result, etc.)
            info = {
                "date": record.get_time().strftime("%Y-%m-%d %H"),
                "ville": data.get("ville"),
                "temperature": data.get("temperature"),
                "humidite": data.get("humidite"),
                "pression": data.get("pression"),
                "vitesse_vent" : data.get('vitesse_vent')
            }
            current_data.append(info)

    if current_data : 
        message ="success" 
        return jsonify({ 
        "current_data": current_data,
        "message":message
        }), 200

    else :
        message="empty"
        return jsonify({ 
        "message":message
        }), 200

