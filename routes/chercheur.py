from itertools import combinations
from flask import Blueprint, Flask, current_app, jsonify
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
from requests_cache import logger
from influxclient import client
from sql_server import engine
from sqlalchemy import text 
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from scipy.stats import linregress
from models.model import db,MethodeAnalyse
from auth_middleware import token_required, chercheur_required , get_current_user , get_userId
import logging
from collections import defaultdict
import json


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
@chercheur_routes.route('/chercheur/last_24_avg/<bucket>/<ville>/<param>', methods=['GET'])
@token_required
def get_day_avg(bucket, ville, param):
    # Heure UTC actuelle
    maintenant = datetime.now()

    # Heure précédente complète 
    heure_cible_debut = maintenant.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    heure_cible_fin = heure_cible_debut.replace(minute=59, second=59)

    # Format RFC3339 pour InfluxDB
    start_time = heure_cible_debut.isoformat() + "Z"
    stop_time = heure_cible_fin.isoformat() + "Z"
    message =""
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: -24h,stop:{stop_time})
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
            hour_key = record.get_time().strftime("%d-%H")
            hourly_data[hour_key].append(record.get_value())

    # Moyenne par heure
    last24_avg = []
    for hour, values in sorted(hourly_data.items()):
        moyenne = sum(values) / len(values)
        last24_avg.append({
            "date": hour,
            "valeur": round(moyenne,2) 
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
    



#Last 7 days avg 
@chercheur_routes.route('/chercheur/daily_avg_7/<bucket>/<ville>/<param>',methods=['GET'])
@token_required
def get_daily_avg(bucket,ville,param):
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: -7d , stop: -1d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r._field == "{param}")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> yield(name: "raw")
    '''

    result = client.query_api().query(org=org, query=query)

    # Dictionnaire pour stocker les valeurs par date
    daily_values = defaultdict(list)

    for table in result:
        for record in table.records:
            date_only = record.get_time().strftime("%Y-%m-%d")  # Tronque l'heure
            daily_values[date_only].append(record.get_value())

    # Calcul de la moyenne par date
    last7d_avg = []
    for date, values in daily_values.items():
        moyenne = sum(values) / len(values)
        last7d_avg.append({
            "date": date,
            "moyenne": round(moyenne,2) 
        })

    # Tri par date ascendante
    last7d_avg.sort(key=lambda x: x["date"])

    # Réponse finale
    if last7d_avg:
        return jsonify({
            "last7d_avg": last7d_avg,
            "message": "success"
        }), 200
    else:
        return jsonify({
            "message": "empty"
        }), 200


#Last Thirty days avg 
@chercheur_routes.route('/chercheur/daily_avg_30/<bucket>/<ville>/<param>',methods=['GET'])
@token_required
def get_monthly_avg(bucket,ville,param):
    query = f'''
        from(bucket: "{bucket}")
        |> range(start: -30d,stop: -1d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r._field == "{param}")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> yield(name: "raw")
    '''
    

    result = client.query_api().query(org=org, query=query)

    from collections import defaultdict

    # Dictionnaire pour stocker les valeurs par date
    daily_values = defaultdict(list)

    for table in result:
        for record in table.records:
            date_only = record.get_time().strftime("%Y-%m-%d")  # Tronque l'heure
            daily_values[date_only].append(record.get_value())

    # Calcul de la moyenne par date
    monthly_avg = []
    for date, values in daily_values.items():
        moyenne = sum(values) / len(values)
        monthly_avg.append({
            "date": date,
            "moyenne": round(moyenne,2) 
        })

    # Tri par date ascendante
    monthly_avg.sort(key=lambda x: x["date"])

    # Réponse finale
    if monthly_avg:
        return jsonify({
            "monthly_avg": monthly_avg,
            "message": "success"
        }), 200
    else:
        return jsonify({
            "message": "empty"
        }), 200
    





#Get last hour data
@chercheur_routes.route('/chercheur/last_hour_data/<bucket>/<ville>', methods=['GET'])
@token_required
def get_data_source(bucket, ville):
    # Heure UTC actuelle
    maintenant = datetime.now()

    # Heure précédente complète 
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
                "date": record.get_time().strftime("%Y-%m-%d %H:%M"),
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
@token_required
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
                "date": record.get_time().strftime("%Y-%m-%d %H:%M"),
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


# Last Seven days avg for weather API (moyenne calculée côté Python)
@chercheur_routes.route('/chercheur/last7weather/<ville>/<param>', methods=['GET'])
@token_required
def get_last7weather(ville, param):
    query = f'''
        from(bucket: "{bucket_weather}")
        |> range(start: -7d, stop:-1d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r._field == "{param}")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> yield(name: "raw")
    '''

    result = client.query_api().query(org=org, query=query)

    # Dictionnaire pour stocker les valeurs par date
    daily_values = defaultdict(list)

    for table in result:
        for record in table.records:
            date_only = record.get_time().strftime("%Y-%m-%d")  
            daily_values[date_only].append(record.get_value())

    # Calcul de la moyenne par date
    last7_weather = []
    for date, values in daily_values.items():
        moyenne = sum(values) / len(values)
        last7_weather.append({
            "date": date,
            "moyenne": round(moyenne)
        })

    # Tri par date ascendante
    last7_weather.sort(key=lambda x: x["date"])

    # Réponse finale
    if last7_weather:
        return jsonify({
            "last7_weather": last7_weather,
            "message": "success"
        }), 200
    else:
        return jsonify({
            "message": "empty"
        }), 200


#Last Seven days avg for open meteo
@chercheur_routes.route('/chercheur/last7meteo/<ville>/<param>',methods=['GET'])
@token_required
def get_last7meteo(ville,param):
    query = f'''
        from(bucket: "{bucket_open}")
        |> range(start: -7d , stop: -1d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r._field == "{param}")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> yield(name: "raw")
    '''

    result = client.query_api().query(org=org, query=query)

    from collections import defaultdict

    # Dictionnaire pour stocker les valeurs par date
    daily_values = defaultdict(list)

    for table in result:
        for record in table.records:
            date_only = record.get_time().strftime("%Y-%m-%d")  
            daily_values[date_only].append(record.get_value())

    # Calcul de la moyenne par date
    last7_meteo = []
    for date, values in daily_values.items():
        moyenne = sum(values) / len(values)
        last7_meteo.append({
            "date": date,
            "moyenne": round(moyenne)
        })

    # Tri par date ascendante
    last7_meteo.sort(key=lambda x: x["date"])
    # Réponse finale
    if last7_meteo:
        return jsonify({
            "last7_meteo": last7_meteo,
            "message": "success"
        }), 200
    else:
        return jsonify({
            "message": "empty"
        }), 200
    


#Last Seven days avg for open weather
@chercheur_routes.route('/chercheur/last7open/<ville>/<param>',methods=['GET'])
@token_required
def get_last7open(ville,param):
    query = f'''
        from(bucket: "{bucket_openweather}")
        |> range(start: -7d , stop: -1d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r._field == "{param}")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> yield(name: "raw")
    '''

    result = client.query_api().query(org=org, query=query)

    from collections import defaultdict

    # Dictionnaire pour stocker les valeurs par date
    daily_values = defaultdict(list)

    for table in result:
        for record in table.records:
            date_only = record.get_time().strftime("%Y-%m-%d")  
            daily_values[date_only].append(record.get_value())

    # Calcul de la moyenne par date
    last7_open = []
    for date, values in daily_values.items():
        moyenne = sum(values) / len(values)
        last7_open.append({
            "date": date,
            "moyenne": round(moyenne)
        })

    # Tri par date ascendante
    last7_open.sort(key=lambda x: x["date"])

    # Réponse finale
    if last7_open:
        return jsonify({
            "last7_open": last7_open,
            "message": "success"
        }), 200
    else:
        return jsonify({
            "message": "empty"
        }), 200



# Descriptive Analysis
@chercheur_routes.route('/chercheur/descriptive/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_descriptive_analysis(villes, source, params, period):
    try:
        r = current_app.config['redis_client']
        cache_key = f"descriptive_{villes}_{source}_{params}_{period}"

        if (cached := r.get(cache_key)):
            return jsonify(json.loads(cached)), 200

        if period.endswith('d'):
            days = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7d ou 30d'}), 400

        ville_list = villes.split(',')
        param_list = params.split(',')
        if not ville_list or not param_list:
            return jsonify({'error': 'Au moins une ville et un paramètre sont requis'}), 400

        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        start_str = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        results = {}

        for ville in ville_list:
            results[ville] = {}
            for param in param_list:
                query = f'''
                    from(bucket: "{source}")
                    |> range(start: {start_str}, stop: {end_str})
                    |> filter(fn: (r) => r._measurement == "meteo")
                    |> filter(fn: (r) => r._field == "{param}")
                    |> filter(fn: (r) => r.ville == "{ville}")
                    |> yield(name: "raw")
                '''
                result = client.query_api().query(org=org, query=query)
                daily_values = defaultdict(list)
                for table in result:
                    for record in table.records:
                        date_only = record.get_time().strftime("%Y-%m-%d")
                        daily_values[date_only].append(record.get_value())

                data = []
                for date, values in daily_values.items():
                    moyenne = sum(values) / len(values)
                    data.append({"date": date, "value": round(moyenne)})

                if not data:
                    results[ville][param] = {'message': f'Aucune donnée pour le paramètre {param}'}
                    continue

                df = pd.DataFrame(data, columns=['value'])
                description = df.describe().to_dict()
                results[ville][param] = description['value']

        response = {
            'villes': ville_list,
            'source': source,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'statistics': results,
            'message': 'success'
        }

        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': f'Erreur lors de l\'analyse : {str(e)}'}), 500


# Tendances Analysis
@chercheur_routes.route('/chercheur/tendances/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_trend_analysis(villes, source, params, period):
    try:
        r = current_app.config['redis_client']
        cache_key = f"tendance_{villes}_{source}_{params}_{period}"

        if (cached := r.get(cache_key)):
            return jsonify(json.loads(cached)), 200

        if period.endswith('d'):
            days = int(period[:-1])
        else:
            return jsonify({'error': 'Format invalide'}), 400

        ville_list = villes.split(',')
        param_list = params.split(',')
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        start_str = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        results = {}

        for ville in ville_list:
            results[ville] = {}
            for param in param_list:
                query = f'''
                    from(bucket: "{source}")
                    |> range(start: {start_str}, stop: {end_str})
                    |> filter(fn: (r) => r._measurement == "meteo")
                    |> filter(fn: (r) => r._field == "{param}")
                    |> filter(fn: (r) => r.ville == "{ville}")
                    |> yield(name: "raw")
                '''
                result = client.query_api().query(org=org, query=query)
                daily_values = defaultdict(list)
                for table in result:
                    for record in table.records:
                        date_only = record.get_time().strftime("%Y-%m-%d")
                        daily_values[date_only].append(record.get_value())

                data = [{"time": d, "value": round(sum(v)/len(v))} for d, v in daily_values.items()]
                if not data:
                    results[ville][param] = {'message': f"Aucune donnée pour {param}"}
                    continue

                df = pd.DataFrame(data)
                df['time'] = pd.to_datetime(df['time'])
                df['time_ordinal'] = df['time'].apply(lambda x: x.timestamp())
                slope, intercept, r_value, p_value, std_err = linregress(df['time_ordinal'], df['value'])
                total_increase = slope * (df['time'].iloc[-1] - df['time'].iloc[0]).total_seconds()
                trend_dir = "croissante" if slope > 0 else "décroissante" if slope < 0 else "stable"

                results[ville][param] = {
                    'slope': slope, 'r_value': r_value, 'p_value': p_value,
                    'std_err': std_err, 'trend_direction': trend_dir,
                    'total_increase': round(total_increase, 3),
                    'data_points': len(df)

                }

        response = {
            'villes': ville_list,
            'source': source,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'trends': results,
            'message': 'success'
        }

        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Correlation Analysis
@chercheur_routes.route('/chercheur/correlation/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_correlation_analysis(villes, source, params, period):
    try:
        r = current_app.config['redis_client']
        cache_key = f"correlation_{villes}_{source}_{params}_{period}"

        if (cached := r.get(cache_key)):
            return jsonify(json.loads(cached)), 200

        if not period.endswith('d'):
            return jsonify({'error': 'Format de period invalide'}), 400
        days = int(period[:-1])
        ville_list = villes.split(',')
        param_list = params.split(',')
        if len(param_list) < 2:
            return jsonify({'error': 'Deux paramètres minimum requis'}), 400

        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        start_str = start.isoformat() + "Z"
        end_str = end.isoformat() + "Z"
        results = {}

        for ville in ville_list:
            field_filters = " or ".join([f'r._field == "{param}"' for param in param_list])
            query = f'''
                from(bucket: "{source}")
                |> range(start: {start_str}, stop: {end_str})
                |> filter(fn: (r) => r._measurement == "meteo")
                |> filter(fn: (r) => ({field_filters}))
                |> filter(fn: (r) => r.ville == "{ville}")
                |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> yield(name: "raw")
            '''
            result = client.query_api().query(org=org, query=query)
            data = [{**r.values} for table in result for r in table.records]
            df = pd.DataFrame(data)

            if df.empty or df[param_list].dropna().empty:
                results[ville] = {'message': 'Aucune donnée disponible'}
                continue

            corr = df[param_list].corr().to_dict()
            results[ville] = {'correlation_matrix': corr, 'data_points': len(df)}

        response = {
            'villes': ville_list,
            'source': source,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'correlations': results,
            'message': 'success'
        }

        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Comparative Analysis
@chercheur_routes.route('/chercheur/comparative/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_direct_comparaison(villes, source, params, period):
    try:
        r = current_app.config['redis_client']
        cache_key = f"comparative_{villes}_{source}_{params}_{period}"

        if (cached := r.get(cache_key)):
            return jsonify(json.loads(cached)), 200

        if not period.endswith('d'):
            return jsonify({'error': 'Format de period invalide'}), 400
        days = int(period[:-1])
        ville_list = villes.split(',')
        param_list = params.split(',')
        if len(ville_list) < 2:
            return jsonify({'error': 'Au moins deux villes requises'}), 400

        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=days - 1)
        start_str = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_str = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        averages, differences = {}, {}

        for ville in ville_list:
            averages[ville] = {}
            for param in param_list:
                query = f'''
                    from(bucket: "{source}")
                    |> range(start: {start_str}, stop: {end_str})
                    |> filter(fn: (r) => r._measurement == "meteo")
                    |> filter(fn: (r) => r._field == "{param}")
                    |> filter(fn: (r) => r.ville == "{ville}")
                    |> yield(name: "raw")
                '''
                result = client.query_api().query(org=org, query=query)
                daily_values = defaultdict(list)
                for table in result:
                    for record in table.records:
                        date_only = record.get_time().strftime("%Y-%m-%d")
                        daily_values[date_only].append(record.get_value())

                data = [{"value": sum(v)/len(v)} for v in daily_values.values()]
                if not data:
                    averages[ville][param] = {'message': f'Aucune donnée pour {param}'}
                    continue

                df = pd.DataFrame(data)
                averages[ville][param] = round(df['value'].mean(), 2)

        for param in param_list:
            differences[param] = {}
            for v1, v2 in combinations(ville_list, 2):
                if isinstance(averages[v1].get(param), (int, float)) and isinstance(averages[v2].get(param), (int, float)):
                    diff = averages[v1][param] - averages[v2][param]
                    differences[param][f"{v1} - {v2}"] = round(diff, 3)

        response = {
            'villes': ville_list,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'averages': averages,
            'differences': differences,
            'message': 'success'
        }

        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Descriptive Analysis
@chercheur_routes.route('/chercheur/descriptive_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_descriptive_sql_analysis(villes, params, period):
    r = current_app.config['redis_client']
    cache_key = f"descriptive_sqlserver_{villes}_{params}_{period}"

    if (cached := r.get(cache_key)):
        return jsonify(json.loads(cached)), 200

    try:
        if not period.endswith('y'):
            return jsonify({'error': 'Format de period invalide (ex : 7y)'}), 400
        year = int(period[:-1])
        year_start, year_end = 2025 - year, 2024

        ville_list, param_list = villes.split(','), params.split(',')
        results = {}

        with engine.connect() as connection:
            for ville in ville_list:
                results[ville] = {}
                for param in param_list:
                    query = text(f"""
                        SELECT z.Ville, f.{param}, d.Annee, d.Mois
                        FROM [climate_data_integration].[dbo].[FaitClimat] AS f
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] AS z ON f.Zone_Id = z.Id
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Date] AS d ON f.Date_Id = d.Id
                        WHERE z.Ville = '{ville}' AND d.Annee BETWEEN {year_start} AND {year_end}
                        ORDER BY d.Id
                    """)
                    result = connection.execute(query).mappings().fetchall()
                    values = [row[param] for row in result if row[param] is not None]

                    if not values:
                        results[ville][param] = {'message': f"Aucune donnée pour {param}"}
                        continue

                    df = pd.DataFrame(values, columns=['value'])
                    results[ville][param] = df.describe().to_dict()['value']

        response = {
            'villes': ville_list,
            'params': param_list,
            'period': period,
            'statistics': results,
            'message': 'success'
        }
        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Tendance Analysis
@chercheur_routes.route('/chercheur/tendances_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_trend_sql_analysis(villes, params, period):
    r = current_app.config['redis_client']
    cache_key = f"tendance_sqlserver_{villes}_{params}_{period}"

    if (cached := r.get(cache_key)):
        return jsonify(json.loads(cached)), 200

    try:
        if not period.endswith('y'):
            return jsonify({'error': 'Format de period invalide (ex : 7y)'}), 400
        year = int(period[:-1])
        year_start, year_end = 2025 - year, 2024

        ville_list, param_list = villes.split(','), params.split(',')
        results = {}

        with engine.connect() as connection:
            for ville in ville_list:
                results[ville] = {}
                for param in param_list:
                    query = text(f"""
                        SELECT f.{param} AS value, d.Annee, d.Mois
                        FROM [climate_data_integration].[dbo].[FaitClimat] f
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] z ON f.Zone_Id = z.Id
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Date] d ON f.Date_Id = d.Id
                        WHERE z.Ville = '{ville}' AND d.Annee BETWEEN {year_start} AND {year_end}
                        ORDER BY d.Id
                    """)
                    result = connection.execute(query).mappings().fetchall()

                    data = [{'time': datetime(r['Annee'], r['Mois'], 1), 'value': r['value']} for r in result if r['value'] is not None]
                    if not data:
                        results[ville][param] = {'message': f"Aucune donnée pour {param}"}
                        continue

                    df = pd.DataFrame(data)
                    df['time_ordinal'] = df['time'].apply(lambda x: x.timestamp())
                    slope, intercept, r_value, p_value, std_err = linregress(df['time_ordinal'], df['value'])
                    total_increase = slope * ((year_end - year_start + 1) * 365 * 24 * 60 * 60)
                    direction = "croissante" if slope > 0 else "décroissante" if slope < 0 else "stable"

                    results[ville][param] = {
                        'slope': slope,
                        'r_value': r_value,
                        'p_value': p_value,
                        'trend_direction': direction,
                        'total_increase': round(total_increase, 3),
                        'data_points': len(df)
                    }

        response = {
            'villes': ville_list,
            'params': param_list,
            'period': period,
            'trends': results,
            'message': 'success'
        }
        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500



# Correlation Analysis
@chercheur_routes.route('/chercheur/correlation_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_correlation_sql_analysis(villes, params, period):
    r = current_app.config['redis_client']
    cache_key = f"correlation_sqlserver_{villes}_{params}_{period}"

    if (cached := r.get(cache_key)):
        return jsonify(json.loads(cached)), 200

    try:
        if not period.endswith('y'):
            return jsonify({'error': 'Format de period invalide (ex : 7y)'}), 400
        year = int(period[:-1])
        year_start, year_end = 2025 - year, 2024

        ville_list, param_list = villes.split(','), params.split(',')
        results = {}

        with engine.connect() as connection:
            for ville in ville_list:
                cols = ', '.join([f"f.[{p}]" for p in param_list])
                query = text(f"""
                    SELECT {cols}, d.Annee, d.Mois
                    FROM [climate_data_integration].[dbo].[FaitClimat] f
                    INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] z ON f.Zone_Id = z.Id
                    INNER JOIN [climate_data_integration].[dbo].[Dim_Date] d ON f.Date_Id = d.Id
                    WHERE z.Ville = '{ville}' AND d.Annee BETWEEN {year_start} AND {year_end}
                """)
                result = connection.execute(query).mappings().fetchall()
                data = [{p: r[p] for p in param_list if r[p] is not None} for r in result]

                df = pd.DataFrame(data).dropna()
                if df.empty:
                    results[ville] = {'message': f"Aucune donnée alignée pour {params}"}
                    continue

                results[ville] = {
                    'correlation_matrix': df.corr().to_dict(),
                    'data_points': len(df)
                }

        response = {
            'villes': ville_list,
            'params': param_list,
            'period': period,
            'correlations': results,
            'message': 'success'
        }
        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        logger.error(f"Erreur : {str(e)}")
        return jsonify({'error': str(e)}), 500


# Comparative Analysis
@chercheur_routes.route('/chercheur/comparative_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_comparaison_sql_server(villes, params, period):
    r = current_app.config['redis_client']
    cache_key = f"comparative_sqlserver_{villes}_{params}_{period}"

    if (cached := r.get(cache_key)):
        return jsonify(json.loads(cached)), 200

    try:
        if not period.endswith('y'):
            return jsonify({'error': 'Format de period invalide (ex : 7y)'}), 400
        year = int(period[:-1])
        year_start, year_end = 2025 - year, 2024

        ville_list, param_list = villes.split(','), params.split(',')
        averages, differences = {}, {}

        with engine.connect() as connection:
            for ville in ville_list:
                averages[ville] = {}
                for param in param_list:
                    query = text(f"""
                        SELECT f.{param} AS value
                        FROM [climate_data_integration].[dbo].[FaitClimat] f
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] z ON f.Zone_Id = z.Id
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Date] d ON f.Date_Id = d.Id
                        WHERE z.Ville = :ville AND d.Annee BETWEEN :start AND :end
                    """)
                    result = connection.execute(query, {"ville": ville, "start": year_start, "end": year_end}).mappings().fetchall()
                    values = [r['value'] for r in result if r['value'] is not None]
                    if not values:
                        averages[ville][param] = {'message': f"Aucune donnée pour {param}"}
                        continue
                    df = pd.DataFrame(values, columns=['value'])
                    averages[ville][param] = round(df['value'].mean(), 3)

        for param in param_list:
            differences[param] = {}
            for v1, v2 in combinations(ville_list, 2):
                if isinstance(averages[v1].get(param), (int, float)) and isinstance(averages[v2].get(param), (int, float)):
                    diff = averages[v1][param] - averages[v2][param]
                    differences[param][f"{v1} - {v2}"] = round(diff, 3)

        response = {
            'villes': ville_list,
            'params': param_list,
            'period': f'{year_start}-{year_end}',
            'averages': averages,
            'differences': differences,
            'message': 'success'
        }
        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


#Prevision 
@chercheur_routes.route('/chercheur/prevision/<ville>/<n_days>', methods=['GET'])
@token_required
def get_mlast_avg(ville, n_days):
    try:
        model = current_app.config['model']
        scalers_ville = current_app.config['scalers_ville']
        r = current_app.config['redis_client']
        
        if ville not in scalers_ville:
            return jsonify({"error": f"Ville inconnue : {ville}"}), 400

        cache_key = f"prevision_{ville}_{n_days}"
        if (cached := r.get(cache_key)):
            return jsonify(json.loads(cached)), 200

        query = f'''
            from(bucket: "climate_data_openweather")
            |> range(start: -30d , stop:-1d)
            |> filter(fn: (r) => r._measurement == "meteo")
            |> filter(fn: (r) => r._field == "temperature")
            |> filter(fn: (r) => r.ville == "{ville}")
            |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)
            |> yield(name: "daily_avg")
        '''
        
        result = client.query_api().query(org=org, query=query)
        daily_values = []
        for table in result:
            for record in table.records:
                daily_values.append({
                    "date": record.get_time().strftime("%Y-%m-%d"),
                    "moyenne": round(record.get_value(), 2)
                })
        
        if len(daily_values) < 1:
            return jsonify({"error": "Pas assez de données pour la prédiction"}), 400

        df = pd.DataFrame(daily_values)
        df["date"] = pd.to_datetime(df["date"])
        df.sort_values("date", inplace=True)

        def predict_prochain_jours(last_days, model, scaler, n_days):
            last_days_scaled = scaler.transform(last_days.reshape(-1, 1))
            input_seq = last_days_scaled.reshape(1, last_days_scaled.shape[0], 1)
            preds = []
            for _ in range(int(n_days)):
                try:
                    pred = model(input_seq, training=False).numpy()[0][0]
                except AttributeError:
                    pred = model.predict(input_seq, verbose=0)[0][0]
                preds.append(pred)
                input_seq = np.concatenate([input_seq[:, 1:, :], np.array(pred).reshape(1,1,1)], axis=1)

            preds = np.array(preds).reshape(-1, 1)
            return scaler.inverse_transform(preds).flatten()

        derniers_jours = df["moyenne"].values[-30:]
        scaler = scalers_ville[ville]
        predictions = predict_prochain_jours(derniers_jours, model, scaler, n_days)

        derniere_date = df["date"].iloc[-1]
        dates = [derniere_date + timedelta(days=i) for i in range(1, int(n_days)+1)]

        response = {
            "ville": ville,
            "predictions": [round(x, 2) for x in predictions.tolist()],
            "dates": [d.strftime("%Y-%m-%d") for d in dates],
            "n_jours": n_days,
            "message": "success"
        }

        r.setex(cache_key, 3600, json.dumps(response))
        return jsonify(response), 200

    except KeyError as e:
        return jsonify({"error": f"Clé manquante : {e}"}), 500
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
