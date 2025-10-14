from itertools import combinations
from flask import Blueprint, Flask, jsonify
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import requests
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
import pickle
from collections import defaultdict


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

    from collections import defaultdict

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
    

#Descriptive Analysis
@chercheur_routes.route('/chercheur/descriptive/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_descriptive_analysis(villes, source, params, period):
    # Valider et extraire le nombre de jours depuis le paramètre period
    try:
        if period.endswith('d'):
            days = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7d ou 30d'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "d"'}), 400

    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if not ville_list or not param_list:
        return jsonify({'error': 'Au moins une ville et un paramètre sont requis'}), 400

    # Calculer la période
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=days-1) 

    # Formatter les dates en RFC3339 pour InfluxDB (avec nanosecondes)
    start_str = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    end_str = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Initialiser le dictionnaire pour stocker les résultats
    results = {}

    try:
        # Boucler sur chaque ville
        for ville in ville_list:
            results[ville] = {}
            # Boucler sur chaque paramètre
            for param in param_list:
                # Construire la requête InfluxDB pour ce paramètre
                query = f'''
                    from(bucket: "{source}")
                    |> range(start: {start_str}, stop: {end_str})
                    |> filter(fn: (r) => r._measurement == "meteo")
                    |> filter(fn: (r) => r._field == "{param}")
                    |> filter(fn: (r) => r.ville == "{ville}")
                    |> yield(name: "raw")
                '''

                # Exécuter la requête
                result = client.query_api().query(org=org, query=query)

                from collections import defaultdict

                # Dictionnaire pour stocker les valeurs par date
                daily_values = defaultdict(list)

                for table in result:
                    for record in table.records:
                        date_only = record.get_time().strftime("%Y-%m-%d")  
                        daily_values[date_only].append(record.get_value())

                # Calcul de la moyenne par date
                data = []
                for date, values in daily_values.items():
                    moyenne = sum(values) / len(values)
                    data.append({
                        "date" : date,
                        "value": round(moyenne)
                    })

                if not data:
                    results[ville][param] = {'message': f'Aucune donnée pour le paramètre {param}'}
                    continue
                
                # Convertir en DataFrame pour analyse descriptive
                df = pd.DataFrame(data, columns=['value'])
                description = df.describe().to_dict()

                # Stocker les statistiques pour ce paramètre
                results[ville][param] = description['value']

        # Vérifier si aucune donnée n'a été trouvée pour aucune ville/paramètre
        if not any('count' in param_data for ville_data in results.values() for param_data in ville_data.values()):
            return jsonify({
                'message': f'Aucune donnée pour les {days} derniers jours, villes {villes}, paramètres {params}'
            }), 200
        
        user_id = get_userId()
        new_method =  MethodeAnalyse(
            nom = 'Analyse Descriptive',
            description = "Avoir une description détaillée des valeurs pour les paramètres climatiques en fonction de la zone",
            categorie = "Descriptive",
            parametres = param_list,
            zone = ville_list,
            complexite = "Moyen",
            user_id = user_id )
        
        db.session.add(new_method)
        db.session.commit()
        # Formater la réponse
        return jsonify({
            'villes': ville_list,
            'source': source,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'statistics': results,
            'message': 'success'
        }), 200 
    except Exception as e:
        return jsonify({'error': 'Erreur lors de l\'analyse veuillez réessayer'}), 500
    

#Tendances analysis
@chercheur_routes.route('/chercheur/tendances/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_trend_analysis(villes, source, params, period):
    # Valider et extraire le nombre de jours depuis le paramètre period
    try:
        if period.endswith('d'):
            days = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7d ou 30d'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "d"'}), 400

    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if not ville_list or not param_list:
        return jsonify({'error': 'Au moins une ville et un paramètre sont requis'}), 400
    # Calculer la période
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=days-1) 

    # Formatter les dates en RFC3339 pour InfluxDB (avec nanosecondes)
    start_str = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    end_str = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Calculer la durée de la période en secondes
    period_seconds = days * 24 * 60 * 60

    # Initialiser le dictionnaire pour stocker les résultats
    results = {}

    try:
        # Boucler sur chaque ville
        for ville in ville_list:
            results[ville] = {}
            # Boucler sur chaque paramètre
            for param in param_list:
                # Construire la requête InfluxDB pour ce paramètre
                query = f'''
                    from(bucket: "{source}")
                    |> range(start: {start_str}, stop: {end_str})
                    |> filter(fn: (r) => r._measurement == "meteo")
                    |> filter(fn: (r) => r._field == "{param}")
                    |> filter(fn: (r) => r.ville == "{ville}")
                    |> yield(name: "raw")
                '''

                # Exécuter la requête
                result = client.query_api().query(org=org, query=query)

                from collections import defaultdict

                # Dictionnaire pour stocker les valeurs par date
                daily_values = defaultdict(list)

                for table in result:
                    for record in table.records:
                        date_only = record.get_time().strftime("%Y-%m-%d")  
                        daily_values[date_only].append(record.get_value())

                # Calcul de la moyenne par date
                data = []
                for date, values in daily_values.items():
                    moyenne = sum(values) / len(values)
                    data.append({
                        "time":date,
                        "value": round(moyenne)
                    })

                # Tri par date ascendante
                data.sort(key=lambda x: x["time"])

                if not data:
                    results[ville][param] = {'message': f'Aucune donnée pour le paramètre {param}'}
                    continue
                
                # Convertir en DataFrame
                df = pd.DataFrame(data)
                df['time'] = pd.to_datetime(df['time'])
                df['time_ordinal'] = df['time'].apply(lambda x: x.timestamp())  # Convertir en timestamp Unix

                # Effectuer la régression linéaire pour détecter la tendance
                slope,intercept, r_value, p_value, std_err = linregress(df['time_ordinal'], df['value'])
                

                # Calculer l'augmentation totale  (unité du paramètre)
                period_seconds_reel = (df['time'].iloc[-1] - df['time'].iloc[0]).total_seconds()
                total_increase = slope * period_seconds_reel

                # Interpréter la tendance
                trend_direction = "croissante" if slope > 0 else "décroissante" if slope < 0 else "stable"

                # Stocker les résultats de la tendance
                results[ville][param] = {
                    'slope': slope,
                    'r_value': r_value,
                    'p_value': p_value,
                    'std_err': std_err,
                    'trend_direction': trend_direction,
                    'total_increase': round(total_increase, 3),  # Arrondi à 3 décimales
                    'data_points': len(df)
                }

        # Vérifier si aucune donnée n'a été trouvée pour aucune ville/paramètre
        if not any('slope' in param_data for ville_data in results.values() for param_data in ville_data.values()):
            return jsonify({
                'message': f'Aucune donnée pour les {days} derniers jours, villes {villes}, paramètres {params}'
            }), 200
        
        user_id = get_userId()
        new_method =  MethodeAnalyse(
            nom = 'Analyse Tendance',
            description = "Analyse de tendance pour les paramètres climatiques en fonction de la zone",
            categorie = "Tendance",
            parametres = param_list,
            zone = ville_list,
            complexite = "Avancé",
            user_id = user_id )
        
        db.session.add(new_method)
        db.session.commit()
        # Formater la réponse
        return jsonify({
            'villes': ville_list,
            'source': source,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'trends': results,
            'message': 'success'
        }), 200

    except Exception as e:
        print(e)
        return jsonify({'error': 'Erreur lors de l\'analyse veuillez réessayer'}), 500    
    
#Correlation Analysis
@chercheur_routes.route('/chercheur/correlation/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_correlation_analysis(villes, source, params, period):
    # Valider et extraire le nombre de jours depuis le paramètre period
    try:
        if period.endswith('d'):
            days = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7d ou 30d'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "d"'}), 400

    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if not ville_list:
        return jsonify({'error': 'Au moins une ville est requise'}), 400
    if len(param_list) < 2:
        return jsonify({'error': 'Au moins deux paramètres sont requis pour une analyse de corrélation (ex. : temperature,humidity)'}), 400

    # Calculer la période
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=days-1) 

    # Formatter les dates en RFC3339 (sans nanosecondes pour compatibilité)
    start_str = start.replace(microsecond=0).isoformat() + "Z"
    end_str = end.replace(microsecond=0).isoformat() + "Z"

    # Initialiser le dictionnaire pour stocker les résultats
    results = {}

    try:
        # Boucler sur chaque ville
        for ville in ville_list:
            # Construire la requête InfluxDB améliorée
            # Utiliser un filtre explicite sur _field avec OR
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

            # Exécuter la requête
            result = client.query_api().query(org=org, query=query)

            # Extraire les données dans un DataFrame
            data = []
            for table in result:
                for record in table.records:
                    row = {**record.values}  # Inclut _time et les champs pivotés
                    data.append(row)

            df = pd.DataFrame(data)

            # Vérifier si des données existent
            if df.empty:
                results[ville] = {'message': f'Aucune donnée pour les paramètres {params} dans la ville {ville}'}
                continue

            # Sélectionner uniquement les colonnes de paramètres
            df_pivot = df[param_list]

            # Gérer les valeurs manquantes (supprimer les lignes incomplètes)
            df_pivot = df_pivot.dropna()

            if df_pivot.empty:
                results[ville] = {'message': 'Aucune donnée alignée pour calculer la corrélation'}
                continue

            # Calculer la matrice de corrélation (Pearson par défaut)
            correlation_matrix = df_pivot.corr().to_dict()

            # Stocker les résultats
            results[ville] = {
                'correlation_matrix': correlation_matrix,
                'data_points': len(df_pivot)
            }

        # Vérifier si aucune donnée n'a été trouvée
        if not any('correlation_matrix' in result for result in results.values()):
            return jsonify({
                'message': f'Aucune donnée pour les {days} derniers jours, villes {villes}, paramètres {params}'
            }), 200

        # Enregistrer la méthode d'analyse
        user_id = get_userId()
        new_method = MethodeAnalyse(
            nom='Analyse Correlation',
            description="Voire la relation qui existe entre les paramètres climatiques",
            categorie="Correlation",
            parametres=param_list,
            zone=ville_list,
            complexite="Avancé",
            user_id=user_id
        )
        db.session.add(new_method)
        db.session.commit()

        # Formater la réponse
        return jsonify({
            'villes': ville_list,
            'source': source,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'correlations': results,
            'message': 'success'
        }), 200

    except Exception as e:
        return jsonify({'error': f'Erreur lors de l\'analyse : {str(e)}'}), 500


#Comparative analysis
@chercheur_routes.route('/chercheur/comparative/<villes>/<source>/<params>/<period>', methods=['GET'])
@token_required
def get_direct_comparaison(villes,source,params, period):
    # Valider et extraire le nombre de jours depuis le paramètre period
    try:
        if period.endswith('d'):
            days = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7d ou 30d'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "d"'}), 400

    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if len(ville_list) < 2:
        return jsonify({'error': 'Au moins deux villes sont requises pour une comparaison directe'}), 400
    if not param_list:
        return jsonify({'error': 'Au moins un paramètre est requis'}), 400

    # Calculer la période
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=days-1) 

    # Formatter les dates en RFC3339 pour InfluxDB (avec nanosecondes)
    start_str = start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    end_str = end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    # Initialiser le dictionnaire pour stocker les moyennes
    averages = {}
    # Initialiser le dictionnaire pour stocker les différences
    differences = {}

    try:
        # Boucler sur chaque ville
        for ville in ville_list:
            averages[ville] = {}
            for param in param_list:
                # Construire la requête InfluxDB
                query = f'''
                    from(bucket: "{source}")
                    |> range(start: {start_str}, stop: {end_str})
                    |> filter(fn: (r) => r._measurement == "meteo")
                    |> filter(fn: (r) => r._field == "{param}")
                    |> filter(fn: (r) => r.ville == "{ville}")
                    |> yield(name: "raw")
                '''

                # Exécuter la requête
                result = client.query_api().query(org=org, query=query)

                from collections import defaultdict

                # Dictionnaire pour stocker les valeurs par date
                daily_values = defaultdict(list)

                for table in result:
                    for record in table.records:
                        date_only = record.get_time().strftime("%Y-%m-%d")  
                        daily_values[date_only].append(record.get_value())

                # Calcul de la moyenne par date
                data = []
                for date, values in daily_values.items():
                    moyenne = sum(values) / len(values)
                    data.append({
                        round(moyenne)
                    })
                
                # Vérifier si des données existent
                if not data:
                    averages[ville][param] = {'message': f'Aucune donnée pour le paramètre {param}'}
                    continue

                # Calculer la moyenne
                df = pd.DataFrame(data, columns=['value'])
                averages[ville][param] = round(df['value'].mean(), 2)
        
        # Vérifier si aucune donnée n'a été trouvée
        if not any(isinstance(param_data, (int, float)) for ville_data in averages.values() for param_data in ville_data.values()):
            return jsonify({
                'message': f'Aucune donnée pour les {days} derniers jours, villes {villes}, paramètres {params}'
            }), 200

        # Calculer les différences entre chaque paire de villes
        for param in param_list:
            differences[param] = {}
            # Générer toutes les paires de villes
            for ville1, ville2 in combinations(ville_list, 2):
                if isinstance(averages[ville1].get(param), (int, float)) and isinstance(averages[ville2].get(param), (int, float)):
                    diff = averages[ville1][param] - averages[ville2][param]
                    differences[param][f"{ville1} - {ville2}"] = round(diff, 3)
        user_id = get_userId()
        new_method =  MethodeAnalyse(
            nom = 'Analyse Comparative',
            description = "Voir l'écart entre les mêmes paramètres climatiques pour des zones différentes",
            categorie = "Comparative",
            parametres = param_list,
            zone = ville_list,
            complexite = "Moyen",
            user_id = user_id )
        
        db.session.add(new_method)
        db.session.commit()
        # Formater la réponse
        return jsonify({
            'villes': ville_list,
            'params': param_list,
            'period': f'{days} derniers jours',
            'start': start_str,
            'end': end_str,
            'averages': averages,
            'differences': differences,
            'message': 'success'
        }), 200
    except Exception as e:
        return jsonify({'error': 'Erreur lors de l\'analyse veuillez réessayer'}), 500
    


#Descriptive analysis Sqlserver
@chercheur_routes.route('/chercheur/descriptive_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_descriptive_sql_analysis(villes, params, period):
    # Valider et extraire l'intervalle d'années depuis le paramètre period
    try:
        if period.endswith('y'):
            year = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7d ou 30d'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "d"'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un intervalle d\'années comme 2024-2025'}), 400
    year_start  = 2025 - year
    year_end = 2024
    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if not ville_list or not param_list:
        return jsonify({'error': 'Au moins une ville et un paramètre sont requis'}), 400

    # Initialiser le dictionnaire pour stocker les résultats
    results = {}

    try:
        with engine.connect() as connection:
            # Boucler sur chaque ville
            for ville in ville_list:
                results[ville] = {}
                # Boucler sur chaque paramètre
                for param in param_list:
                    # Construire la requête SQL
                    query = text(f"""
                        SELECT
                            z.Ville,
                            f.{param},
                            d.Annee,
                            d.Mois
                        FROM [climate_data_integration].[dbo].[FaitClimat] AS f
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] AS z
                            ON f.Zone_Id = z.Id
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Date] AS d
                            ON f.Date_Id = d.Id
                        WHERE z.Ville = '{ville}'
                            AND d.Annee BETWEEN {year_start} AND {year_end}
                        ORDER BY z.Ville, d.Id
                    """)

                    # Exécuter la requête avec les paramètres
                    result = connection.execute(query, {
                        "param": param,
                    }).mappings().fetchall()
                   
                    
                    # Extraire les valeurs pour le paramètre
                    values = [row[param] for row in result if row[param] is not None]

                    # Vérifier si des données existent pour ce paramètre
                    if not values:
                        results[ville][param] = {'message': f'Aucune donnée pour le paramètre {param}'}
                        continue

                    # Convertir en DataFrame pour analyse descriptive
                    df = pd.DataFrame(values, columns=['value'])
                    description = df.describe().to_dict()

                    # Stocker les statistiques pour ce paramètre
                    results[ville][param] = description['value']

            # Vérifier si aucune donnée n'a été trouvée pour aucune ville/paramètre
            if not any('count' in param_data for ville_data in results.values() for param_data in ville_data.values()):
                return jsonify({
                    'message': f'Aucune donnée pour les années {period}, villes {villes}, paramètres {params}'
                }), 200

            # Enregistrer la méthode d'analyse
            user_id = get_userId()
            new_method = MethodeAnalyse(
                nom='Analyse Descriptive',
                description="Avoir une description détaillée des valeurs pour les paramètres climatiques en fonction de la zone",
                categorie="Descriptive",
                parametres=param_list,
                zone=ville_list,
                complexite="Moyen",
                user_id=user_id
            )
            db.session.add(new_method)
            db.session.commit()
            # Formater la réponse
            return jsonify({
                'villes': ville_list,
                'params': param_list,
                'period': period,
                'statistics': results,
                'message': 'success'
            }), 200

    except Exception as e:
        return jsonify({'error': f'Erreur lors de l\'analyse : {str(e)}'}), 500



#Tendances Analysis for etl
@chercheur_routes.route('/chercheur/tendances_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_trend_sql_analysis(villes, params, period):
    # Valider et extraire l'intervalle d'années depuis le paramètre period
    try:
        if period.endswith('y'):
            year = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7y'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "y"'}), 400

    year_start = 2025 - year
    year_end = 2024

    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if not ville_list or not param_list:
        return jsonify({'error': 'Au moins une ville et un paramètre sont requis'}), 400

    # Initialiser le dictionnaire pour stocker les résultats
    results = {}

    try:
        with engine.connect() as connection:
            # Boucler sur chaque ville
            for ville in ville_list:
                results[ville] = {}
                # Boucler sur chaque paramètre
                for param in param_list:
                    # Construire la requête SQL avec paramètres bindés
                    query = text(f"""
                        SELECT
                            z.Ville,
                            f.{param} AS value,
                            d.Annee,
                            d.Mois
                        FROM [climate_data_integration].[dbo].[FaitClimat] AS f
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] AS z
                            ON f.Zone_Id = z.Id
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Date] AS d
                            ON f.Date_Id = d.Id
                        WHERE z.Ville = '{ville}'
                            AND d.Annee BETWEEN {year_start} AND {year_end}
                        ORDER BY z.Ville, d.Id
                    """)

                    # Exécuter la requête avec les paramètres
                    result = connection.execute(query, {
                        "param": param
                    }).mappings().fetchall()

                    # Extraire les timestamps et valeurs
                    data = []
                    for row in result:
                        if row['value'] is not None:
                            data.append({
                                'time': datetime(row['Annee'], row['Mois'], 1),                                 
                                'value': row['value']
                            })

                    # Vérifier si des données existent pour ce paramètre
                    if not data:
                        results[ville][param] = {'message': f'Aucune donnée pour le paramètre {param}'}
                        continue

                    # Convertir en DataFrame
                    df = pd.DataFrame(data)
                    df['time_ordinal'] = df['time'].apply(lambda x: x.timestamp())  # Convertir en timestamp Unix

                    # Effectuer la régression linéaire pour détecter la tendance
                    period_seconds = (year_end - year_start + 1) * 365 * 24 * 60 * 60  # Approximation en secondes
                    slope, intercept, r_value, p_value, std_err = linregress(df['time_ordinal'], df['value'])

                    # Calculer l'augmentation totale en unités du paramètre
                    total_increase = slope * period_seconds

                    # Interpréter la tendance
                    trend_direction = "croissante" if slope > 0 else "décroissante" if slope < 0 else "stable"

                    # Stocker les résultats de la tendance
                    results[ville][param] = {
                        'slope': slope,
                        'intercept': intercept,
                        'r_value': r_value,
                        'p_value': p_value,
                        'std_err': std_err,
                        'trend_direction': trend_direction,
                        'total_increase': round(total_increase, 3),
                        'data_points': len(df)
                    }

            # Vérifier si aucune donnée n'a été trouvée pour aucune ville/paramètre
            if not any('slope' in param_data for ville_data in results.values() for param_data in ville_data.values()):
                return jsonify({
                    'message': f'Aucune donnée pour les années {year_start}-{year_end}, villes {villes}, paramètres {params}'
                }), 200

            # Enregistrer la méthode d'analyse
            user_id = get_userId()
            new_method = MethodeAnalyse(
                nom='Analyse Tendance',
                description="Analyse de tendance pour les paramètres climatiques en fonction de la zone",
                categorie="Tendance",
                parametres=param_list,
                zone=ville_list,
                complexite="Avancé",
                user_id=user_id
            )
            db.session.add(new_method)
            db.session.commit()
            period = 0
            if year_start == year_end :
                period = year_end
            else :
                period = f'{year_start}-{year_end}'
            # Formater la réponse
            return jsonify({
                'villes': ville_list,
                'params': param_list,
                'period': period,
                'trends': results,
                'message': 'success'
            }), 200

    except Exception as e:
        print(e)
        return jsonify({'error': f'Erreur lors de l\'analyse : {str(e)}'}), 500

from flask import jsonify
from sqlalchemy import text, create_engine
from datetime import datetime
import pandas as pd
import logging

# Configurer le logger
logger = logging.getLogger(__name__)

#Correlation Analysis
@chercheur_routes.route('/chercheur/correlation_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_correlation_sql_analysis(villes, params, period):
    # Valider et extraire l'intervalle d'années depuis le paramètre period
    try:
        if period.endswith('y'):
            year = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7y'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "y"'}), 400

    year_start = 2025 - year
    year_end = 2024

    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if not ville_list:
        return jsonify({'error': 'Au moins une ville est requise'}), 400
    if len(param_list) < 2:
        return jsonify({'error': 'Au moins deux paramètres sont requis pour une analyse de corrélation (ex. : Temperature,Humidity)'}), 400

    # Initialiser le dictionnaire pour stocker les résultats
    results = {}

    try:
        with engine.connect() as connection:
            # Boucler sur chaque ville
            for ville in ville_list:
                # Construire la requête SQL pour tous les paramètres
                param_columns = ', '.join([f"f.[{param}]" for param in param_list])
                query = text(f"""
                    SELECT
                        z.Ville,
                        {param_columns},
                        d.Annee,
                        d.Mois
                    FROM [climate_data_integration].[dbo].[FaitClimat] AS f
                    INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] AS z
                        ON f.Zone_Id = z.Id
                    INNER JOIN [climate_data_integration].[dbo].[Dim_Date] AS d
                        ON f.Date_Id = d.Id
                    WHERE z.Ville = '{ville}'
                        AND d.Annee BETWEEN {year_start} AND {year_end}
                    ORDER BY d.Annee, d.Mois
                """)

                # Exécuter la requête avec les paramètres
                result = connection.execute(query).mappings().fetchall()

                # Extraire les données : année-mois, paramètres
                data = []
                for row in result:
                    time_obj = datetime(row['Annee'], row['Mois'], 1)  # Crée un objet datetime
                    row_data = {
                        'time': f"{row['Annee']}-{row['Mois']:02d}",  # Format YYYY-MM
                        'time_obj': time_obj  # Utilisé comme index
                    }
                    for param in param_list:
                        if row[param] is not None:
                            row_data[param] = row[param]
                    data.append(row_data)

                # Vérifier si des données existent pour cette ville
                if not data:
                    results[ville] = {'message': f'Aucune donnée pour les paramètres {params} dans la ville {ville}'}
                    continue

                # Convertir en DataFrame avec time_obj comme index
                df = pd.DataFrame(data).set_index('time_obj')

                # Sélectionner uniquement les colonnes de paramètres
                df_pivot = df[param_list]

                # Gérer les valeurs manquantes (supprimer les lignes incomplètes)
                df_pivot = df_pivot.dropna()

                if df_pivot.empty:
                    results[ville] = {'message': 'Aucune donnée alignée pour calculer la corrélation'}
                    continue

                # Calculer la matrice de corrélation (Pearson par défaut)
                correlation_matrix = df_pivot.corr().to_dict()

                # Stocker les résultats
                results[ville] = {
                    'correlation_matrix': correlation_matrix,
                    'data_points': len(df_pivot)
                }

            # Vérifier si aucune donnée n'a été trouvée pour aucune ville
            if not any('correlation_matrix' in result for result in results.values()):
                return jsonify({
                    'message': f'Aucune donnée pour les années {year_start}-{year_end}, villes {villes}, paramètres {params}'
                }), 200

            # Enregistrer la méthode d'analyse
            user_id = get_userId()
            new_method = MethodeAnalyse(
                nom='Analyse Correlation',
                description="Voire la relation qui existe entre les paramètres climatiques",
                categorie="Correlation",
                parametres=param_list,
                zone=ville_list,
                complexite="Avancé",
                user_id=user_id
            )
            db.session.add(new_method)
            db.session.commit()
            period = 0
            if year_start == year_end :
                period = year_end
            else :
                period = f'{year_start}-{year_end}'
            # Formater la réponse
            return jsonify({
                'villes': ville_list,
                'params': param_list,
                'period': period ,
                'correlations': results,
                'message': 'success'
            }), 200

    except Exception as e:
        logger.error(f"Erreur lors de l'analyse : {str(e)}", exc_info=True)
        return jsonify({'error': f'Erreur lors de l\'analyse : {str(e)}'}), 500
    

# Comparative analysis
@chercheur_routes.route('/chercheur/comparative_sqlserver/<villes>/<params>/<period>', methods=['GET'])
@token_required
def get_comparaison_sql_server(villes, params, period):
    # Valider et extraire le nombre de jours depuis le paramètre period
    try:
        if period.endswith('y'):
            year = int(period[:-1])
        else:
            return jsonify({'error': 'Format de period invalide, utiliser par ex. 7y'}), 400
    except ValueError:
        return jsonify({'error': 'Format de period invalide, utiliser un nombre suivi de "y"'}), 400

    year_start = 2025 - year
    year_end = 2024

    # Extraire les villes et paramètres (séparés par des virgules)
    ville_list = villes.split(',')
    param_list = params.split(',')
    if len(ville_list) < 2:
        return jsonify({'error': 'Au moins deux villes sont requises pour une comparaison directe'}), 400
    if not param_list:
        return jsonify({'error': 'Au moins un paramètre est requis'}), 400
    # Initialiser le dictionnaire pour stocker les moyennes
    averages = {}
    # Initialiser le dictionnaire pour stocker les différences
    differences = {}

    try:
        with engine.connect() as connection:
            # Boucler sur chaque ville
            for ville in ville_list:
                averages[ville] = {}
                # Boucler sur chaque paramètre
                for param in param_list:
                    # Construire la requête SQL avec paramètres bindés
                    query = text(f"""
                        SELECT
                            z.Ville,
                            f.{param} AS value,
                            d.Annee,
                            d.Mois
                        FROM [climate_data_integration].[dbo].[FaitClimat] AS f
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Zone] AS z
                            ON f.Zone_Id = z.Id
                        INNER JOIN [climate_data_integration].[dbo].[Dim_Date] AS d
                            ON f.Date_Id = d.Id
                        WHERE z.Ville = :ville
                            AND d.Annee BETWEEN :year_start AND :year_end
                        ORDER BY z.Ville, d.Id
                    """)

                    # Exécuter la requête avec les paramètres
                    result = connection.execute(query, {
                        "ville": ville,
                        "year_start": year_start,
                        "year_end": year_end
                    }).mappings().fetchall()

                    # Extraire les valeurs
                    values = [row['value'] for row in result if row['value'] is not None]

                    # Vérifier si des données existent
                    if not values:
                        averages[ville][param] = {'message': f'Aucune donnée pour le paramètre {param}'}
                        continue

                    # Calculer la moyenne
                    df = pd.DataFrame(values, columns=['value'])
                    averages[ville][param] = round(df['value'].mean(), 3)

        # Vérifier si aucune donnée n'a été trouvée
        if not any(isinstance(param_data, (int, float)) for ville_data in averages.values() for param_data in ville_data.values()):
            return jsonify({
                'message': f'Aucune donnée pour les années {year_start} à {year_end}, villes {villes}, paramètres {params}'
            }), 200

        # Calculer les différences entre chaque paire de villes
        for param in param_list:
            differences[param] = {}
            # Générer toutes les paires de villes
            for ville1, ville2 in combinations(ville_list, 2):
                if isinstance(averages[ville1].get(param), (int, float)) and isinstance(averages[ville2].get(param), (int, float)):
                    diff = averages[ville1][param] - averages[ville2][param]
                    differences[param][f"{ville1} - {ville2}"] = round(diff, 3)
        
        user_id = get_userId()
        new_method = MethodeAnalyse(
            nom='Analyse Comparative',
            description="Voir l'écart entre les mêmes paramètres climatiques pour des zones différentes",
            categorie="Comparative",
            parametres=param_list,
            zone=ville_list,
            complexite="Moyen",
            user_id=user_id
        )
        
        db.session.add(new_method)
        db.session.commit()

        period = 0
        if year_start == year_end :
            period = year_end
        else :
            period = f'{year_start}-{year_end}'
        # Formater la réponse
        return jsonify({
            'villes': ville_list,
            'params': param_list,
            'period': period,
            'start': str(year_start),
            'end': str(year_end),
            'averages': averages,
            'differences': differences,
            'message': 'success'
        }), 200

    except Exception as e:
        return jsonify({'error': 'Erreur lors de l\'analyse veuillez réessayer'}), 500




#Previsions
@chercheur_routes.route('/chercheur/prevision/<ville>/<n_days>', methods=['GET'])
@token_required
def get_mlast_avg(ville,n_days):
    query = f'''
        from(bucket: "climate_data_openweather")
        |> range(start: -30d, stop: -1d)
        |> filter(fn: (r) => r._measurement == "meteo")
        |> filter(fn: (r) => r._field == "temperature")
        |> filter(fn: (r) => r.ville == "{ville}")
        |> yield(name: "raw")
    '''

    result = client.query_api().query(org=org, query=query)
    daily_values = defaultdict(list)

    for table in result:
        for record in table.records:
            date_only = record.get_time().strftime("%Y-%m-%d")
            daily_values[date_only].append(record.get_value())

    monthly_avg = []
    for date, values in daily_values.items():
        moyenne = sum(values) / len(values)
        monthly_avg.append({
            "date": date,
            "moyenne": round(moyenne, 2)
        })

    monthly_avg.sort(key=lambda x: x["date"])
    df = pd.DataFrame(monthly_avg)
    df["date"] = pd.to_datetime(df["date"])

    # Charger le modèle et les scalers
    with open("C:/Users/Mark/Downloads/lstm_temperature_predict_v2.sav", "rb") as f:
        model = pickle.load(f)

    with open("C:/Users/Mark/Downloads/scaler_temperature.sav", "rb") as f:
        scalers_ville = pickle.load(f)

    def predict_prochain_jours(last_days, model, scaler, n_days):
        last_days_scaled = scaler.transform(last_days.reshape(-1, 1))
        input_seq = last_days_scaled.reshape(1, last_days_scaled.shape[0], 1)

        predictions = []
        for _ in range( int(n_days)):
            pred = model.predict(input_seq, verbose=0)[0][0]
            predictions.append(pred)
            input_seq = np.append(input_seq[:, 1:, :], [[[pred]]], axis=1)

        return scaler.inverse_transform(np.array(predictions).reshape(-1, 1)).flatten()

    derniers_jours = df["moyenne"].values[-30:]
    scaler = scalers_ville[ville]

    predictions = predict_prochain_jours(derniers_jours, model, scaler, n_days)
    derniere_date = df["date"].iloc[-1]
    dates = [derniere_date + timedelta(days=i) for i in range(1, int(n_days)+1)]

   
    return jsonify({
        "predictions":list(map(lambda x: round(x,2), predictions.tolist())),    
        "dates": [d.strftime("%Y-%m-%d") for d in dates],  
        "n_jours" : n_days,
        "message": "success"
    }), 200
