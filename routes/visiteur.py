from flask import Blueprint, Flask, jsonify
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxclient import client
from dotenv import load_dotenv
import os

load_dotenv()

bucket_weather = "climate_data_weather"
org = os.getenv("org")

visiteur_routes = Blueprint('visiteur_routes', __name__)


@visiteur_routes.route('/visiteur/meteo/<ville>', methods=['GET'])
def get_meteo_data(ville):
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
        "data":data
        }),200

"""


# Service d'Analyse Climatique avec Flask
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from datetime import datetime
import json
import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import redis
from celery import Celery
import logging

# Configuration de l'application
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://user:password@localhost/climate_analysis'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

# Initialisation des extensions
db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Configuration Celery pour les tâches asynchrones
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Configuration Redis pour le cache
redis_client = redis.Redis(host='localhost', port=6379, db=1)

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===== MODÈLES DE BASE DE DONNÉES =====

class MethodeAnalyse(db.Model):
    __tablename__ = 'methodes_analyse'
    
    id = db.Column(db.Integer, primary_key=True)
    nom_methode = db.Column(db.String(100), nullable=False)
    categorie = db.Column(db.Enum('descriptive', 'temporelle', 'spatiale', 'comparative', 'predictive', 'impact', name='categorie_enum'), nullable=False)
    description = db.Column(db.Text)
    parametres_requis = db.Column(db.JSON)
    type_donnees_compatibles = db.Column(db.JSON)
    niveau_complexite = db.Column(db.Enum('basique', 'intermediaire', 'avance', name='complexite_enum'))
    actif = db.Column(db.Boolean, default=True)
    date_creation = db.Column(db.DateTime, default=datetime.utcnow)

class HistoriqueAnalyse(db.Model):
    __tablename__ = 'historique_analyses'
    
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    methode_analyse_id = db.Column(db.Integer, db.ForeignKey('methodes_analyse.id'))
    parametres_utilises = db.Column(db.JSON)
    dataset_source = db.Column(db.String(255))
    resultats = db.Column(db.JSON)
    date_execution = db.Column(db.DateTime, default=datetime.utcnow)
    duree_execution = db.Column(db.Integer)  # en secondes
    statut = db.Column(db.String(20), default='en_cours')
    
    methode = db.relationship('MethodeAnalyse', backref='historique')

# ===== CLASSES D'ANALYSE =====

class AnalyseDescriptive:
    @staticmethod
    def calculer_statistiques(data):
        Calcule les statistiques descriptives de base
        try:
            df = pd.DataFrame(data)
            resultats = {}
            
            for column in df.select_dtypes(include=[np.number]).columns:
                resultats[column] = {
                    'moyenne': float(df[column].mean()),
                    'mediane': float(df[column].median()),
                    'ecart_type': float(df[column].std()),
                    'minimum': float(df[column].min()),
                    'maximum': float(df[column].max()),
                    'q1': float(df[column].quantile(0.25)),
                    'q3': float(df[column].quantile(0.75)),
                    'nombre_valeurs': int(df[column].count()),
                    'valeurs_manquantes': int(df[column].isnull().sum())
                }
            
            return resultats
        except Exception as e:
            logger.error(f"Erreur dans calculer_statistiques: {str(e)}")
            raise

    @staticmethod
    def calculer_correlations(data, variables):
        Calcule les corrélations entre variables
        try:
            df = pd.DataFrame(data)
            correlation_matrix = df[variables].corr()
            return correlation_matrix.to_dict()
        except Exception as e:
            logger.error(f"Erreur dans calculer_correlations: {str(e)}")
            raise

class AnalyseTemporelle:
    @staticmethod
    def detecter_tendance(data, variable_temps, variable_valeur):
        Détecte les tendances temporelles
        try:
            df = pd.DataFrame(data)
            df[variable_temps] = pd.to_datetime(df[variable_temps])
            df = df.sort_values(variable_temps)
            
            # Régression linéaire pour détecter la tendance
            X = np.arange(len(df)).reshape(-1, 1)
            y = df[variable_valeur].values
            
            model = LinearRegression()
            model.fit(X, y)
            
            # Test de significativité
            correlation = stats.pearsonr(X.flatten(), y)
            
            return {
                'pente': float(model.coef_[0]),
                'intercept': float(model.intercept_),
                'r_squared': float(model.score(X, y)),
                'correlation': float(correlation[0]),
                'p_value': float(correlation[1]),
                'tendance': 'croissante' if model.coef_[0] > 0 else 'décroissante' if model.coef_[0] < 0 else 'stable'
            }
        except Exception as e:
            logger.error(f"Erreur dans detecter_tendance: {str(e)}")
            raise

    @staticmethod
    def analyser_saisonnalite(data, variable_temps, variable_valeur):
        Analyse la saisonnalité des données
        try:
            df = pd.DataFrame(data)
            df[variable_temps] = pd.to_datetime(df[variable_temps])
            df['mois'] = df[variable_temps].dt.month
            
            moyennes_mensuelles = df.groupby('mois')[variable_valeur].mean()
            ecarts_mensuelles = df.groupby('mois')[variable_valeur].std()
            
            return {
                'moyennes_par_mois': moyennes_mensuelles.to_dict(),
                'ecarts_type_par_mois': ecarts_mensuelles.to_dict(),
                'amplitude_saisonniere': float(moyennes_mensuelles.max() - moyennes_mensuelles.min())
            }
        except Exception as e:
            logger.error(f"Erreur dans analyser_saisonnalite: {str(e)}")
            raise

class AnalysePredictive:
    @staticmethod
    def prediction_regression(data, variable_cible, variables_explicatives, horizon=30):
        Prédiction par régression
        try:
            df = pd.DataFrame(data)
            
            X = df[variables_explicatives]
            y = df[variable_cible]
            
            # Modèle Random Forest
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X, y)
            
            # Score du modèle
            score = model.score(X, y)
            
            # Importance des variables
            importance = dict(zip(variables_explicatives, model.feature_importances_))
            
            return {
                'score_modele': float(score),
                'importance_variables': {k: float(v) for k, v in importance.items()},
                'modele_type': 'RandomForest',
                'nombre_echantillons': len(df)
            }
        except Exception as e:
            logger.error(f"Erreur dans prediction_regression: {str(e)}")
            raise

# ===== TÂCHES ASYNCHRONES CELERY =====

@celery.task
def executer_analyse_async(user_id, methode_id, parametres, data):
    Exécute une analyse de manière asynchrone
    start_time = datetime.utcnow()
    
    try:
        # Récupération de la méthode
        methode = MethodeAnalyse.query.get(methode_id)
        if not methode:
            raise ValueError("Méthode d'analyse non trouvée")
        
        # Exécution selon le type d'analyse
        resultats = {}
        
        if methode.categorie == 'descriptive':
            if methode.nom_methode == 'statistiques_descriptives':
                resultats = AnalyseDescriptive.calculer_statistiques(data)
            elif methode.nom_methode == 'correlations':
                resultats = AnalyseDescriptive.calculer_correlations(data, parametres.get('variables', []))
                
        elif methode.categorie == 'temporelle':
            if methode.nom_methode == 'detection_tendance':
                resultats = AnalyseTemporelle.detecter_tendance(
                    data, 
                    parametres.get('variable_temps'), 
                    parametres.get('variable_valeur')
                )
            elif methode.nom_methode == 'analyse_saisonnalite':
                resultats = AnalyseTemporelle.analyser_saisonnalite(
                    data,
                    parametres.get('variable_temps'),
                    parametres.get('variable_valeur')
                )
                
        elif methode.categorie == 'predictive':
            if methode.nom_methode == 'regression_predictive':
                resultats = AnalysePredictive.prediction_regression(
                    data,
                    parametres.get('variable_cible'),
                    parametres.get('variables_explicatives', [])
                )
        
        # Calcul du temps d'exécution
        duree = (datetime.utcnow() - start_time).total_seconds()
        
        # Sauvegarde en base
        with app.app_context():
            historique = HistoriqueAnalyse(
                user_id=user_id,
                methode_analyse_id=methode_id,
                parametres_utilises=parametres,
                resultats=resultats,
                duree_execution=int(duree),
                statut='termine'
            )
            db.session.add(historique)
            db.session.commit()
        
        return {
            'success': True,
            'resultats': resultats,
            'duree_execution': duree,
            'historique_id': historique.id
        }
        
    except Exception as e:
        logger.error(f"Erreur dans l'analyse asynchrone: {str(e)}")
        
        # Sauvegarde de l'erreur en base
        with app.app_context():
            historique = HistoriqueAnalyse(
                user_id=user_id,
                methode_analyse_id=methode_id,
                parametres_utilises=parametres,
                statut='erreur',
                duree_execution=int((datetime.utcnow() - start_time).total_seconds())
            )
            db.session.add(historique)
            db.session.commit()
        
        return {
            'success': False,
            'error': str(e)
        }

# ===== ROUTES API =====

@app.route('/health', methods=['GET'])
def health_check():
    Vérification de l'état du service
    return jsonify({'status': 'healthy', 'service': 'climate-analysis'}), 200

@app.route('/methodes', methods=['GET'])
def get_methodes():
    Récupère toutes les méthodes d'analyse disponibles
    try:
        methodes = MethodeAnalyse.query.filter_by(actif=True).all()
        result = []
        
        for methode in methodes:
            result.append({
                'id': methode.id,
                'nom_methode': methode.nom_methode,
                'categorie': methode.categorie,
                'description': methode.description,
                'parametres_requis': methode.parametres_requis,
                'niveau_complexite': methode.niveau_complexite
            })
        
        return jsonify(result), 200
    except Exception as e:
        logger.error(f"Erreur dans get_methodes: {str(e)}")
        return jsonify({'error': 'Erreur serveur'}), 500

@app.route('/analyse/synchrone', methods=['POST'])
def analyse_synchrone():
    Exécute une analyse de manière synchrone (pour analyses rapides)
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        methode_id = data.get('methode_id')
        parametres = data.get('parametres', {})
        dataset = data.get('data')
        
        if not all([user_id, methode_id, dataset]):
            return jsonify({'error': 'Paramètres manquants'}), 400
        
        # Vérification en cache
        cache_key = f"analyse:{methode_id}:{hash(str(parametres))}:{hash(str(dataset))}"
        cached_result = redis_client.get(cache_key)
        
        if cached_result:
            return jsonify(json.loads(cached_result)), 200
        
        # Exécution de l'analyse
        result = executer_analyse_async.apply(args=[user_id, methode_id, parametres, dataset])
        response_data = result.get()
        
        if response_data['success']:
            # Mise en cache pour 1 heure
            redis_client.setex(cache_key, 3600, json.dumps(response_data))
            return jsonify(response_data), 200
        else:
            return jsonify(response_data), 400
            
    except Exception as e:
        logger.error(f"Erreur dans analyse_synchrone: {str(e)}")
        return jsonify({'error': 'Erreur serveur'}), 500

@app.route('/analyse/asynchrone', methods=['POST'])
def analyse_asynchrone():
    Lance une analyse de manière asynchrone (pour analyses longues)
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        methode_id = data.get('methode_id')
        parametres = data.get('parametres', {})
        dataset = data.get('data')
        
        if not all([user_id, methode_id, dataset]):
            return jsonify({'error': 'Paramètres manquants'}), 400
        
        # Lancement de la tâche asynchrone
        task = executer_analyse_async.delay(user_id, methode_id, parametres, dataset)
        
        return jsonify({
            'task_id': task.id,
            'status': 'en_cours',
            'message': 'Analyse lancée avec succès'
        }), 202
        
    except Exception as e:
        logger.error(f"Erreur dans analyse_asynchrone: {str(e)}")
        return jsonify({'error': 'Erreur serveur'}), 500

@app.route('/analyse/statut/<task_id>', methods=['GET'])
def statut_analyse(task_id):
    Vérifie le statut d'une analyse asynchrone
    try:
        task = executer_analyse_async.AsyncResult(task_id)
        
        if task.state == 'PENDING':
            response = {'status': 'en_cours', 'message': 'Analyse en cours...'}
        elif task.state == 'SUCCESS':
            response = {
                'status': 'termine',
                'resultats': task.result
            }
        else:
            response = {
                'status': 'erreur',
                'error': str(task.info)
            }
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"Erreur dans statut_analyse: {str(e)}")
        return jsonify({'error': 'Erreur serveur'}), 500

@app.route('/historique/<int:user_id>', methods=['GET'])
def get_historique(user_id):
   Récupère l'historique des analyses d'un utilisateur
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        
        historique = HistoriqueAnalyse.query.filter_by(user_id=user_id)\
                                           .order_by(HistoriqueAnalyse.date_execution.desc())\
                                           .paginate(page=page, per_page=per_page, error_out=False)
        
        result = []
        for h in historique.items:
            result.append({
                'id': h.id,
                'methode_nom': h.methode.nom_methode,
                'methode_categorie': h.methode.categorie,
                'date_execution': h.date_execution.isoformat(),
                'duree_execution': h.duree_execution,
                'statut': h.statut,
                'parametres': h.parametres_utilises
            })
        
        return jsonify({
            'historique': result,
            'total': historique.total,
            'pages': historique.pages,
            'page_courante': page
        }), 200
        
    except Exception as e:
        logger.error(f"Erreur dans get_historique: {str(e)}")
        return jsonify({'error': 'Erreur serveur'}), 500

@app.route('/analyse/resultats/<int:historique_id>', methods=['GET'])
def get_resultats(historique_id):
   Récupère les résultats détaillés d'une analyse
    try:
        historique = HistoriqueAnalyse.query.get(historique_id)
        
        if not historique:
            return jsonify({'error': 'Analyse non trouvée'}), 404
        
        return jsonify({
            'id': historique.id,
            'methode': {
                'nom': historique.methode.nom_methode,
                'categorie': historique.methode.categorie,
                'description': historique.methode.description
            },
            'parametres_utilises': historique.parametres_utilises,
            'resultats': historique.resultats,
            'date_execution': historique.date_execution.isoformat(),
            'duree_execution': historique.duree_execution,
            'statut': historique.statut
        }), 200
        
    except Exception as e:
        logger.error(f"Erreur dans get_resultats: {str(e)}")
        return jsonify({'error': 'Erreur serveur'}), 500

# ===== INITIALISATION DES DONNÉES =====

def init_methodes_analyse():
    Initialise les méthodes d'analyse en base
    methodes = [
        {
            'nom_methode': 'statistiques_descriptives',
            'categorie': 'descriptive',
            'description': 'Calcule les statistiques descriptives de base (moyenne, médiane, écart-type, etc.)',
            'parametres_requis': {'variables': 'Liste des variables à analyser'},
            'niveau_complexite': 'basique'
        },
        {
            'nom_methode': 'correlations',
            'categorie': 'descriptive',
            'description': 'Calcule les corrélations entre variables',
            'parametres_requis': {'variables': 'Liste des variables pour la matrice de corrélation'},
            'niveau_complexite': 'basique'
        },
        {
            'nom_methode': 'detection_tendance',
            'categorie': 'temporelle',
            'description': 'Détecte les tendances temporelles dans les données',
            'parametres_requis': {
                'variable_temps': 'Variable contenant les dates',
                'variable_valeur': 'Variable à analyser'
            },
            'niveau_complexite': 'intermediaire'
        },
        {
            'nom_methode': 'analyse_saisonnalite',
            'categorie': 'temporelle',
            'description': 'Analyse la saisonnalité des données climatiques',
            'parametres_requis': {
                'variable_temps': 'Variable contenant les dates',
                'variable_valeur': 'Variable à analyser'
            },
            'niveau_complexite': 'intermediaire'
        },
        {
            'nom_methode': 'regression_predictive',
            'categorie': 'predictive',
            'description': 'Modèle prédictif par régression Random Forest',
            'parametres_requis': {
                'variable_cible': 'Variable à prédire',
                'variables_explicatives': 'Liste des variables explicatives'
            },
            'niveau_complexite': 'avance'
        }
    ]
    
    for methode_data in methodes:
        methode = MethodeAnalyse.query.filter_by(nom_methode=methode_data['nom_methode']).first()
        if not methode:
            methode = MethodeAnalyse(**methode_data)
            db.session.add(methode)
    
    db.session.commit()

# ===== POINT D'ENTRÉE =====

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        init_methodes_analyse()
    
    app.run(debug=True, host='0.0.0.0', port=5000)"""
