from flask import Flask, Blueprint, jsonify
from flask_cors import CORS
from sqlalchemy import inspect 
from config import Config
from models.model import db
import pickle
import redis
from routes.chercheur import chercheur_routes

# Importer le middleware d'authentification
from auth_middleware import auth  # Assurez-vous que le chemin est correct

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

@app.route("/")
def home():
    return "Bienvenue sur votre application Flask !"

# Configurations and initializations
app.config.from_object(Config)

# Initialiser le middleware d'authentification
auth.init_app(app)

# Configuration CORS
CORS(app)

# Register routes 
app.register_blueprint(chercheur_routes, url_prefix='/api')

# Route de test pour vérifier l'authentification
@app.route('/api/test-auth', methods=['GET'])
def test_auth():
    """Route de test pour vérifier que l'app fonctionne"""
    return jsonify({
        'success': True,
        'message': 'Application Flask fonctionnelle',
        'middleware': 'Auth middleware initialisé'
    })

# Gestionnaire d'erreur pour les erreurs d'authentification
@app.errorhandler(401)
def unauthorized(error):
    return jsonify({
        'success': False,
        'message': 'Non autorisé',
        'error_code': 'UNAUTHORIZED'
    }), 401

@app.errorhandler(403)
def forbidden(error):
    return jsonify({
        'success': False,
        'message': 'Accès interdit',
        'error_code': 'FORBIDDEN'
    }), 403

# Gestionnaire d'erreur général
@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'message': 'Erreur interne du serveur',
        'error_code': 'INTERNAL_ERROR'
    }), 500




# Function to create tables
def create_tables_if_not_exist():
    inspector = inspect(db.engine)
    existing_tables = inspector.get_table_names()
    tables_to_create = [table for table in db.metadata.tables.keys() if table not in existing_tables]
    if tables_to_create:
        print("Creating tables")
        db.create_all()
    else:
        print("Tables are already created")

# Create tables
with app.app_context():
    create_tables_if_not_exist()

# Charger le modèle et les scalers
with open("C:/Users/Mark/Downloads/lstm_temperature_predict_v2.sav", "rb") as f:
    model = pickle.load(f)

with open("C:/Users/Mark/Downloads/scaler_temperature.sav", "rb") as f:
    scalers_ville = pickle.load(f)

app.config['model'] = model
app.config['scalers_ville'] = scalers_ville

#Initialisation de Redis 
try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.ping()
except redis.ConnectionError as e:
    r = None 

app.config['redis_client'] = r
# Run app
if __name__ == '__main__': 
    app.run(port=5001,debug=False, threaded=True, use_reloader=False)