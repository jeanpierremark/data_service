from flask import Flask, Blueprint, jsonify
from flask_cors import CORS
from sqlalchemy import inspect 
from config import Config
from routes.visiteur import visiteur_routes
from routes.chercheur import chercheur_routes

# Importer le middleware d'authentification
from auth_middleware import auth  # Assurez-vous que le chemin est correct

app = Flask(__name__)

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
app.register_blueprint(visiteur_routes, url_prefix='/api')
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

# Run app
if __name__ == '__main__':
    app.run(port=5001, debug=True)