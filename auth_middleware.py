from flask import request, jsonify, current_app
from functools import wraps
import jwt as pyjwt
from datetime import datetime, timezone
from dotenv import load_dotenv
import os

load_dotenv() 
JWT_SECRET_KEY = os.getenv('SECRET_KEY')  # Corrigé: était 'SECRET_KEY'

class AuthMiddleware:
    """Classe pour gérer l'authentification et l'autorisation"""
    
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialiser l'application Flask avec le middleware"""
        self.app = app
        # Configuration par défaut
        app.config.setdefault('JWT_SECRET_KEY', os.getenv('JWT_SECRET_KEY', 'default-secret-key'))
        app.config.setdefault('JWT_ALGORITHM', 'HS256')
    
    def extract_token_from_header(self, auth_header):
        """Extraire le token du header Authorization"""
        if not auth_header:
            return None, "Header Authorization manquant"
        
        try:
            # Séparer "Bearer" du token
            parts = auth_header.split()
            if parts[0].lower() != 'bearer':
                return None, "Format du header invalide. Utilisez: Bearer <token>"
            
            if len(parts) != 2:
                return None, "Format du token invalide"
            
            return parts[1], None
        except Exception as e:
            return None, f"Erreur lors de l'extraction du token: {str(e)}"
    
    def decode_jwt_token(self, token):
        """Décoder et valider le token JWT"""
        try:
            # Utiliser la clé secrète depuis la variable d'environnement ou la config
            secret_key = JWT_SECRET_KEY or current_app.config.get('JWT_SECRET_KEY')
            
            if not secret_key:
                return None, "Clé secrète JWT non configurée"
            
            # Décoder le token
            payload = pyjwt.decode(
                token,
                secret_key,
                algorithms=[current_app.config.get('JWT_ALGORITHM', 'HS256')]
            )
            
            # Vérifier les champs obligatoires
            required_fields = ['user_id', 'exp']
            for field in required_fields:
                if field not in payload:
                    return None, f"Champ obligatoire manquant dans le token: {field}"
            
            # Vérifier si le token n'est pas expiré (avec timezone UTC)
            if 'exp' in payload:
                exp_timestamp = payload['exp']
                current_timestamp = datetime.now(timezone.utc).timestamp()
                if current_timestamp > exp_timestamp:
                    return None, "Token expiré"
            
            return payload, None
            
        except pyjwt.ExpiredSignatureError:
            return None, "Token expiré"
        except pyjwt.InvalidSignatureError:
            return None, "Signature du token invalide"
        except pyjwt.DecodeError:
            return None, "Token malformé"
        except pyjwt.InvalidTokenError as e:
            return None, f"Token invalide: {str(e)}"
        except Exception as e:
            return None, f"Erreur lors du décodage du token: {str(e)}"
    
    def get_user_info_from_token(self, token_payload):
        """Extraire les informations utilisateur du payload du token"""
        return {
            'id': token_payload.get('user_id'),
            'role': token_payload.get('role'),
            'exp': token_payload.get('exp')
        }
    
    def token_required(self, f):
        """Décorateur pour vérifier la présence et la validité du token JWT"""
        @wraps(f)
        def decorated(*args, **kwargs):
            # Récupérer l'en-tête Authorization
            auth_header = request.headers.get('Authorization')
            
            # Extraire le token
            token, error = self.extract_token_from_header(auth_header)
            if error:
                return jsonify({
                    'success': False,
                    'message': error,
                    'error_code': 'INVALID_AUTH_HEADER'
                }), 401
            
            # Décoder le token
            payload, error = self.decode_jwt_token(token)
            if error:
                return jsonify({
                    'success': False,
                    'message': error,
                    'error_code': 'TOKEN_VALIDATION_FAILED'
                }), 401
            
            # Ajouter les informations utilisateur à la requête
            request.current_user = self.get_user_info_from_token(payload)
            request.raw_token = token
            
            return f(*args, **kwargs)
        
        return decorated
    
    def role_required(self, allowed_roles):
        """Décorateur pour vérifier les rôles utilisateur"""
        if isinstance(allowed_roles, str):
            allowed_roles = [allowed_roles]
        
        def decorator(f):
            @wraps(f)
            def decorated(*args, **kwargs):
                # Vérifier si l'utilisateur est authentifié
                if not hasattr(request, 'current_user') or not request.current_user:
                    return jsonify({
                        'success': False,
                        'message': 'Authentification requise',
                        'error_code': 'AUTH_REQUIRED'
                    }), 401
                
                # Vérifier le rôle
                user_role = request.current_user.get('role', '').lower()
                allowed_roles_lower = [role.lower() for role in allowed_roles]
                
                if user_role not in allowed_roles_lower:
                    return jsonify({
                        'success': False,
                        'message': f'Accès refusé. Rôles autorisés: {", ".join(allowed_roles)}',
                        'error_code': 'INSUFFICIENT_PRIVILEGES',
                        'required_roles': allowed_roles,
                        'user_role': user_role
                    }), 403
                
                return f(*args, **kwargs)
            
            return decorated
        return decorator
    
    def permission_required(self, required_permissions):
        """Décorateur pour vérifier les permissions utilisateur"""
        if isinstance(required_permissions, str):
            required_permissions = [required_permissions]
        
        def decorator(f):
            @wraps(f)
            def decorated(*args, **kwargs):
                # Vérifier si l'utilisateur est authentifié
                if not hasattr(request, 'current_user') or not request.current_user:
                    return jsonify({
                        'success': False,
                        'message': 'Authentification requise',
                        'error_code': 'AUTH_REQUIRED'
                    }), 401
                
                # Récupérer les permissions de l'utilisateur
                user_permissions = request.current_user.get('permissions', [])
                
                # Vérifier si l'utilisateur a toutes les permissions requises
                missing_permissions = []
                for permission in required_permissions:
                    if permission not in user_permissions:
                        missing_permissions.append(permission)
                
                if missing_permissions:
                    return jsonify({
                        'success': False,
                        'message': f'Permissions manquantes: {", ".join(missing_permissions)}',
                        'error_code': 'INSUFFICIENT_PERMISSIONS',
                        'required_permissions': required_permissions,
                        'missing_permissions': missing_permissions
                    }), 403
                
                return f(*args, **kwargs)
            
            return decorated
        return decorator
    
    def optional_auth(self, f):
        """Décorateur pour une authentification optionnelle"""
        @wraps(f)
        def decorated(*args, **kwargs):
            auth_header = request.headers.get('Authorization')
            
            if auth_header:
                # Si un token est fourni, le valider
                token, error = self.extract_token_from_header(auth_header)
                if not error:
                    payload, decode_error = self.decode_jwt_token(token)
                    if not decode_error:
                        request.current_user = self.get_user_info_from_token(payload)
                        request.raw_token = token
            
            # Continuer même sans token valide
            if not hasattr(request, 'current_user'):
                request.current_user = None
            
            return f(*args, **kwargs)
        
        return decorated

# Instance globale du middleware
auth = AuthMiddleware()

# Décorateurs de convenance
def token_required(f):
    return auth.token_required(f)

def chercheur_required(f):
    return auth.role_required(['chercheur'])(f)

def admin_required(f):
    return auth.role_required('admin')(f)

def role_required(*roles):
    return auth.role_required(roles)

def permission_required(*permissions):
    return auth.permission_required(permissions)

def optional_auth(f):
    return auth.optional_auth(f)

# Fonctions utilitaires
def get_current_user():
    """Récupérer l'utilisateur actuel"""
    return getattr(request, 'current_user', None)

def get_current_user_id():
    """Récupérer l'ID de l'utilisateur actuel"""
    user = get_current_user()
    return user.get('id') if user else None

def get_current_user_role():
    """Récupérer le rôle de l'utilisateur actuel"""
    user = get_current_user()
    return user.get('role') if user else None

def get_current_user_permissions():
    """Récupérer les permissions de l'utilisateur actuel"""
    user = get_current_user()
    return user.get('permissions', []) if user else []

def is_authenticated():
    """Vérifier si l'utilisateur est authentifié"""
    return get_current_user() is not None

def has_role(role):
    """Vérifier si l'utilisateur a un rôle spécifique"""
    current_role = get_current_user_role()
    return current_role and current_role.lower() == role.lower()

def has_permission(permission):
    """Vérifier si l'utilisateur a une permission spécifique"""
    user_permissions = get_current_user_permissions()
    return permission in user_permissions

def get_token_info():
    """Récupérer toutes les informations du token décodé"""
    user = get_current_user()
    if not user:
        return None
    
    return {
        'user_id': user.get('id'),
        'role': user.get('role'),
        'permissions': user.get('permissions', []),
        'email': user.get('email'),
        'expires_at': datetime.fromtimestamp(user.get('exp'), tz=timezone.utc).isoformat() if user.get('exp') else None,
        'issued_at': datetime.fromtimestamp(user.get('iat'), tz=timezone.utc).isoformat() if user.get('iat') else None,
        'is_expired': False,  # Si on arrive ici, le token n'est pas expiré
        'raw_payload': user.get('full_payload', {})
    }