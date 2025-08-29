
from datetime import date
from sqlalchemy import Integer, String, Enum, Time, Boolean, ForeignKey, Date, DateTime, ARRAY
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship


db = SQLAlchemy()

#Define Enum
define_role = ("Admin","Chercheur","Etudiant")
define_genre = ("M","F")
define_categorie = ("Descriptive","Comparative","Tendance","Correlation")

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(Integer, primary_key=True)
    prenom = db.Column(String(100))
    nom = db.Column(String(100))
    email = db.Column(String(120), unique=True)
    password = db.Column(String(200))
    role = db.Column(Enum(*define_role, name="role"))
    genre = db.Column(Enum(*define_genre, name="genre"))
    age = db.Column(Integer)
    isActive = db.Column(Boolean, default = True)
    connected = db.Column(Boolean,default = False)
    inscription = db.Column(Date, default=date.today)  
    last_connexion = db.Column(DateTime)


class LogConnexion(db.Model):
    __tablename__='log_connexions'
    id = db.Column(Integer,primary_key = True)
    date_connexion = db.Column(DateTime)
    adresse_Ip = db.Column(String(50))
    resultat = db.Column(String(50))
    user_id = db.Column(Integer, ForeignKey('users.id',ondelete="CASCADE"))
    browser = db.Column(String(50))           
    browser_version = db.Column(String(20))  
    os = db.Column(String(50))               
    os_version = db.Column(String(20))       
    device = db.Column(String(50))          
    is_mobile = db.Column(Boolean, default=False)   
    is_tablet = db.Column(Boolean, default=False)   
    is_pc = db.Column(Boolean, default=False)       
    is_bot = db.Column(Boolean, default=False) 

class UserActivite(db.Model):
    __tablename__='user_activite'
    id = db.Column(Integer, primary_key = True)
    action = db.Column(String(100))
    date_action = db.Column(DateTime)
    statut = db.Column(Boolean)
    user_id = db.Column(Integer, ForeignKey('users.id'))

class MethodeAnalyse(db.Model):
    __tablename__='methode_analyses'
    id = db.Column(Integer, primary_key = True)
    nom = db.Column(String(100))
    description = db.Column(String(500))
    categorie = db.Column(Enum(*define_categorie, name="categorie"))
    parametres = db.Column(ARRAY(String(50)))
    zone = db.Column(ARRAY(String(50)))
    complexite = db.Column(String(50))
    user_id = db.Column(Integer, ForeignKey('users.id',ondelete="CASCADE"))


class RapportAnalyse(db.Model):
    __tablename__='rapport_analyses'
    id = db.Column(Integer, primary_key = True)
    titre = db.Column(String(100))
    description = db.Column(String(500))
    creation = db.Column(DateTime)
    modification = db.Column(DateTime)
    resultat= db.Column(String(50))
    conclusion = db.Column(String(50))
    user_id = db.Column(Integer, ForeignKey('users.id',ondelete="CASCADE"))

#class Visualisation(db.Model):
    #__tablename__='visualisations'
    #id = db.Column(Integer, primary_key = True)
    #type = db.Column(String(100))
    #image = db.Column(String(500))
    #analyse_id = db.Column(Integer, ForeignKey('methode_analyses.id'))


