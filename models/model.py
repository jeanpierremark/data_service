
from sqlalchemy import Integer, String, Enum, Time, Boolean, ForeignKey, Date, DateTime, ARRAY
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import relationship


db = SQLAlchemy()

#Define Enum

define_categorie = ("Descriptive","Comparative","Temporelle")


class MethodeAnalyse(db.Model):
    __tablename__='methode_analyses'
    id = db.Column(Integer, primary_key = True)
    nom = db.Column(String(100))
    description = db.Column(String(500))
    categorie = db.Column(Enum(*define_categorie, name="categorie"))
    parametres = db.Column(ARRAY(String(50)))
    zone = db.Column(ARRAY(String(50)))
    complexite = db.Column(String(50))
    user_id = db.Column(Integer, ForeignKey('users.id'))


class RapportAnalyse(db.Model):
    __tablename__='rapport_analyses'
    id = db.Column(Integer, primary_key = True)
    titre = db.Column(String(100))
    description = db.Column(String(500))
    creation = db.Column(DateTime)
    modification = db.Column(DateTime)
    resultat= db.Column(String(50))
    conclusion = db.Column(String(50))
    user_id = db.Column(Integer, ForeignKey('users.id'))

class RapportAnalyse(db.Model):
    __tablename__='visualisations'
    id = db.Column(Integer, primary_key = True)
    type = db.Column(String(100))
    image = db.Column(String(500))
    analyse_id = db.Column(Integer, ForeignKey('methode_analyses.id'))


