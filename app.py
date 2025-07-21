from flask import Flask,Blueprint
from flask_cors import CORS
from sqlalchemy import inspect 
from config import Config
from routes.visiteur import visiteur_routes
from routes.chercheur import chercheur_routes


app = Flask(__name__)

@app.route("/")
def home():
    return "Bienvenue sur votre application Flask !"

CORS(app, origins=["http://localhost:4200"], methods=["GET", "POST", "PUT", "DELETE"], supports_credentials=True)



# Configurations and initializations
app.config.from_object(Config)


#Register routes 
app.register_blueprint(visiteur_routes,url_prefix='/api')
app.register_blueprint(chercheur_routes,url_prefix='/api')


#Run app
if __name__ == '__main__':
    app.run(port=5001, debug=True)
