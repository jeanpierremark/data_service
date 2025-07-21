from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
import urllib
from dotenv import load_dotenv
import os

load_dotenv()


# Pour authentification Windows, supprime username/password et mets ``
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('SERVER')};"
    f"DATABASE={os.getenv('DB')};"
    f"trusted_connection=yes",
)

# Engine SQLAlchemy
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
