"""
Módulo de conexión a base de datos MySQL
"""

import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Cargar variables de entorno
load_dotenv()

def get_connection():
    """
    Crear conexión a MySQL usando pymysql
    """
    try:
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("✅ Conexión exitosa a la base de datos")
        return connection
    except Exception as e:
        print(f"❌ Error al conectar a la base de datos: {e}")
        return None

def get_engine():
    """
    Crear engine de SQLAlchemy para pandas
    """
    try:
        connection_string = (
            f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        engine = create_engine(connection_string)
        print("✅ Engine SQLAlchemy creado exitosamente")
        return engine
    except Exception as e:
        print(f"❌ Error al crear engine: {e}")
        return None

def query_to_dataframe(query, engine=None):
    """
    Ejecutar query y retornar DataFrame
    
    Parameters:
    -----------
    query : str
        Query SQL a ejecutar
    engine : SQLAlchemy engine, optional
        Engine de SQLAlchemy. Si no se provee, se crea uno nuevo
    
    Returns:
    --------
    pd.DataFrame
        Resultado del query
    """
    try:
        if engine is None:
            engine = get_engine()
        
        df = pd.read_sql(query, engine)
        print(f"✅ Query ejecutado exitosamente. Filas: {len(df)}")
        return df
    except Exception as e:
        print(f"❌ Error al ejecutar query: {e}")
        return None
