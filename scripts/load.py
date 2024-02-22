import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
import os 

def conectar_snowflake():
    """Función para conectar a Snowflake"""
    conn = snowflake.connector.connect(
        user=os.environ['SNOWSQL_USER'],
        password=os.environ['SNOWSQL_PWD'],
        account=os.environ['SNOWSQL_ACCOUNT'],
        warehouse='COMPUTE_WH',
        database='SOURCE',
        schema='RAW'
    )
    return conn

def crear_tabla_subte(conn):
    """Crea la tabla 'subtedata' si no existe"""
    cur = conn.cursor()
    cur.execute("""
                DROP TABLE RAW.SUBTEDATA;
                CREATE TABLE IF NOT EXISTS subtedata (
                   id_linea VARCHAR(100) NOT NULL,
                    Route_Id VARCHAR(100) NOT NULL,
                    Direction_ID INT NOT NULL,
                    Direction_to  VARCHAR(200) NOT NULL,
                    start_date  DATE NOT NULL,
                    stop_name VARCHAR(200) NOT NULL,
                    arrival_time TIMESTAMP,
                    arrival_delay FLOAT,
                    departure_time TIMESTAMP,
                    departure_delay FLOAT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
    cur.close()

def cargar_datos_db_subte(conn, df):
    """Carga datos desde un DataFrame de pandas a la tabla 'subtedata' en Snowflake"""
    write_pandas(conn, df, 'SUBTEDATA')
    print("DataFrame cargado en Snowflake.")

def main():
    conn = conectar_snowflake()
    crear_tabla_subte(conn)
    
    # Asumiendo que 'subte_data.csv' es el archivo con los datos extraídos
    df = pd.read_csv('subte_data.csv')
    df.columns = df.columns.str.upper()

    cargar_datos_db_subte(conn, df)
    
    # Cerrar la conexión
    conn.close()

if __name__ == "__main__":
    main()

