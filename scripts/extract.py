import requests
import pandas as pd
from datetime import datetime
import sqlalchemy as sa
import warnings
# warnings.filterwarnings("ignore", category=sa.exc.RemovedIn20Warning)warnings.filterwarnings("ignore", category=sa.exc.MovedIn20Warning)
warnings.filterwarnings("ignore", category=sa.exc.MovedIn20Warning)


# 
def get_data():
    endpoint_subte = "https://apitransporte.buenosaires.gob.ar/subtes/forecastGTFS?client_id=eeea2fd521514498a37629a810012185&client_secret=14C6598f35E2498185685Ccfc6b2b372"

    try:
        response = requests.get(endpoint_subte).json()
    except Exception as e:
        print(e)

    return response

# Obtenemos todos los datos que seleccionamos.
def get_forecast(linea):
    id_linea = linea['ID']
    Route_Id = linea['Linea']['Route_Id']
    Direction_ID = linea['Linea']['Direction_ID']
    start_date = linea['Linea']['start_date']

    forecasts = []
    for estacion in linea['Linea']['Estaciones']:
        stop_name = estacion['stop_name']
        arrival_time = estacion['arrival']['time']
        arrival_delay = estacion['arrival']['delay']
        departure_time = estacion['departure']['time']
        departure_delay = estacion['departure']['delay']

        forecasts.append([id_linea, Route_Id, Direction_ID, start_date, stop_name, arrival_time, arrival_delay, departure_time, departure_delay])

    return forecasts

# Creamos un dataFrame
def create_df(data):
    col = ['id_linea', 'Route_Id', 'Direction_ID', 'start_date', 'stop_name', 'arrival_time', 'arrival_delay', 'departure_time', 'departure_delay']
    df = pd.DataFrame(data, columns=col)
    return df

def procesar_data():
    # obtenemos el json
    response = get_data()

    # Recomoremos el json desde la clave ['Entity'] llamamos a la funcione get_forecast() la obtener los datos seleccionados en la función 
    # y los guardamos en la lista  datos[]
    datos = []
    for linea in response['Entity']:
        datos.extend(get_forecast(linea))
    # Creamos el dataframe
    df = create_df(datos)


    # Crear una nueva columna Direction_to para darle nombre hacia que direccón va el tren.
    df['Direction_to'] = df['Direction_ID'].map({1: 'Inbound', 0: 'Outbound'})

    # Agrupar por id_linea y actualizar Direction_to con el último stop_name
    df['Direction_to'] = df.groupby('id_linea')['stop_name'].transform('last')
    new_order_columns = ['id_linea', 'Route_Id', 'Direction_ID', 'Direction_to', 'start_date', 'stop_name', 'arrival_time', 'arrival_delay', 'departure_time', 'departure_delay']
    df = df[new_order_columns]

    # Convertir start_date a formato de fecha
    df['start_date'] = pd.to_datetime(df['start_date'], format='%Y%m%d')

    # Convertir arrival_time y departure_time a formato de fecha y hora
    df['arrival_time'] = pd.to_datetime(df['arrival_time'], unit='s')
    df['departure_time'] = pd.to_datetime(df['departure_time'], unit='s')

    # Cambiar formato de arrival_delay y departure_delay a minutos 
    df['arrival_delay'] = df['arrival_delay'] / 60
    df['departure_delay'] = df['departure_delay'] / 60
  
    if response.status_code == 200:    
    # Guardar los datos en un archivo CSV
    with open('subte_data.csv', 'w') as file:
        file.write(response.text)
    
        print("Datos extraídos y guardados en 'users_data.csv'.")
    else:
        print("Error al hacer la petición a la API:", response.status_code)
      
  
procesar_data()     

