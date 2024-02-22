import requests
import pandas as pd

def get_data():
    endpoint_subte = "https://apitransporte.buenosaires.gob.ar/subtes/forecastGTFS?client_id=eeea2fd521514498a37629a810012185&client_secret=14C6598f35E2498185685Ccfc6b2b372"

    try:
        response = requests.get(endpoint_subte)
        if response.status_code == 200:
            return response.json()
        else:
            print("Error al hacer la petición a la API:", response.status_code)
            return None
    except Exception as e:
        print("Error al hacer la petición a la API:", str(e))
        return None

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

def create_df(data):
    col = ['id_linea', 'Route_Id', 'Direction_ID', 'start_date', 'stop_name', 'arrival_time', 'arrival_delay', 'departure_time', 'departure_delay']
    df = pd.DataFrame(data, columns=col)
    return df

def procesar_data():
    response = get_data()
    if response:
        datos = []
        for linea in response['Entity']:
            datos.extend(get_forecast(linea))
        df = create_df(datos)

        df['Direction_to'] = df['Direction_ID'].map({1: 'Inbound', 0: 'Outbound'})
        df['Direction_to'] = df.groupby('id_linea')['stop_name'].transform('last')
        new_order_columns = ['id_linea', 'Route_Id', 'Direction_ID', 'Direction_to', 'start_date', 'stop_name', 'arrival_time', 'arrival_delay', 'departure_time', 'departure_delay']
        df = df[new_order_columns]

        df['start_date'] = pd.to_datetime(df['start_date'], format='%Y%m%d')
        df['arrival_time'] = pd.to_datetime(df['arrival_time'], unit='s')
        df['departure_time'] = pd.to_datetime(df['departure_time'], unit='s')

        df['arrival_delay'] = df['arrival_delay'] / 60
        df['departure_delay'] = df['departure_delay'] / 60

        df.to_csv('subte_data.csv', index=False)
        print("Datos extraídos y guardados en 'subte_data.csv'.")
    else:
        print("No se pudieron obtener los datos.")

procesar_data()
