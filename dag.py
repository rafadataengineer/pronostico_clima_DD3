import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_weather_data():
    """Obtiene el último registro devuelto por el servicio de pronóstico por municipio y por hora."""
    
    # URL del servicio web
    url = 'https://smn.conagua.gob.mx/webservices/?method=1'
    
    # Realizar la solicitud HTTP
    response = requests.get(url)
    
    # Verificar si la solicitud fue exitosa
    if response.status_code == 200:
        # Parsear la respuesta a formato JSON
        json_data = response.json()
        
        # Obtener los datos del último registro
        last_record = json_data['pronostico_horario_municipios'][-1]
        
        # Devolver los datos como un diccionario
        return {
            'municipio': last_record['nombre'],
            'temperatura': float(last_record['temperatura']),
            'precipitacion': float(last_record['precipitacion'])
        }
    else:
        # Si la solicitud falla, devolver un valor nulo
        return None

def calculate_averages(data):
    """Calcula el promedio de temperatura y precipitación de las últimas dos horas."""
    
    # Obtener la fecha y hora actual
    now = datetime.now()
    
    # Calcular la fecha y hora de hace dos horas
    two_hours_ago = now - timedelta(hours=2)
    
    # Filtrar los datos para las últimas dos horas
    filtered_data = data[(data['fecha'] >= two_hours_ago) & (data['fecha'] <= now)]
    
    # Calcular el promedio de temperatura y precipitación
    avg_temp = filtered_data['temperatura'].mean()
    avg_precip = filtered_data['precipitacion'].mean()
    
    # Devolver los resultados como un diccionario
    return {
        'temperatura_promedio': avg_temp,
        'precipitacion_promedio': avg_precip
    }

def process_data():
    """Procesa los datos de clima y datos a nivel municipio para generar una tabla que los cruce."""
    
    # Obtener los datos del clima
    weather_data = get_weather_data()
    
    if weather_data is not None:
        # Cargar los datos a nivel municipio
        data_path = '/data_municipios'
        data = pd.read_csv(data_path)
        
        # Obtener los promedios de las últimas dos horas
        averages = calculate_averages(data)
        
        # Agregar los datos del clima a la tabla
        result = pd.DataFrame({
            'fecha': [datetime.now()],
            'municipio': [weather_data['municipio']],
            'temperatura': [weather_data['temperatura']],
            'precipitacion': [weather_data['precipitacion']],
            'temperatura_promedio': [averages['temperatura_promedio']],
            'precipitacion_promedio': [averages['precipitacion_promedio']]
        })
        
        # Guardar la tabla en un archivo CSV
        version = datetime.now().strftime('%Y%m%d%H%M%S')
        result.to_csv(f'/data_procesada/result_{version}.csv', index=False)
        
        # Generar la versión "current" de la tabla
        result.to_csv('/data_procesada/result_current.csv', index=False)


# Definir los argumentos del DAG
default_args = {
    'owner': 'data_science_team',
    'start_date': datetime(2023, 3, 20),
    'schedule_interval': '@hourly'
}

# Crear el DAG
dag = DAG('weather_data', default_args=default_args)

# Definir los operadores
get_weather_data = PythonOperator(
    task_id='get_weather_data',
    python_callable=get_weather_data,
    dag=dag
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)

# Definir las dependencias
get_weather_data >> process_data