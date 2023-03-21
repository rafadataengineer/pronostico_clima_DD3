# Pronostico clima DD3

## Resumen 
Para consumir los datos del servicio web en Python, vamos a usar la biblioteca requests para hacer solicitudes HTTP a la API del servicio web `'https://smn.conagua.gob.mx/webservices/?method=1'` y recuperar los datos. Ademas, vamos a estar utilizando pandas para procesar los datos y crear las tablas de salida.

Luego, vamos a empaquetar todo en un contenedor de Docker para asegurarnos de que las dependencias y el entorno de ejecución estén configurados de manera consistente.

Finalmente, para orquestar y programar la tarea de forma periódica, vamos a utilizar Airflow. Crearemos un flujo de trabajo de Airflow que se encargue de la programación y la ejecución de la tarea de Python en un horario determinado.

## Implementacion

1. Importar las librerias necesarias
```python
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
```

2. Definir la funcion `get_weather_data()`,la cual obtiene el último registro devuelto por el servicio de pronóstico por municipio y por hora.

    2.1. Declarar la variable con la URL del servicio web.
    ```python
    url = 'https://smn.conagua.gob.mx/webservices/?method=1'
    ```
    
   2.2. Realizar la solicitud HTTP
   ```python
   response = requests.get(url)
   ```
   
   2.3. Verificar si la solicitud fue exitosa. Luego, parsear la respuesta a formato JSON, obtener los datos del ultimo registro y devolver los datos como un diccionario.
   ```python
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
   ```

3. Definir la funcion `calculate_averages()`, la cual calcula el promedio de temperatura y precipitación de las últimas dos horas.

    3.1. Obtener la fecha y hora actual.
    ```python
    now = datetime.now()
    ```
    
    3.2. Calcular la fecha y hora de hace dos horas.
    ```python
    two_hours_ago = now - timedelta(hours=2)
    ```
    
    3.3. Filtrar los datos para las ultimas dos horas.
    ```python
    filtered_data = data[(data['fecha'] >= two_hours_ago) & (data['fecha'] <= now)]
    ```
    
    3.4. Calcular el promedio de temperatura y precipitacion
    ```python
    avg_temp = filtered_data['temperatura'].mean()
    avg_precip = filtered_data['precipitacion'].mean()
    ```
    
    3.5. Devolver los resultados como un diccionario.
    ```python
    return {
        'temperatura_promedio': avg_temp,
        'precipitacion_promedio': avg_precip
    }
    ```
    
4. Definimos la funcion `process_data()` la cual procesa los datos de clima y datos a nivel municipio para generar una tabla que los cruce.
    
    4.1. Obtener los datos del clima.
    ```python
    weather_data = get_weather_data()
    ```
    
    4.2. Cargar los datos a nivel municipio.
    ```python
     data_path = '/data_municipios'
     data = pd.read_csv(data_path)
    ```
    
    4.3. Obtener los promedios de las ultimas dos horas
    ```python
    averages = calculate_averages(data)
    ```
    
    4.4. Agregar los datos del clima a la tabla.
    ```python
    result = pd.DataFrame({
            'fecha': [datetime.now()],
            'municipio': [weather_data['municipio']],
            'temperatura': [weather_data['temperatura']],
            'precipitacion': [weather_data['precipitacion']],
            'temperatura_promedio': [averages['temperatura_promedio']],
            'precipitacion_promedio': [averages['precipitacion_promedio']]
        })
    ```
    
    4.5. Guardar la tabla en un archivo CSV.
    ```python
    version = datetime.now().strftime('%Y%m%d%H%M%S')
    result.to_csv(f'/data_procesada/result_{version}.csv', index=False)
    ```
    
    4.6. Generar la version "current" de la tabla.
    ```python
    result.to_csv('/data_procesada/result_current.csv', index=False)
    ```
    
5. Definir los argumentos del DAG.
```python
default_args = {
    'owner': 'data_science_team',
    'start_date': datetime(2023, 3, 20),
    'schedule_interval': '@hourly'
}
```

6. Crear el DAG.
```python
dag = DAG('weather_data', default_args=default_args)
```

7. Definir los operadores.
```python
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
```

8. Definir las dependencias.
```python
get_weather_data >> process_data
```

## ¿Qué mejoras propondrías a tu solución para siguientes versiones?

1. Parámetros de configuración: Es posible que algunos aspectos de la solución, como la frecuencia de actualización de los datos, necesiten ser configurados. Incluir parámetros de configuración permitirá ajustar la solución sin necesidad de modificar el código.

2. Pruebas automatizadas: Para asegurarse de que la solución funciona correctamente en todo momento, se podrían incluir pruebas automatizadas que validen la funcionalidad de la solución. Esto podría ayudar a detectar errores más rápidamente y garantizar la calidad de la solución.

3. Gestión de dependencias: Si la solución tiene muchas dependencias, es importante asegurarse de que todas estén instaladas y actualizadas. Para hacer esto más fácilmente, se podrían utilizar herramientas como pip o Anaconda.

## ¿Qué aspectos o herramientas considerarías para escalar, organizar y automatizar tu solución?

1. Almacenamiento de datos: Si los datos son grandes o deben ser accesibles por múltiples usuarios, se podría utilizar un sistema de almacenamiento de datos como Apache Hadoop o AWS S3. Esto permitiría escalar la solución para manejar grandes cantidades de datos y hacer que los datos sean más accesibles.

2. Monitoreo: Para asegurarse de que la solución funciona correctamente en todo momento, es importante monitorear su desempeño y detectar problemas en tiempo real.

3. Orquestador de tareas: Para coordinar las diferentes tareas necesarias para obtener y procesar los datos, se podría utilizar un orquestador de tareas como Apache Airflow. Esto permitiría definir y programar las tareas necesarias y controlar su ejecución.
