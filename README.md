# Pronostico clima DD3

## Resumen 
Para consumir los datos del servicio web en Python, vamos a usar la biblioteca requests para hacer solicitudes HTTP a la API del servicio web `'https://smn.conagua.gob.mx/webservices/?method=1'` y recuperar los datos. Ademas, vamos a estar utilizando pandas para procesar los datos y crear las tablas de salida.

Luego, vamos a empaquetar todo en un contenedor de Docker para asegurarnos de que las dependencias y el entorno de ejecución estén configurados de manera consistente.

Finalmente, para orquestar y programar la tarea de forma periódica, vamos a utilizar Airflow. Crearemos un flujo de trabajo de Airflow que se encargue de la programación y la ejecución de la tarea de Python en un horario determinado.

## Implementacion

