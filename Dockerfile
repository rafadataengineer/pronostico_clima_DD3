FROM python:3.9

# Copiar los archivos necesarios
COPY requirements.txt /app/requirements.txt
COPY dag.py /app/dag.py

# Instalar las dependencias
WORKDIR /app
RUN pip install --no-cache-dir -r requirements.txt

# Definir el comando de inicio
CMD ["airflow", "scheduler"]