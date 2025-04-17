# Dockerfile
FROM apache/airflow:2.10.5

USER root

USER airflow

# Copy all your source code into the image
COPY . /opt/airflow

# Install Python dependencies
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt