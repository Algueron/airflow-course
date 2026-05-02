FROM apache/airflow:3.1.5

COPY requirements.txt /requirements.txt

# Install additional requirements
RUN pip install --no-cache-dir -r /requirements.txt
