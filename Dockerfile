FROM apache/airflow:2.11.0
COPY requirements.txt /requirements.txt
#RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt