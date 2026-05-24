FROM apache/airflow:3.2.1-python3.12

COPY --chown=airflow:root requirements.txt /requirements.txt
RUN /usr/local/bin/python3 -m pip install --no-cache-dir -r /requirements.txt
