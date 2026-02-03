# Dockerfile for data ingestion
FROM python:3.13

RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app

ENTRYPOINT ["python", "ingest_data.py"]