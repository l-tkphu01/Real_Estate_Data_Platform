FROM python:3.11-slim

WORKDIR /opt/dagster/app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
