FROM python:3.11-slim

WORKDIR /opt/dagster/app

# Cài đặt Java (JRE) để chạy PySpark an toàn không sập máy
RUN apt-get update && apt-get install -y default-jre && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
