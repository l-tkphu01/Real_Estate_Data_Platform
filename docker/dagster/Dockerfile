FROM python:3.11-slim

WORKDIR /opt/dagster/app

# Cài đặt Java (JRE) để chạy PySpark an toàn không sập máy
RUN apt-get update && apt-get install -y default-jre && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Cài PySpark từ file LOCAL (đã tải sẵn, không cần internet nữa!)
COPY docker/dagster/wheels/pyspark-3.5.0.tar.gz /tmp/pyspark-3.5.0.tar.gz
RUN pip install --no-cache-dir /tmp/pyspark-3.5.0.tar.gz && rm /tmp/pyspark-3.5.0.tar.gz

# Cài các thư viện còn lại (nhẹ, nhanh)
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries=3 -r requirements.txt

COPY . .

# Copy cấu hình Cloud vào container (thay thế cho việc nhập tay trên Azure Portal)
COPY .env.cloud .env

# Tạo thư mục DAGSTER_HOME cho Cloud (Local dùng Docker Volume nên không cần)
RUN mkdir -p /opt/dagster/dagster_home

# Lệnh khởi động mặc định khi container bật lên
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
