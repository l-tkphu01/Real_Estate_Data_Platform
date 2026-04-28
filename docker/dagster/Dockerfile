FROM python:3.11-slim

WORKDIR /opt/dagster/app

# Cài đặt Java (JRE) để chạy PySpark an toàn không sập máy
RUN apt-get update && apt-get install -y default-jre && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Copy cấu hình Cloud vào container (thay thế cho việc nhập tay trên Azure Portal)
COPY .env.cloud .env

# Tạo thư mục DAGSTER_HOME cho Cloud (Local dùng Docker Volume nên không cần)
RUN mkdir -p /opt/dagster/dagster_home

# Lệnh khởi động mặc định khi container bật lên
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
