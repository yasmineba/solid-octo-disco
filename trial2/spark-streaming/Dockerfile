FROM bitnami/spark:latest
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["spark-submit", "--master", "local[*]", "SparkS.py"]
