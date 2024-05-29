# project-root/spark-streaming/Dockerfile

FROM bitnami/spark:latest

COPY sparkS.py /app/sparkS.py

ENTRYPOINT ["/opt/bitnami/scripts/spark/entrypoint.sh"]
CMD ["spark-submit", "/app/sparkS.py"]
