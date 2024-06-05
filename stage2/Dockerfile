FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .


# Set proxy environment variables
ENV http_proxy=http://proxyparfil.si.fr.intraorange:8080
ENV https_proxy=http://proxyparfil.si.fr.intraorange:8080

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3.12", "producer.py", "completed_queries", "public.completed_queries"]
