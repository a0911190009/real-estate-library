FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD exec gunicorn --bind :${PORT:-8080} --workers 1 --threads 8 --worker-class gthread --timeout 120 app:app
