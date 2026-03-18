FROM python:3.11-slim

WORKDIR /app

# 安裝 antiword（.doc → 純文字，供雲端 Word 物件總表解析使用）
RUN apt-get update && apt-get install -y --no-install-recommends antiword && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD exec gunicorn --bind :${PORT:-8080} --workers 1 --threads 8 --worker-class gthread --timeout 120 app:app
