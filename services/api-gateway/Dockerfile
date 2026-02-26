FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY services/api-gateway/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY services/api-gateway/app ./app

EXPOSE 8000

CMD ["sh", "-c", "uvicorn app.api_gateway:app --host 0.0.0.0 --port ${PORT:-8000}"]