FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY services/mock-wms/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/mock-wms/app ./app

EXPOSE 8006 9000

CMD ["sh", "-c", "uvicorn app.wms_service:app --host 0.0.0.0 --port ${PORT:-8006}"]
