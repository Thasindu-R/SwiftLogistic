FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY services/mock-cms/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/mock-cms/app ./app

EXPOSE 8004

CMD ["sh", "-c", "uvicorn app.cms_service:app --host 0.0.0.0 --port ${PORT:-8004}"]
