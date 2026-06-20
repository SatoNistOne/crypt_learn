FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY services/matching_engine/ ./services/matching_engine/

CMD ["uvicorn", "services.matching_engine.main:app", "--host", "0.0.0.0", "--port", "8000"]