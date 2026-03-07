FROM python:3.11-slim

WORKDIR /app

COPY requirements/market-data-service.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/market-data-service/app /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]