FROM python:3.11-slim

WORKDIR /app

COPY requirements/strategy-engine.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/strategy-engine/app /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]