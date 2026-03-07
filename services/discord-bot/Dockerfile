FROM python:3.11-slim

WORKDIR /app

COPY requirements/discord-bot.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY services/discord-bot/app /app

CMD ["python", "bot.py"]