FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN pip install --no-cache-dir "psycopg2-binary>=2.9,<3"

COPY . .

CMD ["python", "-m", "carga"]
