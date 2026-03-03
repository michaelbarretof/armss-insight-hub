FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Código
COPY src/ ./src/

# Para que "src/" esté en el path
ENV PYTHONPATH=/app/src

# Ejecuta el ETL
CMD ["python", "-m", "support_metrics.main"]