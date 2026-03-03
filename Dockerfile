FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copia el dispatcher
COPY run.py ./run.py

# Copia el código
COPY src/ ./src/
ENV PYTHONPATH=/app/src

# Entry + default task (puedes cambiarla al ejecutar)
ENTRYPOINT ["python", "run.py"]
CMD ["metricas_soporte"]