## Jira Support Metrics (Jira Cloud -> Postgres)

### 1) Crear tablas
psql "postgresql://USER:PASS@HOST:PORT/DB" -f sql/001_tables.sql

### 2) Configurar .env
Copiar el .env y ajustar credenciales (Jira + Postgres).

### 3) Instalar y correr
python -m venv .venv
source .venv/bin/activate
pip install -e .
jira-support-metrics

### 4) Programar cada 10 minutos
Usa cron, Cloud Scheduler, etc.

Ejemplo cron (cada 10 min):
*/10 * * * * cd /ruta/jira-support-metrics && . .venv/bin/activate && jira-support-metrics >> run.log 2>&1
