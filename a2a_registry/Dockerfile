FROM python:3.10
WORKDIR /app
COPY requirements.txt .
RUN pip install --default-timeout=100 --retries 5 --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
# Ensure the SQLite file is stored in a persistent volume if needed (for demo, local file is fine)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-config", "logging_config.yml"]
