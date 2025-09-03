FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN apt-get update && apt-get install -y build-essential libffi-dev libssl-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y build-essential && apt-get autoremove -y

COPY . .

EXPOSE 8000

CMD ["uvicorn", "login_server:app", "--host", "0.0.0.0", "--port", "8000"]