FROM python:slim-bullseye

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY *.py .

HEALTHCHECK --interval=5s --timeout=3s --retries=3 CMD curl --fail http://localhost:3000/health || exit 1

EXPOSE 3000
ENTRYPOINT uvicorn main:app --port 3000 --host 0.0.0.0
