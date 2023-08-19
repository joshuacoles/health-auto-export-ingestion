FROM python:slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY *.py .

EXPOSE 3000
ENTRYPOINT uvicorn main:app --port 3000 --host 0.0.0.0
