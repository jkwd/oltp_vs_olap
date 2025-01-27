FROM python:3.9-slim

# PYTHONUNBUFFERED
ENV PYTHONUNBUFFERED 1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .