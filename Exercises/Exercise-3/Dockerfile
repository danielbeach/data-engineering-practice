FROM python:3.9-slim

RUN pip install boto3

WORKDIR /app
COPY . /app

CMD ["python", "main.py"]