# syntax=docker/dockerfile:1

FROM python:3.8.13

# create workdir
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "main.py"]
