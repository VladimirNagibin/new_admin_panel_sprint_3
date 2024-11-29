FROM python:3.11-slim

WORKDIR /etl

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt

COPY . .

CMD ["python", "-u", "./main.py"]