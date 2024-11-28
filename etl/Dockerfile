FROM python:3.10-slim

WORKDIR /opt/app

COPY requirements.txt requirements.txt

RUN  pip install --upgrade pip \
     && pip install -r requirements.txt

COPY . .

CMD ["python", "-u", "./main.py"]