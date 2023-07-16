FROM python:3.11

WORKDIR /usr/src/app

RUN pip install requests

COPY main.py .
COPY server.py .
COPY data/ ./data

CMD [ "python", "./main.py" ]
