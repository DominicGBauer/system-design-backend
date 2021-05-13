FROM python:3.9.5

COPY ./app/requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install -r requirements.txt

COPY ./app /app

CMD ["uwsgi", "--ini", "uwsgi.ini"]
