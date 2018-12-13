FROM python:3-alpine

WORKDIR /usr/src/app

ADD . /code/
RUN pip install -r /code/requirements-dev.txt -e /code

CMD [ "python" ]
