FROM python:3-alpine

WORKDIR /usr/src/app

COPY requirements-dev.txt /tmp/
RUN pip install -r /tmp/requirements-dev.txt && rm /tmp/requirements-dev.txt

ADD . /code/

ENV PYTHONPATH=$PYTHONPATH:/code

CMD [ "python" ]
