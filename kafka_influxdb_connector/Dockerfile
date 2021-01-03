FROM ubuntu:18.04

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN apt-get update \
    && apt-get install -y --no-install-recommends software-properties-common

RUN apt-get install -y python3-pip

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "./src/run.py"]