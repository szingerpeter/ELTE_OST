FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install -y unzip vim curl wget gpg-agent expect \
    && apt-get install -y unzip vim curl wget dos2unix

RUN apt-get install -y --no-install-recommends software-properties-common

RUN wget -q https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.3_linux_amd64.tar.gz -O /tmp/influxdb.tar.gz \
    && tar -xzvf /tmp/influxdb.tar.gz -C /opt/ \
    && cp /opt/influxdb2-2.0.3_linux_amd64/influx /usr/local/bin/ \
    && cp /opt/influxdb2-2.0.3_linux_amd64/influxd /usr/local/bin/

RUN curl -s https://repos.influxdata.com/influxdb2.key | gpg --import -
RUN wget https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.3_linux_amd64.tar.gz.asc -O /tmp/influxdb.tar.gz.asc

RUN gpg --verify /tmp/influxdb.tar.gz.asc /tmp/influxdb.tar.gz

RUN mkdir -p /opt/app

COPY ./docker-entrypoint.sh /opt/app/docker-entrypoint.sh
RUN chmod +x /opt/app/docker-entrypoint.sh \
    && dos2unix /opt/app/docker-entrypoint.sh

CMD ["bash", "/opt/app/docker-entrypoint.sh"]

