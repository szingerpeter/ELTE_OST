FROM cassandra:latest

RUN apt-get update \
  && apt-get install dos2unix

RUN mkdir -p /opt/app
WORKDIR /opt/app

COPY entrypoint-wrap.sh /entrypoint-wrap.sh
RUN chmod +x /entrypoint-wrap.sh \
  && dos2unix /entrypoint-wrap.sh

COPY . .

ENTRYPOINT ["/entrypoint-wrap.sh"]

CMD ["cassandra", "-f"]
