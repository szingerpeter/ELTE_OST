FROM cassandra:latest

RUN mkdir -p /opt/app
WORKDIR /opt/app

COPY entrypoint-wrap.sh /entrypoint-wrap.sh
RUN chmod +x /entrypoint-wrap.sh

COPY . .

ENTRYPOINT ["/entrypoint-wrap.sh"]

CMD ["cassandra", "-f"]
