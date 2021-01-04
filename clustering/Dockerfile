FROM maven:3.6-jdk-8-slim AS builder

RUN apt-get update \
    && apt-get install -y unzip vim curl wget dos2unix

COPY . /opt/clustering
WORKDIR /opt/clustering
RUN mvn clean install

FROM flink:1.12

WORKDIR /opt/flink/bin

# Copy Click Count Job
COPY --from=builder /opt/clustering/target/project-*.jar /opt/clustering/project.jar

COPY docker-entrypoint.sh opt/clustering/docker-entrypoint.sh
COPY wait-for-it.sh opt/clustering/wait-for-it.sh

RUN chmod +x opt/clustering/docker-entrypoint.sh\
    && chmod +x opt/clustering/wait-for-it.sh\
    && dos2unix opt/clustering/docker-entrypoint.sh\
    && dos2unix opt/clustering/wait-for-it.sh

CMD ["bash", "opt/clustering/docker-entrypoint.sh"]