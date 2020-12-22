FROM ubuntu:18.04

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN apt-get update \
    && apt-get install -y unzip vim curl wget

RUN apt-get install -y --no-install-recommends software-properties-common

RUN apt-get install -y python3-pip

RUN add-apt-repository -y ppa:openjdk-r/ppa \
    && apt-get update \
    && apt-get install -y openjdk-8-jdk openjdk-8-jre

ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN wget -q https://downloads.lightbend.com/scala/2.12.2/scala-2.12.2.tgz -O /tmp/scala.tgz \
    && tar -xvf /tmp/scala.tgz -C /opt/ \
    && ln -s /opt/scala-2.12.2 /usr/local/scala

ENV SCALA_HOME="/usr/local/scala"
ENV PATH="${SCALA_HOME}/bin:${PATH}"

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
    && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add \
    && apt-get -y update \
    && apt-get -y install sbt

RUN wget -q https://ftp.cixug.es/apache/flink/flink-1.12.0/flink-1.12.0-bin-scala_2.12.tgz -O /tmp/flink.tgz \
    && tar -xvf /tmp/flink.tgz -C /opt/ \
    && ln -s /opt/flink-1.12.0/ /usr/local/flink

ENV FLINK_HOME="/usr/local/flink"
ENV PATH="${FLINK_HOME}/bin:${PATH}"

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

RUN chmod +x docker-entrypoint.sh

CMD ["bash", "docker-entrypoint.sh"]