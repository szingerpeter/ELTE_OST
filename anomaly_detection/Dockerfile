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

#RUN wget -q https://downloads.lightbend.com/scala/2.12.2/scala-2.12.2.tgz -O /tmp/scala.tgz \
#    && tar -xvf /tmp/scala.tgz -C /opt/ \
#    && ln -s /opt/scala-2.12.2 /usr/local/scala

#ENV SCALA_HOME="/usr/local/scala"
#ENV PATH="${SCALA_HOME}/bin:${PATH}"

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
    && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add \
    && apt-get -y update \
    && apt-get -y install sbt

RUN wget -q https://downloads.apache.org/kafka/2.6.0/kafka_2.13-2.6.0.tgz -O /tmp/kafka.tgz \
    && tar -xvf /tmp/kafka.tgz -C /opt/ \
    && ln -s /opt/kafka_2.13-2.6.0/ /usr/local/kafka

ENV KAFKA_HOME="/usr/local/kafka"
ENV PATH="${KAFKA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/app 
WORKDIR /opt/app

# new for anomaly detection, put them at last for rebuilding
RUN wget -q https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz -O /tmp/spark.tgz \
        && tar -xvf /tmp/spark.tgz -C /opt/ \
        && ln -s /opt/spark-3.0.1-bin-hadoop3.2/ /usr/local/spark

ENV SPARK_HOME="/usr/local/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN mkdir /opt/app/mounted_data
VOLUME /opt/app/mounted_data

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

RUN apt-get update \
    && apt-get install -y unzip vim curl wget dos2unix

RUN chmod +x docker-entrypoint.sh\
	&& dos2unix docker-entrypoint.sh
CMD ["bash", "docker-entrypoint.sh"]
