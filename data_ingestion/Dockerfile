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

RUN wget https://downloads.lightbend.com/scala/2.12.2/scala-2.12.2.tgz -O /tmp/scala.tgz \
    && tar -xvf /tmp/scala.tgz -C /opt/ \
    && ln -s /opt/scala-2.12.2 /usr/local/scala

ENV SCALA_HOME="/usr/local/scala"
ENV PATH="${SCALA_HOME}/bin:${PATH}"

RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
    && curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add \
    && apt-get -y update \
    && apt-get -y install sbt

RUN wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz -O /tmp/spark.tgz \
        && tar -xvf /tmp/spark.tgz -C /opt/ \
        && ln -s /opt/spark-3.0.1-bin-hadoop3.2/ /usr/local/spark 

ENV SPARK_HOME="/usr/local/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN wget https://downloads.apache.org/kafka/2.6.0/kafka_2.13-2.6.0.tgz -O /tmp/kafka.tgz \
    && tar -xvf /tmp/kafka.tgz -C /opt/ \
    && ln -s /opt/kafka_2.13-2.6.0/ /usr/local/kafka

ENV KAFKA_HOME="/usr/local/kafka"
ENV PATH="${KAFKA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/app \
    && mkdir /opt/app/mounted_data

VOLUME /opt/app/mounted_data

WORKDIR /opt/app

COPY . .

RUN pip3 install -r requirements.txt

RUN chmod +x docker-entrypoint.sh

VOLUME /opt/app/mounted_data/

CMD ["bash", "docker-entrypoint.sh"]
#sudo docker run --mount src=$(pwd)/data/,target=/opt/app/mounted_data/,type=bind,readonly -p 4040:4040 -it --name test test bash


#zookeeper-server-start.sh config/zookeeper.properties
#kafka-server-start.sh config/server.properties
#kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
#spark-shell --packages "org.apache.spark":"spark-sql-kafka-0-10_2.12":"3.0.1"




#ENV HADOOP_HOME="/opt/hadoop-2.7.1" 
#ENV PATH="${HADOOP_HOME}/bin:${PATH}"

#RUN wget http://apachemirror.wuchna.com/hive/hive-3.1.2/apache-#hive-3.1.2-bin.tar.gz -O /tmp/hive.tar.gz \
#    && tar -xvf /tmp/hive.tar.gz -C /opt/ \
#    && ln -s /opt/apache-hive_3.1.2-bin/ /usr/local/hive

#ENV HIVE_HOME="usr/local/hive"
#ENV PATH="${HIVE_HOME}/bin:${PATH}"

#RUN wget https://downloads.apache.org/kafka/2.6.0/kafka_2.13-2.6.0.tgz -O /tmp/kafka.tgz \
#    && tar -xvf /tmp/kafka.tgz -C /opt/ \
#    && ln -s /opt/kafka_2.13-2.6.0/ /usr/local/kafka

#ENV KAFKA_HOME="/usr/local/kafka"
#ENV PATH="${KAFKA_HOME}/bin:${PATH}"

#FROM angelcervera/docker-hadoop:2.7.1-single
