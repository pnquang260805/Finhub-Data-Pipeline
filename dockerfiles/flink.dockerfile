FROM flink:1.20.2-java11

RUN mkdir -p /opt/flink/usrlib
RUN mkdir -p /opt/flink/jobs
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.1.0/kafka-clients-4.1.0.jar \
    # Hudi + flink jar
    https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink-bundle_2.12/0.10.1/hudi-flink-bundle_2.12-0.10.1.jar \
    # Flink s3
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.2/flink-s3-fs-hadoop-1.20.2.jar  \
    # Flink Prometheus
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.20.2/flink-metrics-prometheus-1.20.2.jar

USER root

RUN apt-get update -y && \
    apt-get install python3 python3-pip -y
RUN ln -s /usr/bin/python3 /usr/bin/python
COPY ./requirements.txt .
RUN pip install -r ./requirements.txt --no-cache-dir


USER flink