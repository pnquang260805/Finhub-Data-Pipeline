FROM flink:1.20.2-java11

USER root
USER flink
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_KEY=password

RUN mkdir -p /opt/flink/usrlib
RUN mkdir -p /opt/flink/jobs
RUN wget -P /opt/flink/lib https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.1.0/kafka-clients-4.1.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-hive-2.3.10_2.12/1.20.2/flink-sql-connector-hive-2.3.10_2.12-1.20.2.jar \
    # AWS SDK Bundle
    # https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.38.5/bundle-2.38.5.jar \
    # For Hadoop
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/2.8.3/hadoop-common-2.8.3.jar \
    # Cái shaded này chứa toàn bộ các thư viện cần của hadoop
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    # ======================= 
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.20.2/flink-metrics-prometheus-1.20.2.jar \
    # Iceberg
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.0/iceberg-flink-runtime-1.20-1.7.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.6.1/iceberg-nessie-1.6.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.0/iceberg-aws-bundle-1.7.0.jar
    
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && wget -P /opt/flink/plugins/s3-fs-hadoop \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.2/flink-s3-fs-hadoop-1.20.2.jar

CMD ["./bin/start-cluster.sh"]