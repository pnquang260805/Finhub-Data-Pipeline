FROM flink:2.1.0-scala_2.12-java21

USER root
RUN mkdir -p /opt/flink/jobs
WORKDIR /opt/flink/jobs
RUN chown -R flink:flink /opt/flink

RUN apt-get update && apt-get install -y python3 python3-pip
RUN ln -s /usr/bin/python3 /usr/bin/python
COPY ./requirements.txt .
RUN pip install -r ./requirements.txt


USER flink