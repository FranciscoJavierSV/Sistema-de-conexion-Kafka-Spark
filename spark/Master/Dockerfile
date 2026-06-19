FROM apache/spark:latest

ENV SPARK_MASTER_HOST=100.100.10.100
ENV SPARK_MASTER_PORT=7077
ENV SPARK_MASTER_WEBUI_PORT=8080

ENV SPARK_LOCAL_IP=100.100.10.100
ENV SPARK_PUBLIC_DNS=100.100.10.100

ENV HOME=/tmp
ENV SPARK_LOCAL_DIRS=/tmp/spark-local

RUN mkdir -p /tmp/.ivy2 \
    && mkdir -p /tmp/spark-local \
    && mkdir -p /tmp/output \
    && mkdir -p /tmp/checkpoints

EXPOSE 7077
EXPOSE 8080

ENTRYPOINT []

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]