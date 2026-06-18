FROM apache/spark:latest

ENV SPARK_MASTER_URL=spark://100.100.10.103:7077

# evita problemas de hostname en workers también
ENV SPARK_LOCAL_IP=0.0.0.0

ENTRYPOINT []
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://100.100.10.103:7077"]