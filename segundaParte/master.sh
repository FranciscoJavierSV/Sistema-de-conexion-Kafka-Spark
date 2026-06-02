docker run -d \
  --name spark-master \
  --hostname spark-master \
  --network kafka-redes-final \
  -p 8080:8080 \
  -p 7077:7077 \
  -e PATH="/opt/spark/bin:/opt/spark/sbin:$PATH" \
  spark-nuevo:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master