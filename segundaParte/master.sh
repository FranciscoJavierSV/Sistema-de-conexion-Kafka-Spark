docker run -d \
  --name spark-master \
  --hostname spark-master \
  --network host \
  -e PATH="/opt/spark/bin:/opt/spark/sbin:$PATH" \
  spark-nuevo:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master