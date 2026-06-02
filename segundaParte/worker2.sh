docker run -d \
  --name spark-worker-2 \
  --network kafka-redes-final \
  -p 8082:8081 \
  -e PATH="/opt/spark/bin:/opt/spark/sbin:$PATH" \
  spark-nuevo:latest \
  /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077