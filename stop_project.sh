set -e

echo "Stopping Airflow"
cd orchestration
docker-compose down
cd ..

echo "Stopping Spark + MinIO"
cd processing
docker-compose down
cd ..

echo "Stopping Kafka + Producers"
cd streaming
docker-compose down
cd ..

echo "All components stopped!"