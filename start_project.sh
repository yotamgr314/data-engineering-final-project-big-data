set -e

echo "Creating shared Docker network"
docker network inspect bigdata-net >/dev/null 2>&1 || docker network create bigdata-net

echo "Starting Kafka + Producers"
cd streaming
docker-compose up -d
cd ..

echo "Starting Spark + MinIO"
cd processing
docker-compose up -d
cd ..

echo "Starting Airflow..."
cd orchestration
docker-compose up -d
cd ..

echo "All components started successfully!"
echo "Spark Master : http://localhost:8080/"
