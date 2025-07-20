starting from root - if in windows use wsl

1. ./start_project.sh

now you can see all addresses:
- Spark Master: http://localhost:8080/
- Airflow: http://localhost:8083/
- MinIO: http://localhost:9001/

3. 
  a. docker exec -it processing-spark-master-1 bash  
  b. spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/jobs/seed_bronze.py  
  c. exit (before starting stage 4 - to exit the container path)

4. create new user in airflow - only once (unless you do rebuild)
   docker exec -it orchestration-airflow-webserver-1 airflow users create \
     --username user \
     --firstname userfirstname \
     --lastname userlastname \
     --email user@example.com \
     --role Admin \
     --password user_password

login to airflow:
  - username: user
  - password: user_password

login to MinIO:
  - username: minioadmin
  - password: minioadmin

to abort all processes after finishing all the tests do from root:
./stop_project.sh
