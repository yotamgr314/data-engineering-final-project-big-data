FROM bitnami/spark:3.3

USER 0

RUN apt-get update -qq && apt-get install -y --no-install-recommends curl python3-pip \
 && pip install --no-cache-dir faker==24.11.0 pyspark boto3 pandas \
 && curl -fsSL -o /opt/bitnami/spark/jars/iceberg-runtime.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar \
 && chown -R 1001:0 /opt/bitnami/spark/jars \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER 1001