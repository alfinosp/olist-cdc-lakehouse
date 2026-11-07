FROM python:3.11-slim-bookworm

# Spark runs on the JVM; pip's pyspark ships Spark itself.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Resolve the Kafka connector once at build time so containers start offline.
# Maven Central (behind Cloudflare) answers 403 to some networks; Google's mirror of
# Central is tried next, so the build works from either.
ENV SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    IVY_DIR=/opt/ivy \
    MAVEN_MIRROR=https://maven-central.storage-download.googleapis.com/maven2/
RUN python -c "from pyspark.sql import SparkSession as S; \
S.builder.master('local[1]').config('spark.jars.packages', '$SPARK_KAFKA_PACKAGE') \
.config('spark.jars.ivy', '$IVY_DIR').config('spark.jars.repositories', '$MAVEN_MIRROR') \
.getOrCreate().stop()" \
 && chmod -R a+rwX "$IVY_DIR"

# The container runs as your WSL user (AIRFLOW_UID, group 0). The JVM needs that
# user to have a passwd entry, which the entrypoint adds.
RUN chmod g+w /etc/passwd
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV HOME=/tmp PYTHONUNBUFFERED=1
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
