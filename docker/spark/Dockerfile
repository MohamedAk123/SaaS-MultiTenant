FROM apache/spark:3.5.6-python3
USER root
RUN python3 -m pip install --no-cache-dir jupyterlab==4.3.6 boto3==1.37.21 kafka-python==2.0.2
# Spark 3.5.6 ships Hadoop 3.3.4: keep the S3A connector and SDK aligned.
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/hadoop-aws-3.3.4.jar
ADD https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
ADD https://repo.maven.apache.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.6.jar
ADD https://repo.maven.apache.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar
ADD https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/kafka-clients-3.4.1.jar
ADD https://repo.maven.apache.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/commons-pool2-2.11.1.jar
ADD https://repo.maven.apache.org/maven2/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar /opt/spark/jars/delta-spark_2.12-3.3.2.jar
ADD https://repo.maven.apache.org/maven2/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar /opt/spark/jars/delta-storage-3.3.2.jar
ADD https://repos.spark-packages.org/graphframes/graphframes/0.8.4-spark3.5-s_2.12/graphframes-0.8.4-spark3.5-s_2.12.jar /opt/spark/jars/graphframes-0.8.4-spark3.5-s_2.12.jar
RUN python3 -m pip install --no-cache-dir numpy==1.24.4 nose==1.3.7
RUN chmod a+r /opt/spark/jars/*.jar && mkdir -p /workspace /opt/spark/events /home/spark && chown -R 185:0 /workspace /opt/spark/events /home/spark
ENV HOME=/home/spark PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/spark/jars/graphframes-0.8.4-spark3.5-s_2.12.jar:/workspace/src
ENV PATH=/opt/spark/bin:/opt/spark/sbin:$PATH
WORKDIR /workspace
USER 185
