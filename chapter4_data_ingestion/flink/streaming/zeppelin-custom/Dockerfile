FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV ZEPPELIN_HOME=/opt/zeppelin
ENV FLINK_HOME=/opt/flink

# Install dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Zeppelin
RUN wget -O /tmp/zeppelin.tgz https://dlcdn.apache.org/zeppelin/zeppelin-0.11.0/zeppelin-0.11.0-bin-all.tgz \
    && tar -xzf /tmp/zeppelin.tgz -C /opt \
    && mv /opt/zeppelin-0.11.0-bin-all /opt/zeppelin \
    && rm /tmp/zeppelin.tgz

# Download and install Flink 1.17.2 for local execution
RUN wget -O /tmp/flink.tgz "https://archive.apache.org/dist/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz" \
    && tar -xzf /tmp/flink.tgz -C /opt \
    && mv /opt/flink-1.17.2 /opt/flink \
    && rm /tmp/flink.tgz

# Download and install Hadoop 3.3.6 (compatible with Flink 1.17.2)
RUN wget -O /tmp/hadoop.tgz "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz" \
    && tar -xzf /tmp/hadoop.tgz -C /opt \
    && mv /opt/hadoop-3.3.6 /opt/hadoop \
    && rm /tmp/hadoop.tgz

# Download required Flink JARs that we tested and confirmed working
RUN wget -O /opt/flink/lib/flink-sql-client-1.17.2.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-client/1.17.2/flink-sql-client-1.17.2.jar" \
    && wget -O /opt/flink/lib/flink-table-api-scala-bridge_2.12-1.17.2.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-table-api-scala-bridge_2.12/1.17.2/flink-table-api-scala-bridge_2.12-1.17.2.jar"

# Download Iceberg Flink runtime JAR for Flink 1.17 (latest supported version is 1.6.1)
RUN wget -O /opt/flink/lib/iceberg-flink-runtime-1.17-1.6.1.jar \
    "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-flink-runtime-1.17/1.6.1/iceberg-flink-runtime-1.17-1.6.1.jar"

# Download Flink Kafka connector JAR for Flink 1.17.2
RUN wget -O /opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar"

# Download aws bundle
RUN wget -O /opt/flink/lib/bundle-2.31.78.jar \
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.31.78/bundle-2.31.78.jar"

# Add Hadoop core libraries to Flink lib directory for Iceberg
RUN cp /opt/hadoop/share/hadoop/common/hadoop-common-3.3.6.jar /opt/flink/lib/ \
    && cp /opt/hadoop/share/hadoop/common/hadoop-auth-3.3.6.jar /opt/flink/lib/ \
    && cp /opt/hadoop/share/hadoop/common/lib/commons-configuration2-2.8.0.jar /opt/flink/lib/ \
    && cp /opt/hadoop/share/hadoop/common/lib/commons-lang3-3.12.0.jar /opt/flink/lib/ \
    && cp /opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar /opt/flink/lib/ \
    && cp /opt/hadoop/share/hadoop/common/lib/slf4j-api-1.7.30.jar /opt/flink/lib/ || true

# Set environment variables
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=$PATH:$FLINK_HOME/bin:$HADOOP_HOME/bin


# Copy configuration files
COPY conf/interpreter.json $ZEPPELIN_HOME/conf/
COPY conf/zeppelin-env.sh $ZEPPELIN_HOME/conf/
COPY conf/interpreter-list $ZEPPELIN_HOME/conf/
COPY start.sh $ZEPPELIN_HOME/start.sh

# Create notebook directory
RUN mkdir -p $ZEPPELIN_HOME/notebook

RUN chmod +x $ZEPPELIN_HOME/start.sh

WORKDIR $ZEPPELIN_HOME

EXPOSE 8080

CMD ["/opt/zeppelin/start.sh"]