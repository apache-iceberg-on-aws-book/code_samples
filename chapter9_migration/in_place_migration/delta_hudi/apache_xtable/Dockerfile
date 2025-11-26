FROM amazoncorretto:11

# Create directories for configs
RUN mkdir -p /opt/app/config

# Copy JAR and config files
COPY xtable-utilities_2.12-0.3.0-incubating-bundled.jar /opt/app/
COPY datasetConfig.yaml /opt/app/config/
COPY glueDataCatalog.yaml /opt/app/config/

# Set working directory
WORKDIR /opt/app

# Command to run the JAR
ENTRYPOINT ["java", "-jar", "xtable-utilities_2.12-0.3.0-incubating-bundled.jar", \
           "--datasetConfig", "config/datasetConfig.yaml", \
           "--icebergCatalogConfig", "config/glueDataCatalog.yaml"]