# syntax = docker/dockerfile:1.3
# VULN_SCAN_TIME=2024-09-16_05:07:05


# The build stage
# ---------------
# This stage is building Python wheels for use in later stages by using a base
# image that has more pre-requisites to do so, such as a C++ compiler.
#
# NOTE: If the image version is updated, also update it in ci/refreeze and
#       hub's Dockerfile!
#
FROM python:3.9-bullseye as build-stage

# Build wheels
#
# We set pip's cache directory and expose it across build stages via an
# ephemeral docker cache (--mount=type=cache,target=${PIP_CACHE_DIR}). We use
# the same technique for the directory /tmp/wheels.
#
COPY requirements.txt requirements.txt
ARG PIP_CACHE_DIR=/tmp/pip-cache
RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    pip install build \
 && pip wheel \
        --wheel-dir=/tmp/wheels \
        -r requirements.txt


# The final stage
# ---------------
#
FROM python:3.9-bullseye
ENV DEBIAN_FRONTEND=noninteractive

ENV NB_USER=jovyan \
    NB_UID=1000 \
    HOME=/home/jovyan

# Đặt các biến môi trường cho MinIO
ENV MINIO_ALIAS="minio_lake"
ENV MINIO_HOST="http://10.168.6.104:9000"
ENV MINIO_ACCESS_KEY="minioadmin"
ENV MINIO_SECRET_KEY="minioadmin"
ENV MINIO_BUCKET="code-spark"
ENV LOCAL_PATH="/home/jovyan"

RUN adduser \
        --disabled-password \
        --gecos "Default user" \
        --uid ${NB_UID} \
        --home ${HOME} \
        --force-badname \
        ${NB_USER}

RUN apt-get update \
 && apt-get upgrade --yes \
 && apt-get install --yes --no-install-recommends \
        ca-certificates \
        dnsutils \
        iputils-ping \
        tini \
        # requirement for nbgitpuller
        git \
        sudo \
        curl \
        vim \
        nano \
        telnet \
        unzip \
        openjdk-11-jdk \
        build-essential \
        software-properties-common \
        ssh && \
        apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# install wheels built in the build-stage
COPY requirements.txt /tmp/requirements.txt
ARG PIP_CACHE_DIR=/tmp/pip-cache
RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    --mount=type=cache,from=build-stage,source=/tmp/wheels,target=/tmp/wheels \
    pip install \
        --find-links=/tmp/wheels/ \
        -r /tmp/requirements.txt


# RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc \
#         --create-dirs \
#         -o $HOME/minio-binaries/mc

# RUN chmod +x $HOME/minio-binaries/mc
# RUN export PATH=$PATH:$HOME/minio-binaries/

RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x mc \
    && mv mc /usr/local/bin/


RUN python3 -m spylon_kernel install

RUN curl https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip -Lo ijava-1.3.0.zip \
  && unzip ijava-1.3.0.zip \
  && python3 install.py --sys-prefix \
  && rm ijava-1.3.0.zip 

# Optional env variables
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.5.1
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.4.2

# https://archive.apache.org/dist/spark/spark-3.5.1/
# Download spark
RUN mkdir -p ${SPARK_HOME} \
 && curl https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

 # Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download AWS bundle
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
 && unzip awscliv2.zip \
 && sudo ./aws/install \
 && rm awscliv2.zip \
 && rm -rf aws/

 # Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/spark/jars/*

RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse /home/iceberg/spark-events /home/iceberg
COPY notebooks/ /home/iceberg/notebooks

RUN mkdir -p /root/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY ipython/startup/README /root/.ipython/profile_default/startup

COPY spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*
COPY mysql-connector-j-8.3.0.jar /opt/spark/jars/
COPY .pyiceberg.yaml /root/.pyiceberg.yaml

RUN wget -O /opt/spark/jars/aws-java-sdk-s3-1.12.625.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.625/aws-java-sdk-s3-1.12.625.jar
RUN wget -O /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget -O /opt/spark/jars/jets3t-0.9.4.jar https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar
RUN wget -O /opt/spark/jars/aws-java-sdk-core-1.12.625.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.625/aws-java-sdk-core-1.12.625.jar
RUN wget -O /opt/spark/jars/aws-java-sdk-1.12.625.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.625/aws-java-sdk-1.12.625.jar
RUN wget -O /opt/spark/jars/aws-java-sdk-dynamodb-1.12.625.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.625/aws-java-sdk-dynamodb-1.12.625.jar


WORKDIR ${HOME}
USER ${NB_USER}

EXPOSE 8888
ENTRYPOINT ["tini", "--"]
CMD ["jupyter", "lab"]

# COPY entrypoint.sh .
# # RUN chmod +x entrypoint.sh
# ENTRYPOINT ["./entrypoint.sh"]
# CMD ["jupyter", "lab"]
