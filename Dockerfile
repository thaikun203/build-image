# syntax = docker/dockerfile:1.3

FROM python:3.11-bookworm AS build-stage

COPY requirements.txt requirements.txt
ARG PIP_CACHE_DIR=/tmp/pip-cache
RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    apt-get update && apt-get install -y --no-install-recommends \
        default-libmysqlclient-dev pkg-config build-essential \
 && pip install build \
 && pip wheel --wheel-dir=/tmp/wheels -r requirements.txt


FROM python:3.11-bookworm
ENV DEBIAN_FRONTEND=noninteractive

ENV NB_USER=jovyan \
    NB_UID=1000 \
    HOME=/home/jovyan

RUN adduser \
        --disabled-password \
        --gecos "Default user" \
        --uid ${NB_UID} \
        --home ${HOME} \
        ${NB_USER} \
 && usermod -aG root ${NB_USER}

RUN apt-get update \
 && apt-get upgrade --yes \
 && apt-get install --yes --no-install-recommends \
        ca-certificates \
        dnsutils \
        iputils-ping \
        tini \
        git \
        sudo \
        curl \
        wget \
        vim \
        nano \
        telnet \
        unzip \
        libmariadb3 \
        ssh \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Node.js 20 cho jupyter lab build
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && npm install -g yarn \
 && ln -sf $(which yarn) /usr/local/bin/jlpm \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && node --version && npm --version

COPY requirements.txt /tmp/requirements.txt
ARG PIP_CACHE_DIR=/tmp/pip-cache
RUN --mount=type=cache,target=${PIP_CACHE_DIR} \
    --mount=type=cache,from=build-stage,source=/tmp/wheels,target=/tmp/wheels \
    pip install --find-links=/tmp/wheels/ -r /tmp/requirements.txt

RUN pip install --no-deps --no-cache-dir \
        boto3==1.42.97 \
        botocore==1.42.97 \
        s3transfer==0.16.1

RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc \
 && chmod +x /usr/local/bin/mc

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
 && unzip awscliv2.zip \
 && ./aws/install \
 && rm -rf awscliv2.zip aws/

RUN mkdir -p /root/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY ipython/startup/README /root/.ipython/profile_default/startup

COPY .pyiceberg.yaml /root/.pyiceberg.yaml

RUN mkdir -p /tmp/sample_notebooks
COPY notebooks/ /tmp/sample_notebooks
RUN chown -R ${NB_USER}:${NB_USER} /tmp/sample_notebooks ${HOME}

ENV AWS_REGION=us-west-2
WORKDIR ${HOME}
USER ${NB_USER}
COPY entrypoint.sh /tmp/
EXPOSE 8888

ENTRYPOINT ["/tmp/entrypoint.sh"]
CMD ["jupyter", "lab", "--notebook-dir=${HOME}"]