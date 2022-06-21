#The base image for the container
FROM python:3.8-slim-buster
# disable interactive mode for apt-get
ENV DEBIAN_FRONTEND noninteractive
# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Install Java needed for pyspark
RUN apt-get -y update \
    && apt-get -y upgrade \
    && apt-get -y install ca-certificates \
    && apt-get -y install apt-utils \
    && apt-get -y install curl \
    && apt-get -y install wget \
    && apt-get -y install sudo \
    && apt-get -y install default-jdk \
    && apt-get -y install nano
ENV JAVA_HOME=/usr/lib/jvm/default-java
RUN export JAVA_HOME
ENV PATH="${PATH}:${JAVA_HOME}"
RUN export PATH

# Copy python requirements to the docker container and install
# RUN pip install -U pip setuptools && pip install --upgrade pip
# COPY ./requirements.txt .
# RUN python -m pip install -r ./requirements.txt

#create a non root user to access the container
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=${USER_UID}

RUN groupadd --gid ${USER_GID} ${USERNAME} \ 
    && useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} \
    && apt-get install -y sudo \
    && echo ${USERNAME} ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/${USERNAME} \
    && chmod 0440 /etc/sudoers.d/${USERNAME}

ENV SHELL /bin/bash
WORKDIR /home/vscode/workspace
