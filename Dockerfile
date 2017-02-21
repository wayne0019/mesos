FROM ubuntu:14.04

LABEL RUN docker run -it -v ${pwd}:/home/ubuntu/mesos -e IMAGE=IMAGE IMAGE bash

# UPDATE THE PACKAGES
RUN apt-get update && \
    apt-get install -y tar wget git && \
    apt-get install -y openjdk-7-jdk && \
    apt-get install -y autoconf libtool && \
    apt-get install -y build-essential python-dev libcurl4-nss-dev libsasl2-modules maven libapr1-dev libsvn-dev libsasl2-dev

WORKDIR /home/ubuntu/mesos
