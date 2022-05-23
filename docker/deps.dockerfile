#
# Copyright 2019 is-land
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM centos:7.7.1908

# install tools
RUN yum install -y \
  git \
  java-11-openjdk-devel \
  wget \
  unzip

# export JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java

# install dependencies for mysql
RUN yum install -y \
  libaio \
  numactl

# build ohara
ARG BRANCH="master"
ARG COMMIT=$BRANCH
ARG REPO="https://github.com/skiptests/ohara.git"
WORKDIR /ohara
RUN git clone $REPO /ohara
RUN git checkout $COMMIT
# download dependencies
RUN ./gradlew clean build -x test
# trigger download of database
RUN ./gradlew cleanTest ohara-client:test --tests TestDatabaseClient -PskipManager

# Add Tini
ARG TINI_VERSION=v0.18.0
RUN wget https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini -O /tini
RUN chmod +x /tini

# change to root
WORKDIR /root