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

FROM centos:7.7.1908 as deps

# install tools
RUN yum install -y \
  wget \
  net-tools \
  git

# export JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/jre

# download hadoop.tar.gz file
ARG HADOOP_DIR=/opt/hadoop
ARG HADOOP_VERSION=2.7.0
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
RUN mkdir ${HADOOP_DIR}
RUN tar -zxvf hadoop-${HADOOP_VERSION}.tar.gz -C ${HADOOP_DIR}
RUN rm -f hadoop-${HADOOP_VERSION}.tar.gz

# set environment variable

FROM centos:7.7.1908

RUN yum install -y \
  java-11-openjdk \
  which

ENV JAVA_HOME=/usr/lib/jvm/jre

# change user from root to ohara
ARG USER=ohara
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER

COPY --from=deps /opt/hadoop /home/$USER
RUN ln -s $(find "/home/$USER" -maxdepth 1 -type d -name "hadoop-*") /home/$USER/default

RUN mkdir -p /home/$USER/default/config
RUN chown -R ohara:ohara /home/$USER/default/config

COPY core-site.sh /home/$USER/default/bin
COPY hdfs-site.sh /home/$USER/default/bin
COPY namenode.sh /home/$USER/default/bin

ENV HADOOP_HOME=/home/$USER/default
ENV HADOOP_CONF_DIR=/home/$USER/default/config
ENV HADOOP_NAMENODE_FOLDER=/home/$USER/hdfs-data
ENV HADOOP_DATANODE_FOLDER=/home/$USER/hdfs-data
ENV PATH=$PATH:$HADOOP_HOME/bin

RUN chmod +x /home/$USER/default/bin/core-site.sh
RUN chmod +x /home/$USER/default/bin/namenode.sh

# copy Tini
COPY --from=oharastream/ohara:deps /tini /tini
RUN chmod +x /tini

USER $USER

ENTRYPOINT ["/tini", "--", "namenode.sh"]