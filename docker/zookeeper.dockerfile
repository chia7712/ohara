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

# add label to intermediate image so jenkins can find out this one to remove
ARG STAGE="intermediate"
LABEL stage=$STAGE

# install tools
RUN yum install -y \
  wget \
  git

# download zookeeper
# WARN: Please don't change the value of ZOOKEEPER_DIR
ARG ZOOKEEPER_DIR=/opt/zookeeper
ARG ZOOKEEPER_VERSION=3.5.8
ARG MIRROR_SITE=https://archive.apache.org/dist
RUN wget $MIRROR_SITE/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN mkdir ${ZOOKEEPER_DIR}
RUN tar -zxvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz -C ${ZOOKEEPER_DIR}
RUN rm -f apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN echo "$ZOOKEEPER_VERSION" > $(find "${ZOOKEEPER_DIR}" -maxdepth 1 -type d -name "apache-zookeeper-*")/bin/zookeeper_version

# clone ohara
ARG BRANCH="master"
ARG COMMIT=$BRANCH
ARG REPO="https://github.com/skiptests/ohara.git"
ARG BEFORE_BUILD=""
WORKDIR /testpatch/ohara
RUN git clone $REPO /testpatch/ohara
RUN git checkout $COMMIT
RUN if [[ "$BEFORE_BUILD" != "" ]]; then /bin/bash -c "$BEFORE_BUILD" ; fi
RUN git rev-parse HEAD > $(find "${ZOOKEEPER_DIR}" -maxdepth 1 -type d -name "apache-zookeeper-*")/bin/ohara_version

FROM centos:7.7.1908

# install tools
RUN yum install -y \
  java-11-openjdk

ENV JAVA_HOME=/usr/lib/jvm/jre

# change user
ARG USER=ohara
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER

# copy zookeeper binary
COPY --from=deps /opt/zookeeper /home/$USER
RUN ln -s $(find "/home/$USER" -maxdepth 1 -type d -name "apache-zookeeper-*") /home/$USER/default
COPY --from=deps /testpatch/ohara/docker/zk.sh /home/$USER/default/bin/
RUN chown -R $USER:$USER /home/$USER
RUN chmod +x /home/$USER/default/bin/zk.sh
ENV ZOOKEEPER_HOME=/home/$USER/default
ENV PATH=$PATH:$ZOOKEEPER_HOME/bin

# copy Tini
COPY --from=ghcr.io/skiptests/ohara/deps /tini /tini
RUN chmod +x /tini

USER $USER

ENTRYPOINT ["/tini", "--", "zk.sh"]
