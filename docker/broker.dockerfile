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

FROM ubuntu:22.04 as deps

# add label to intermediate image so jenkins can find out this one to remove
ARG STAGE="intermediate"
LABEL stage=$STAGE

# install tools
RUN apt-get update && apt-get install -y wget git

# download kafka
# WARN: Please don't change the value of KAFKA_DIR
ARG KAFKA_DIR=/opt/kafka
ARG KAFKA_VERSION=3.1.1
ARG SCALA_VERSION=2.13.3
ARG MIRROR_SITE=https://archive.apache.org/dist
RUN wget $MIRROR_SITE/kafka/${KAFKA_VERSION}/kafka_$(echo $SCALA_VERSION | cut -d. -f1-2)-${KAFKA_VERSION}.tgz
RUN mkdir ${KAFKA_DIR}
RUN tar -zxvf $(find ./ -maxdepth 1 -type f -name "kafka_*") -C ${KAFKA_DIR}
RUN echo "$KAFKA_VERSION" > $(find "${KAFKA_DIR}" -maxdepth 1 -type d -name "kafka_*")/bin/broker_version

# clone ohara
ARG BRANCH="main"
ARG COMMIT=$BRANCH
ARG REPO="https://github.com/skiptests/ohara.git"
ARG BEFORE_BUILD=""
WORKDIR /testpatch/ohara
RUN git clone $REPO /testpatch/ohara
RUN git checkout $COMMIT
RUN if [[ "$BEFORE_BUILD" != "" ]]; then /bin/bash -c "$BEFORE_BUILD" ; fi
RUN git rev-parse HEAD > $(find "${KAFKA_DIR}" -maxdepth 1 -type d -name "kafka_*")/bin/ohara_version

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y openjdk-11-jdk

# change user from root to kafka
ARG USER=ohara
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER

# copy kafka binary
# TODO: we should remove unused dependencies since this image is used to run broker only
COPY --from=deps /opt/kafka /home/$USER
RUN ln -s $(find "/home/$USER" -maxdepth 1 -type d -name "kafka_*") /home/$USER/default
COPY --from=deps /testpatch/ohara/docker/broker.sh /home/$USER/default/bin/
RUN chmod +x /home/$USER/default/bin/broker.sh
RUN chown -R $USER:$USER /home/$USER
ENV KAFKA_HOME=/home/$USER/default
ENV PATH=$PATH:$KAFKA_HOME/bin

# copy Tini
COPY --from=ghcr.io/skiptests/ohara/deps /tini /tini
RUN chmod +x /tini

USER $USER

ENTRYPOINT ["/tini", "--", "broker.sh"]
