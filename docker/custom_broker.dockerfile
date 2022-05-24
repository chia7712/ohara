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

ARG BUILD_OS=ghcr.io/skiptests/ohara/deps
ARG RUNTIME_OS=azul/zulu-openjdk:11
FROM $BUILD_OS as deps

# add label to intermediate image so jenkins can find out this one to remove
ARG STAGE="intermediate"
LABEL stage=$STAGE

# WARN: Please don't change the value of KAFKA_DIR
ARG KAFKA_DIR=/opt/kafka
ARG KAFKA_REVISION="trunk"
ARG SCALA_VERSION=2.13.3
ARG KAFKA_REPO="https://github.com/apache/kafka"
RUN git clone $KAFKA_REPO /kafka
WORKDIR /kafka
RUN git checkout $KAFKA_REVISION
RUN ./gradlew clean releaseTarGz -Pscala.version=$SCALA_VERSION
RUN mkdir ${KAFKA_DIR}
RUN echo $(./gradlew properties | grep "version:" | cut -f2 -d' ') > /version
RUN tar -zxvf $(find ./core/build/distributions/ -maxdepth 1 -type f -name "kafka_*$(cat /version).tgz" | head -n 1) -C ${KAFKA_DIR}
RUN cp /version $(find "${KAFKA_DIR}" -maxdepth 1 -type d -name "kafka_*")/bin/broker_version

# clone ohara
ARG BRANCH="main"
ARG COMMIT=$BRANCH
ARG REPO="https://github.com/skiptests/ohara.git"
WORKDIR /testpatch/ohara
RUN git clone $REPO /testpatch/ohara
RUN git checkout $COMMIT
RUN git rev-parse HEAD > $(find "${KAFKA_DIR}" -maxdepth 1 -type d -name "kafka_*")/bin/ohara_version

FROM $RUNTIME_OS

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
