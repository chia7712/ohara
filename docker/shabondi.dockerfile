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

FROM ghcr.io/chia7712/ohara/deps as deps

# add label to intermediate image so jenkins can find out this one to remove
ARG STAGE="intermediate"
LABEL stage=$STAGE

ARG BRANCH="main"
ARG COMMIT=$BRANCH
ARG SCALA_VERSION=2.13.3
ARG REPO="https://github.com/chia7712/ohara.git"
ARG BEFORE_BUILD=""
WORKDIR /testpatch/ohara
RUN git clone $REPO /testpatch/ohara
RUN git checkout $COMMIT
RUN if [[ "$BEFORE_BUILD" != "" ]]; then /bin/bash -c "$BEFORE_BUILD" ; fi
RUN ./gradlew clean build -x test  \
  -Pscala.version=$SCALA_VERSION
RUN mkdir /opt/ohara
RUN tar -xvf $(find "/testpatch/ohara/ohara-shabondi/build/distributions" -maxdepth 1 -type f -name "*.tar") -C /opt/ohara/

FROM ubuntu:22.04

# install tools
RUN apt-get update && apt-get install -y openjdk-11-jdk

# add user
ARG USER=ohara
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER

# clone ohara binary
COPY --from=deps /opt/ohara /home/$USER
RUN ln -s $(find "/home/$USER/" -maxdepth 1 -type d -name "ohara-*") /home/$USER/default
COPY --from=deps /testpatch/ohara/docker/shabondi.sh /home/$USER/default/bin/
RUN chown -R $USER:$USER /home/$USER
RUN chmod +x /home/$USER/default/bin/shabondi.sh
ENV OHARA_HOME=/home/$USER/default
ENV PATH=$PATH:$OHARA_HOME/bin

# clone Tini
COPY --from=deps /tini /tini
RUN chmod +x /tini

# change to user
USER $USER

ENTRYPOINT ["/tini", "--", "shabondi.sh"]
