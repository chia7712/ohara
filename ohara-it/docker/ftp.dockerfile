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

# install tools
RUN apt-get update && apt-get install -y \
  wget \
  net-tools

# download ftpserver.tar.gz file
ARG FTPSERVER_DIR=/opt/ftpserver
ARG FTPSERVER_VERSION=1.1.1
RUN wget http://ftp.tc.edu.tw/pub/Apache/mina/ftpserver/${FTPSERVER_VERSION}/dist/apache-ftpserver-${FTPSERVER_VERSION}.tar.gz
RUN mkdir ${FTPSERVER_DIR}
RUN tar -zxvf apache-ftpserver-${FTPSERVER_VERSION}.tar.gz -C ${FTPSERVER_DIR}
RUN rm -f apache-ftpserver-${FTPSERVER_VERSION}.tar.gz

FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
  openjdk-11-jdk \
  which

# change user from root to ohara
ARG USER=ohara
RUN groupadd $USER
RUN useradd -ms /bin/bash -g $USER $USER


COPY --from=deps /opt/ftpserver /home/$USER
RUN chown ohara:ohara -R /home/$USER/*ftpserver-*
RUN ln -s $(find "/home/$USER" -maxdepth 1 -type d -name "*ftpserver-*") /home/$USER/default
RUN chown ohara:ohara -R /home/$USER/default

COPY ftp.sh /home/$USER/default/bin
ENV FTPSERVER_HOME=/home/$USER/default
ENV PATH=$PATH:$FTPSERVER_HOME/bin


RUN chmod +x /home/$USER/default/bin/ftp.sh

# copy Tini
COPY --from=ghcr.io/skiptests/ohara/deps /tini /tini
RUN chmod +x /tini

USER $USER

ENTRYPOINT ["/tini", "--", "ftp.sh"]