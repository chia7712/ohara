#!/usr/bin/env bash
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


# JVM options. Below is the default setting.
if [[ -z "$OHARA_OPTS" ]]; then
  export OHARA_OPTS="-Xmx4000m -XX:+UseG1GC"
fi

# This env is set specifically for Ohara manager
if [[ -z "$OHARA_MANAGER_NODE_ENV" ]]; then
  export OHARA_MANAGER_NODE_ENV="prodcution"
fi

#----------[heap]----------#
if [[ -z "$OHARA_HEAP_OPTS" ]]; then
  export OHARA_HEAP_OPTS="-Xms1024M -Xmx1024M"
fi

#----------[JMX]----------#
if [[ -z "$OHARA_JMX_OPTS" ]]; then
  export OHARA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

  if [[ ! -z $JMX_PORT ]]; then
    export OHARA_JMX_OPTS="$OHARA_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT -Dcom.sun.management.jmxremote.rmi.port=$JMX_PORT"
  fi

  if [[ ! -z $JMX_HOSTNAME ]]; then
    export OHARA_JMX_OPTS="$OHARA_JMX_OPTS -Djava.rmi.server.hostname=$JMX_HOSTNAME"
  fi
fi