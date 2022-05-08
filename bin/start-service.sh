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

#----------[LOCATE PROJECT]----------#
SOURCE="${BASH_SOURCE[0]}"
BIN_DIR="$( dirname "$SOURCE" )"
while [ -h "$SOURCE" ]
do
  SOURCE="$(readlink "$SOURCE")"
  BIN_DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
done
BIN_DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
PROJECT_HOME="$(dirname "$BIN_DIR")"

service=$1
shift 1
ARGS=""
i=0
while [ -n "$1" ]
do
  ARGS=$ARGS" "$1
  i=$(($i+1))
  shift
done

if [ "$service" == "manager" ]; then
  cd "$PROJECT_HOME"
  exec env NODE_ENV=production node ./start.js $ARGS
else
  if [ "$service" == "configurator" ]; then
    CLASS="oharastream.ohara.configurator.Configurator"
  elif [ "$service" == "-v" ] || [ "$service" == "version" ] || [ "$service" == "-version" ]; then
    CLASS="oharastream.ohara.common.util.VersionUtils"
  elif [ "$service" == "help" ]; then
    echo "Usage:"
    echo "Option                                   Description"
    echo "-----------                              -----------"
    echo "configurator                             Ohara Configurator provides the service "
    echo "                                         for user and Ohara Manager to use."
    echo ""
    echo "manager                                  Running Ohara Manager. After run this command, you can "
    echo "                                         connect to http://\${HostName or IP}:5050 url by browser."
    echo ""
    echo "class name                               custom class which has main function"
    echo ""
    exit 1
  elif [ "$service" == "" ]; then
    echo "Usage: (configurator|manager|help) [<args>]"
    exit 1
  else
    CLASS=$service
  fi
  #----------[EXECUTION]----------#
  exec "$BIN_DIR/run_java.sh" $CLASS $ARGS
fi
