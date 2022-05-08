#!/bin/bash
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

# change the working path from root to ohara folder
cd $OHARA_HOME/bin

if [[ "$1" == "-v" ]] || [[ "$1" == "-version" ]]; then
  exec $OHARA_HOME/bin/ohara.sh -v
else
  if [[ ! -z "$STREAM_JAR_URLS" ]]; then
    IFS=','
    read -ra ADDR <<< "$STREAM_JAR_URLS"
    for i in "${ADDR[@]}"; do
      echo "start to download jar:$i"
      wget $i -P $OHARA_HOME/lib
    done
  fi
  # Stream collie decides on the main class for starting stream app.
  exec $OHARA_HOME/bin/ohara.sh start "$@"
fi