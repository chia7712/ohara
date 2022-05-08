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

# our manager is running background so a way of breaking manager is necessary
if [ "$1" == "manager" ]; then
  cd "$PROJECT_HOME"
  yarn clean:process
  exit
elif [ "$1" == "-v" ] || [ "$1" == "version" ] || [ "$1" == "-version" ]; then
  "$BIN_DIR/run_java.sh" oharastream.ohara.common.util.VersionUtils
  exit 1
elif [ "$1" == "help" ]; then
  echo "Usage:"
  echo "Option                                   Description"
  echo "--------                                 -----------"
  echo "manager                                  Stop the Ohara Manager."
else
  echo "Usage: (manager|help)"
  exit 1
fi