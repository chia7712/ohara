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


usage="USAGE: $0 [start|stop|--help] arg1 arg2 ..."
if [ $# -lt 1 ];
then
  echo ${usage}
  exit 1
fi

COMMAND=$1
case ${COMMAND} in
  start)
    start="true"
    shift
    ;;
  stop)
    stop="true"
    shift
    ;;
  --help)
    help="true"
    shift
    ;;
  *)
    echo ${usage}
    exit 1
    ;;
esac

if [ "${help}" == "true" ];
then
  echo ${usage}
  echo "Argument             Description"
  echo "--------             -----------"
  echo "--user               Set Samba server user name (required argument)"
  echo "--password           Set Samba server password (required argument)"
  echo "--sport              Set Samba server ssn port (required argument)"
  echo "--dport              Set Samba server ds port (required argument)"
  echo "--ssh_user           Set user name to login remote ssh server"
  echo "--host               Set host name to remote host the Samba server container (required argument)"
  echo "--volume             Expose the container folder to the host path"
  exit 1
fi

containerName="samba-benchmark-test"
ssh_user=$USER
ARGUMENT_LIST=("user" "password" "sport" "dport" "ssh_user" "host" "volume")

opts=$(getopt \
    --longoptions "$(printf "%s:," "${ARGUMENT_LIST[@]}")" \
    --name "$(basename "$0")" \
    --options "" \
    -- "$@"
)
eval set --$opts

while [[ $# -gt 0 ]]; do
  case "$1" in
    --user)
      user=$2
      shift 2
      ;;
    --password)
      password=$2
      shift 2
      ;;
    --sport)
      sport=$2
      shift 2
      ;;
    --dport)
      dport=$2
      shift 2
      ;;
    --ssh_user)
      ssh_user=$2
      shift 2
      ;;
    --host)
      host=$2
      shift 2
      ;;
    --volume)
      volume=$2
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

if [[ -z ${user} ]] && [[ "${start}" == "true" ]];
then
  echo 'Please set the --user ${USER_NAME} argument'
  exit 1
fi

if [[ -z ${password} ]] && [[ "${start}" == "true" ]];
then
  echo 'Please set the --password ${PASSWORD} argument'
  exit 1
fi

if [[ -z ${sport} ]] && [[ "${start}" == "true" ]];
then
  echo 'Please set the --sport ${port} argument'
  exit 1
fi

if [[ -z ${dport} ]] && [[ "${start}" == "true" ]];
then
  echo 'Please set the --dport ${port} argument'
  exit 1
fi

if [[ ! -z ${volume} ]] && [[ "${start}" == "true" ]];
then
  volumeArg="-v ${volume}:/home/ohara/samba-data"
fi

if [[ -z ${host} ]];
then
  echo 'Please set the --host argument to deploy Samba server container. example: --host host1'
  exit 1
fi

if [[ ! -z ${ssh_user} ]];
then
  ssh_user=$USER
fi

sambaDockerImageName="ghcr.io/skiptests/ohara/samba"
if [[ "${start}" == "true" ]];
then
  echo "Pull Samba server docker image"
  ssh $ssh_user@${host} docker pull ${sambaDockerImageName}
  echo "Starting Samba server container. user name is ${user}"
  ssh $ssh_user@${host} docker run -d ${volumeArg} --name ${containerName} --env SAMBA_USER_NAME=${user} --env SAMBA_USER_PASS=${password} -p ${sport}:139 -p ${dport}:445 ${sambaDockerImageName}
fi

if [[ "${stop}" == "true" ]];
then
  echo "Stoping Samba server container"
  ssh $ssh_user@${host} docker rm -f ${containerName}
fi
