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


usage="USAGE: $0 [hdfs|ftp|samba|oracle] [--help | arg1 arg2 ...]"
if [ $# -lt 1 ];
then
  echo ${usage}
  exit 1
fi

COMMAND=$1
case ${COMMAND} in
  hdfs)
    hdfs="true"
    shift
    ;;
  ftp)
    ftp="true"
    shift
    ;;
  samba)
    samba="true"
    shift
    ;;
  oracle)
    oracle="true"
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

status="start"
if [[ ${help} == "true" ]];
then
  echo "bash $0 [serviceName] --help"
  echo "Support the service name is hdfs, ftp, samba and oracle"
fi

if [[ "${hdfs}" == "true" ]];
then
   echo "Start HDFS service"
   if [[ "$1" == "--help" ]];
   then
     bash hdfs-container.sh --help
   else
     bash hdfs-container.sh ${status} $@
   fi
fi

if [[ "${ftp}" == "true" ]];
then
  echo "Start FTP service"
  if [[ "$1" == "--help" ]];
  then
    bash ftp-container.sh --help
  else
    bash ftp-container.sh ${status} $@
  fi
fi

if [[ "${samba}" == "true" ]];
then
  echo "Start Samba service"
  if [[ "$1" == "--help" ]];
  then
    bash samba-container.sh --help
  else
    bash samba-container.sh ${status} $@
  fi
fi

if [[ "${oracle}" == "true" ]];
then
  echo "Start Oracle service"
  if [[ "$1" == "--help" ]];
  then
    bash oracle-container.sh --help
  else
    bash oracle-container.sh ${status} $@
  fi
fi
