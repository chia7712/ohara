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

while getopts n:s:u:v: option
do
 case "${option}"
 in
 n) nameNode=${OPTARG};;
 s) dataNodes=${OPTARG};;
 u) userName=${OPTARG};;
 v) volume=${OPTARG};;
 esac
done

if [ "${help}" == "true" ];
then
  echo ${usage}
  echo "Argument             Description"
  echo "--------             -----------"
  echo "-n                   Set HDFS namenode hostname and port to start the datanode. example: -n host1:9000"
  echo "-s                   Set HDFS datanode hostname list. example: -s host1,host2,host3"
  echo "-u                   Set ssh user name to remote deploy datanode"
  echo "-v                   Expose the namenode and datanode container folder for the host path"
  exit 1
fi

if [[ -z "${userName}" ]];
then
  userName="ohara"
fi

if [[ -z "${nameNode}" ]];
then
  echo "Please set the -n argument for the namenode host"
  exit 1;
fi

if [[ ! -z "${volume}" ]];
then
  volumeArg="-v ${volume}:/home/ohara/hdfs-data"
fi

nameNodeImageName="ghcr.io/chia7712/ohara/hdfs-namenode"
dataNodeImageName="ghcr.io/chia7712/ohara/hdfs-datanode"

nameNodeContainerName="namenode"
dataNodeContainerName="datanode"

IFS=","
if [ "${start}" == "true" ];
then
  echo "Starting HDFS container"
  echo "Starting ${HOSTNAME} node namenode......"
  ssh ${userName}@${nameNode} "docker pull ${nameNodeImageName};docker run -d ${volumeArg} -it --name ${nameNodeContainerName} --env HADOOP_NAMENODE=${nameNode}:9000 --net host ${nameNodeImageName}"

  for dataNode in $dataNodes;
  do
    echo "Starting ${dataNode} node datanode......"
    ssh ${userName}@${dataNode} "docker pull ${dataNodeImageName};docker run -d ${volumeArg} -it --name ${dataNodeContainerName} --env HADOOP_NAMENODE=${nameNode}:9000 --net host ${dataNodeImageName}"
  done
fi

if [ "${stop}" == "true" ];
then
    echo "Stoping HDFS container"
    for dataNode in ${dataNodes};
    do
      echo "Stoping ${dataNode} node datanode......"
      ssh ${userName}@${dataNode} docker rm -f ${dataNodeContainerName}
    done
    echo "Stoping ${nameNode} node namenode......"
    ssh ${userName}@${nameNode} docker rm -f ${nameNodeContainerName}
fi
