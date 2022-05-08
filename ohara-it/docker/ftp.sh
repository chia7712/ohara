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

ftpSettingFilePath=${FTPSERVER_HOME}/res/conf/ftpd-typical.xml
ftpUserInfoFilePath=${FTPSERVER_HOME}/res/conf/users.properties

if [[ -z "${FORCE_PASSIVE_IP}" ]];
then
  echo 'Please setting the ${FORCE_PASSIVE_IP} evnvironment variable'
  exit 1
fi

if [[ -z "${PASSIVE_PORT_RANGE}" ]];
then
  echo 'Please setting the ${PASSIVE_PORT_RANGE} evnvironment variable'
  exit 1
fi

if [[ -z "${FTP_USER_NAME}" ]];
then
  FTP_USER_NAME="ohara"
fi

if [[ -z "${FTP_USER_PASS}" ]];
then
  FTP_USER_PASS="ohara"
fi

echo "<server xmlns=\"http://mina.apache.org/ftpserver/spring/v1\"
        xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"
        xsi:schemaLocation=\"
           http://mina.apache.org/ftpserver/spring/v1 http://mina.apache.org/ftpserver/ftpserver-1.0.xsd
           \"
        id=\"myServer\">
        <listeners>
            <nio-listener name=\"default\" port=\"2121\">
                <ssl>
                    <keystore file=\"./res/ftpserver.jks\" password=\"password\" />
                </ssl>
                <data-connection idle-timeout=\"60\">
                    <active enabled=\"true\" local-address=\"0.0.0.0\" local-port=\"2323\" ip-check=\"true\" />
                    <passive ports=\"${PASSIVE_PORT_RANGE}\" address=\"0.0.0.0\" external-address=\"${FORCE_PASSIVE_IP}\" />
                </data-connection>
            </nio-listener>
        </listeners>
        <file-user-manager file=\"./res/conf/users.properties\" encrypt-passwords=\"clear\" />
</server>" > ${ftpSettingFilePath}

ftpDir=/home/ohara/ftp-data
mkdir -p ${ftpDir}
echo "ftpserver.user.${FTP_USER_NAME}.userpassword=${FTP_USER_PASS}
ftpserver.user.${FTP_USER_NAME}.homedirectory=${ftpDir}
ftpserver.user.${FTP_USER_NAME}.enableflag=true
ftpserver.user.${FTP_USER_NAME}.writepermission=true
ftpserver.user.${FTP_USER_NAME}.maxloginnumber=0
ftpserver.user.${FTP_USER_NAME}.maxloginperip=0
ftpserver.user.${FTP_USER_NAME}.idletime=0
ftpserver.user.${FTP_USER_NAME}.uploadrate=0
ftpserver.user.${FTP_USER_NAME}.downloadrate=0" > ${ftpUserInfoFilePath}

ftpd.sh file://${ftpSettingFilePath}
