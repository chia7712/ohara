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

pwdFile="/tmp/password.txt"

if [[ -z "${SAMBA_USER_NAME}" ]];
then
  SAMBA_USER_NAME="ohara"
fi

if [[ -z "${SAMBA_USER_PASS}" ]];
then
  SAMBA_USER_PASS="ohara"
fi

# create password file
echo "${SAMBA_USER_PASS}
${SAMBA_USER_PASS}" > ${pwdFile}

# Modify smb.conf file
smbDir="/home/ohara/samba-data"
mkdir -p ${smbDir}
echo "[homes]
        comment = Home Directories
        valid users = %S, %D%w%S
        path = ${smbDir}
        browseable = No
        read only = No
        inherit acls = Yes" > /etc/samba/smb.conf

groupadd smbgrp
useradd ${SAMBA_USER_NAME} -G smbgrp
smbpasswd -a ${SAMBA_USER_NAME} < ${pwdFile}
chown -R ohara:ohara ${smbDir}

/usr/sbin/smbd --foreground --no-process-group