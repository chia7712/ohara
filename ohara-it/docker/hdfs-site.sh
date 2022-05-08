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


echo '<?xml version="1.0" encoding="UTF-8"?>'
echo '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>'
echo '<configuration>'
echo '  <property>'
echo '    <name>dfs.replication</name>'
echo "    <value>1</value>"
echo '  </property>'
echo '  <property>'
echo '    <name>dfs.permissions</name>'
echo '    <value>false</value>'
echo '  </property>'
echo '  <property>'
echo '    <name>dfs.data.dir</name>'
echo "    <value>${HADOOP_DATANODE_FOLDER}</value>"
echo '  </property>'
echo '  <property>'
echo '    <name>dfs.namenode.rpc-address</name>'
echo "    <value>${HADOOP_NAMENODE}</value>"
echo '  </property>'
echo '</configuration>'