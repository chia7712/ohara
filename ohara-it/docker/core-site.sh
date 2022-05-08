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
echo '    <name>fs.defaultFS</name>'
echo "    <value>hdfs://${HOSTNAME}:9000</value>"
echo '  </property>'
echo '  <property>'
echo '    <name>hadoop.tmp.dir</name>'
echo "    <value>${HADOOP_NAMENODE_FOLDER}</value>"
echo '  </property>'
echo '</configuration>'