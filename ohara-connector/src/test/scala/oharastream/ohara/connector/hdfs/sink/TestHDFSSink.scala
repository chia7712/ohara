/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oharastream.ohara.connector.hdfs.sink
import java.io.File
import java.nio.file.Paths

import oharastream.ohara.client.filesystem.FileSystem
import oharastream.ohara.common.util.CommonUtils

import oharastream.ohara.connector.CsvSinkTestBase
import oharastream.ohara.kafka.connector.csv.CsvSinkConnector

class TestHDFSSink extends CsvSinkTestBase {
  private[this] val tempFolder: File       = CommonUtils.createTempFolder("local_hdfs")
  private[this] val localHdfsURL: String   = new File(tempFolder.getAbsolutePath).toURI.toString
  private[this] val localTopicsDir: String = Paths.get(tempFolder.getPath, "output").toString

  override val fileSystem: FileSystem = FileSystem.hdfsBuilder.url(localHdfsURL).build()

  override val connectorClass: Class[_ <: CsvSinkConnector] = classOf[HDFSSink]

  override def setupProps: Map[String, String] =
    Map(HDFS_URL_KEY -> localHdfsURL, OUTPUT_FOLDER_KEY -> localTopicsDir)
}
