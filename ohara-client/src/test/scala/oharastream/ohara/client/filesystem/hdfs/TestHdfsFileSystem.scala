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

package oharastream.ohara.client.filesystem.hdfs

import java.io.{BufferedWriter, File, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import oharastream.ohara.client.filesystem.{FileFilter, FileSystem, FileSystemTestBase}
import oharastream.ohara.common.exception.FileSystemException
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._

class TestHdfsFileSystem extends FileSystemTestBase {
  private[this] val tempFolder: File = CommonUtils.createTempFolder("local_hdfs")

  private[this] val hdfsURL: String = new File(tempFolder.getAbsolutePath).toURI.toString

  override protected val fileSystem: FileSystem = FileSystem.hdfsBuilder.url(hdfsURL).build

  override protected val rootDir: String = tempFolder.toString

  // override this method because the Local HDFS doesn't support append()
  @Test
  override def testAppend(): Unit = {
    val file = randomFile()
    fileSystem.create(file).close()

    intercept[FileSystemException] {
      fileSystem.append(file)
    }.getMessage shouldBe "Not supported"
  }

  // override this method because the Local HDFS doesn't support append()
  @Test
  override def testDeleteFileThatHaveBeenRead(): Unit = {
    val file              = randomFile(rootDir)
    val data: Seq[String] = Seq("123", "456")
    val writer            = new BufferedWriter(new OutputStreamWriter(fileSystem.create(file), StandardCharsets.UTF_8))
    try data.foreach(line => {
      writer.append(line)
      writer.newLine()
    })
    finally writer.close()

    fileSystem.exists(file) shouldBe true
    fileSystem.readLines(file) shouldBe data
    fileSystem.delete(file)
    fileSystem.exists(file) shouldBe false
    fileSystem.listFileNames(rootDir, FileFilter.EMPTY).size shouldBe 0
  }
}
