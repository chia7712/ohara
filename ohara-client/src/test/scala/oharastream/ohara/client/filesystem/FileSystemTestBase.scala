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

package oharastream.ohara.client.filesystem

import oharastream.ohara.common.exception.{FileSystemException, NoSuchFileException}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

abstract class FileSystemTestBase extends OharaTest {
  protected val fileSystem: FileSystem

  protected val rootDir: String

  protected def randomDir(parent: String): String = CommonUtils.path(parent, CommonUtils.randomString(10))

  protected def randomDir(): String = randomDir(rootDir)

  protected def randomFile(parent: String): String = CommonUtils.path(parent, CommonUtils.randomString(10) + ".txt")

  protected def randomFile(): String = randomFile(rootDir)

  protected def randomText(): String = CommonUtils.randomString(10)

  @BeforeEach
  def setup(): Unit = {
    println(s"rootDir = $rootDir")
    if (!fileSystem.exists(rootDir)) fileSystem.mkdirs(rootDir)
  }

  @AfterEach
  def cleanup(): Unit = Releasable.close(fileSystem)

  @Test
  def testNormal(): Unit = {
    fileSystem.listFileNames(rootDir).asScala.size shouldBe 0

    // create a folder
    val dir = randomDir()
    fileSystem.exists(dir) shouldBe false
    fileSystem.mkdirs(dir)
    fileSystem.exists(dir) shouldBe true
    fileSystem.listFileNames(rootDir).asScala.size shouldBe 1

    // create a file
    val file = randomFile()
    fileSystem.create(file).close()
    fileSystem.listFileNames(rootDir).asScala.size shouldBe 2

    // delete a file
    fileSystem.delete(file)
    fileSystem.listFileNames(rootDir).asScala.size shouldBe 1
  }

  @Test def testList(): Unit = fileSystem.listFileNames(rootDir).asScala.size shouldBe 0

  @Test
  def testListWithNonExistedPath(): Unit =
    an[NoSuchFileException] should be thrownBy fileSystem.listFileNames(randomDir())

  @Test
  def testCreate(): Unit = {
    val file         = randomFile()
    val text         = randomText()
    val outputStream = fileSystem.create(file)
    outputStream.write(text.getBytes)
    outputStream.close()
    fileSystem.readLines(file) shouldBe Array(text)
  }

  @Test
  def testCreateWithExistedPath(): Unit = {
    val file = randomFile()
    fileSystem.create(file).close()

    // create file with the same path
    an[IllegalArgumentException] should be thrownBy fileSystem.create(file)
  }

  @Test
  def testAppend(): Unit = {
    val file = randomFile()
    val text = randomText()

    // create file
    fileSystem.create(file).close()

    // append file
    val stream = fileSystem.append(file)
    stream.write(text.getBytes)
    stream.close()
    fileSystem.readLines(file) shouldBe Array(text)
  }

  @Test
  def testAppendWithNonExistedPath(): Unit =
    an[NoSuchFileException] should be thrownBy fileSystem.append(randomFile())

  @Test
  def testDelete(): Unit = {
    val file = randomFile()
    fileSystem.create(file).close()
    fileSystem.listFileNames(rootDir).asScala.size shouldBe 1

    // delete file
    fileSystem.delete(file)
    fileSystem.listFileNames(rootDir).asScala.size shouldBe 0

    // delete file again... nothing happen
    fileSystem.delete(file)
  }

  @Test
  def testDeleteWithRecursive(): Unit = {
    val dir   = randomDir()
    val file1 = randomFile(dir)
    val file2 = randomFile(dir)

    fileSystem.create(file1).close()
    fileSystem.create(file2).close()

    fileSystem.listFileNames(dir).asScala.size shouldBe 2

    // It's ok to delete a file.
    fileSystem.delete(file2)
    fileSystem.exists(file2) shouldBe false

    // Delete a folder containing objects
    fileSystem.delete(dir, true)
    fileSystem.exists(dir) shouldBe false
  }

  @Test
  def testDeleteFolderWithContainingFiles(): Unit = {
    val dir  = randomDir()
    val file = randomFile(dir)
    fileSystem.mkdirs(dir)
    fileSystem.create(file).close()
    fileSystem.listFileNames(dir).asScala.size shouldBe 1

    an[FileSystemException] should be thrownBy fileSystem.delete(dir)
  }

  @Test
  def testMoveFile(): Unit = {
    val file1 = randomFile()
    val file2 = randomFile()
    fileSystem.create(file1).close()
    fileSystem.exists(file1) shouldBe true
    fileSystem.exists(file2) shouldBe false

    fileSystem.moveFile(file1, file2)
    fileSystem.exists(file1) shouldBe false
    fileSystem.exists(file2) shouldBe true
  }

  @Test
  def testReMkdirs(): Unit = {
    val dir = "input"
    fileSystem.reMkdirs(dir)
    fileSystem.listFileNames(dir, FileFilter.EMPTY).size shouldBe 0
  }

  @Test
  def testDeleteFileThatHaveBeenRead(): Unit = {
    val file = randomFile(rootDir)
    fileSystem.create(file).close()
    fileSystem.exists(file) shouldBe true
    val data = Seq("123", "456")
    fileSystem.attach(file, data)
    fileSystem.readLines(file) shouldBe data
    fileSystem.delete(file)
    fileSystem.exists(file) shouldBe false
    fileSystem.listFileNames(rootDir, FileFilter.EMPTY).size shouldBe 0
  }

  @Test
  def testFileType(): Unit =
    intercept[NoSuchFileException](fileSystem.fileType(CommonUtils.randomString())).getMessage should include(
      "exist"
    )
}
