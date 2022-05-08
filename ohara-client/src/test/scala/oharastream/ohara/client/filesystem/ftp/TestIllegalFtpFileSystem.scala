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

package oharastream.ohara.client.filesystem.ftp

import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.testing.service.FtpServer
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

class TestIllegalFtpFileSystem extends OharaTest {
  private[this] val server = FtpServer.local()

  private[this] val fileSystem =
    FtpFileSystem.builder
    // login ftp server with an invalid account and then see what happens :)
      .user(CommonUtils.randomString(10))
      .password(server.password)
      .hostname(server.hostname)
      .port(server.port)
      .build()

  @Test
  def testList(): Unit = an[Throwable] should be thrownBy fileSystem.listFileNames("/")

  @Test
  def testExist(): Unit = an[Throwable] should be thrownBy fileSystem.exists("/")

  @Test
  def testNonExist(): Unit = an[Throwable] should be thrownBy fileSystem.nonExists("/")

  @Test
  def testMkDir(): Unit = an[Throwable] should be thrownBy fileSystem.mkdirs(s"/${CommonUtils.randomString(10)}")

  @Test
  def testWorkingFolder(): Unit = an[Throwable] should be thrownBy fileSystem.workingFolder()

  @Test
  def testFileType(): Unit = an[Throwable] should be thrownBy fileSystem.fileType("/")

  @Test
  def testOpen(): Unit = an[Throwable] should be thrownBy fileSystem.open(s"/${CommonUtils.randomString(10)}")

  @Test
  def testWrite(): Unit = an[Throwable] should be thrownBy fileSystem.append(s"/${CommonUtils.randomString(10)}")

  @Test
  def testReMkdirs(): Unit = an[Throwable] should be thrownBy fileSystem.reMkdirs(s"/${CommonUtils.randomString(10)}")

  @Test
  def testDelete(): Unit = an[Throwable] should be thrownBy fileSystem.delete(s"/${CommonUtils.randomString(10)}")

  @Test
  def testAttach(): Unit =
    an[Throwable] should be thrownBy fileSystem.attach(s"/${CommonUtils.randomString(10)}", "abc")

  @AfterEach
  def tearDown(): Unit = {
    Releasable.close(fileSystem)
    Releasable.close(server)
  }
}
