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

package oharastream.ohara.client.configurator

import java.io.File

import oharastream.ohara.client.configurator.FileInfoApi.FileInfo
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.util.CommonUtils
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import spray.json.JsString

import scala.concurrent.ExecutionContext.Implicits.global
class TestFileInfoApi extends OharaTest {
  private[this] def access: FileInfoApi.Access = FileInfoApi.access.hostname(CommonUtils.hostname()).port(22)

  @Test
  def nullKeyInGet(): Unit =
    an[NullPointerException] should be thrownBy access.get(null)

  @Test
  def nullKeyInDelete(): Unit =
    an[NullPointerException] should be thrownBy access.delete(null)

  @Test
  def emptyName(): Unit = an[IllegalArgumentException] should be thrownBy access.request.name("")

  @Test
  def nullName(): Unit = an[NullPointerException] should be thrownBy access.request.name(null)

  @Test
  def emptyGroup(): Unit = an[IllegalArgumentException] should be thrownBy access.request.group("")

  @Test
  def nullGroup(): Unit = an[NullPointerException] should be thrownBy access.request.group(null)

  @Test
  def nullFile(): Unit = an[NullPointerException] should be thrownBy access.request.file(null)

  @Test
  def nonexistentFile(): Unit =
    an[IllegalArgumentException] should be thrownBy access.request.file(new File(CommonUtils.randomString(5)))

  @Test
  def nullTags(): Unit = an[NullPointerException] should be thrownBy access.request.tags(null)

  @Test
  def emptyTags(): Unit = access.request.tags(Map.empty)

  @Test
  def bytesMustBeEmptyAfterSerialization(): Unit = {
    val bytes = CommonUtils.randomString().getBytes()
    val fileInfo = new FileInfo(
      group = CommonUtils.randomString(),
      name = CommonUtils.randomString(),
      lastModified = CommonUtils.current(),
      bytes = bytes,
      url = None,
      classInfos = Seq.empty,
      tags = Map("a" -> JsString("b"))
    )

    val copy = FileInfoApi.FILE_INFO_FORMAT.read(FileInfoApi.FILE_INFO_FORMAT.write(fileInfo))
    copy.group shouldBe fileInfo.group
    copy.name shouldBe fileInfo.name
    copy.lastModified shouldBe fileInfo.lastModified
    copy.bytes shouldBe Array.empty
    copy.url shouldBe fileInfo.url
    copy.tags shouldBe fileInfo.tags
  }

  @Test
  def nullUrlShouldBeRemoved(): Unit = {
    val fileInfo = new FileInfo(
      group = CommonUtils.randomString(),
      name = CommonUtils.randomString(),
      lastModified = CommonUtils.current(),
      bytes = Array.emptyByteArray,
      url = None,
      classInfos = Seq.empty,
      tags = Map("a" -> JsString("b"))
    )
    FileInfoApi.FILE_INFO_FORMAT.write(fileInfo).asJsObject.fields should not contain "url"
  }
}
