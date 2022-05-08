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

package oharastream.ohara.configurator.store

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.ConnectorApi.ConnectorInfo
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestRocksDataStore extends OharaTest {
  private[this] val store: DataStore = DataStore()

  private[this] def createData(_name: String) = SimpleData(
    group = _name,
    name = _name,
    lastModified = CommonUtils.current(),
    kind = _name
  )

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(20, TimeUnit.SECONDS))

  private[this] def createData(): SimpleData = createData(random())

  @Test
  def testReopen(): Unit = {
    val folder = CommonUtils.createTempFolder(CommonUtils.randomString(10))

    val value0 = createData()
    val value1 = createData()

    val s0 = DataStore.builder.persistentFolder(folder.getCanonicalPath).build()
    try {
      s0.numberOfTypes() shouldBe 0
      result(s0.addIfAbsent(value0))
      result(s0.addIfAbsent(value1))
      s0.size() shouldBe 2
      s0.numberOfTypes() shouldBe 1
    } finally s0.close()

    val s1 = DataStore.builder.persistentFolder(folder.getCanonicalPath).build()
    try {
      s1.numberOfTypes() shouldBe 1
      s1.size() shouldBe 2
      result(s1.value[SimpleData](value0.key)) shouldBe value0
      result(s1.value[SimpleData](value1.key)) shouldBe value1
    } finally s1.close()
  }

  @Test
  def testGetter(): Unit = {
    val value = createData()
    result(store.addIfAbsent(value)) shouldBe value
    result(store.get[SimpleData](value.key)) shouldBe Some(value)
  }

  @Test
  def testValue(): Unit = {
    val value = createData()
    an[NoSuchElementException] should be thrownBy result(store.value(value.key))
    result(store.addIfAbsent(value)) shouldBe value
    result(store.value[SimpleData](value.key)) shouldBe value
  }

  @Test
  def testMultiPut(): Unit = {
    store.size() shouldBe 0
    result(store.addIfAbsent(createData()))
    store.size() shouldBe 1
    result(store.addIfAbsent(createData()))
    store.size() shouldBe 2
    result(store.addIfAbsent(createData()))
    store.size() shouldBe 3
  }

  @Test
  def testDelete(): Unit = {
    val value = createData()
    result(store.addIfAbsent(value)) shouldBe value
    result(store.get[SimpleData](value.key)) shouldBe Some(value)
    result(store.remove[SimpleData](value.key)) shouldBe true
    store.size() shouldBe 0
  }

  @Test
  def testDuplicateAddIfAbsent(): Unit = {
    val value0 = createData()
    val value1 = createData(value0.name)
    result(store.addIfAbsent(value0)) shouldBe value0
    store.size() shouldBe 1
    an[IllegalStateException] should be thrownBy result(store.addIfAbsent(value1))
    store.size() shouldBe 1
    result(store.addIfAbsent(createData()))
    store.size() shouldBe 2
    result(store.raws()).size shouldBe store.size()
  }

  @Test
  def testDuplicateAdd(): Unit = {
    val value0 = createData()
    (0 until 10).foreach(_ => result(store.add(value0)))
  }

  @Test
  def testUpdate(): Unit = {
    an[NoSuchElementException] should be thrownBy result(
      store.addIfPresent[SimpleData](ObjectKey.of(random(), random()), (_: SimpleData) => createData())
    )
    val value0 = createData()
    val value1 = value0.copy(kind = CommonUtils.randomString())
    result(store.addIfAbsent(value0)) shouldBe value0
    store.size() shouldBe 1
    result(store.addIfPresent[SimpleData](value0.key, (v: SimpleData) => {
      v shouldBe value0
      value1
    })) shouldBe value1
    store.size() shouldBe 1

    val value2 = value0.copy(kind = CommonUtils.randomString())
    result(store.addIfPresent[SimpleData](value0.key, (v: SimpleData) => {
      v shouldBe value1
      value2
    })) shouldBe value2
    store.size() shouldBe 1
  }

  @Test
  def testValues(): Unit = {
    val value0 = createData()
    val value1 = createData()
    result(store.addIfAbsent(value0)) shouldBe value0
    result(store.addIfAbsent(value1)) shouldBe value1
    result(store.values[SimpleData]()).size shouldBe 2
    result(store.values[SimpleData]()).contains(value0) shouldBe true
    result(store.values[SimpleData]()).contains(value1) shouldBe true
  }

  @Test
  def testExist(): Unit = {
    val value = createData()
    result(store.exist(value.key)) shouldBe false
    result(store.addIfAbsent(value)) shouldBe value
    result(store.exist(value.key)) shouldBe false
    result(store.exist[SimpleData](value.key)) shouldBe true
  }

  @Test
  def testAdd(): Unit = {
    val data = createData("abcd")
    result(store.addIfAbsent(data))

    result(store.exist[SimpleData](data.key)) shouldBe true
    result(store.exist[SimpleData](ObjectKey.of(data.group, CommonUtils.randomString()))) shouldBe false
    result(store.exist[SimpleData](ObjectKey.of(CommonUtils.randomString(), data.name))) shouldBe false
    result(store.nonExist[SimpleData](data.key)) shouldBe false
    result(store.nonExist[SimpleData](ObjectKey.of(data.group, CommonUtils.randomString()))) shouldBe true
    result(store.nonExist[SimpleData](ObjectKey.of(CommonUtils.randomString(), data.name))) shouldBe true
  }

  @Test
  def testList(): Unit = {
    result(store.addIfAbsent(createData("abcd")))
    result(store.addIfAbsent(createData("xyz")))

    store.size() shouldBe 2
  }

  @Test
  def testRemove(): Unit = {
    val data1 = createData()
    val data2 = createData()

    result(store.addIfAbsent(data1))
    result(store.addIfAbsent(data2))
    store.size() shouldBe 2

    result(store.remove(ObjectKey.of(random(), random()))) shouldBe false
    result(store.remove[SimpleData](ObjectKey.of(random(), random()))) shouldBe false
    result(store.remove[ConnectorInfo](ObjectKey.of(random(), random()))) shouldBe false

    result(store.remove[SimpleData](ObjectKey.of(data1.group, data1.name))) shouldBe true
    store.size() shouldBe 1
  }

  @Test
  def testRaw(): Unit = {
    val data1 = createData("abcd")

    result(store.addIfAbsent(data1))
    store.size() shouldBe 1

    result(store.remove(ObjectKey.of(data1.group, data1.name))) shouldBe false
    result(store.remove[SimpleData](ObjectKey.of(data1.group, "1234"))) shouldBe false
    result(store.remove[SimpleData](ObjectKey.of("1234", data1.name))) shouldBe false
    result(store.remove[ConnectorInfo](ObjectKey.of(data1.group, data1.name))) shouldBe false

    result(store.raws()).head.asInstanceOf[SimpleData] shouldBe data1
    result(store.raws(ObjectKey.of(data1.group, data1.name))).head.asInstanceOf[SimpleData] shouldBe data1
  }

  @Test
  def testAccessClosedStore(): Unit = {
    val store2: DataStore = DataStore()
    store2.close()
    an[RuntimeException] should be thrownBy store2.size()
    an[RuntimeException] should be thrownBy store2.numberOfTypes()
    an[RuntimeException] should be thrownBy result(store2.get[SimpleData](ObjectKey.of(random(), random())))
    an[RuntimeException] should be thrownBy result(store2.add[SimpleData](createData()))
    an[RuntimeException] should be thrownBy result(store2.exist[SimpleData](ObjectKey.of(random(), random())))
    an[RuntimeException] should be thrownBy result(
      store2.addIfPresent[SimpleData](ObjectKey.of(random(), random()), (_: SimpleData) => createData())
    )
    an[RuntimeException] should be thrownBy result(store2.raws(ObjectKey.of(random(), random())))
    an[RuntimeException] should be thrownBy result(store2.raws())
    an[RuntimeException] should be thrownBy result(store2.value[SimpleData](ObjectKey.of(random(), random())))
    an[RuntimeException] should be thrownBy result(store2.values[SimpleData]())
    an[RuntimeException] should be thrownBy result(store2.remove[SimpleData](ObjectKey.of(random(), random())))
  }

  @Test
  def addDataWithIncorrectGroupAndName(): Unit = {
    val data = createData()
    store.add(data)
    an[IllegalArgumentException] should be thrownBy result(
      store.addIfPresent(data.key, (_: SimpleData) => createData())
    )
  }

  private[this] def random(): String = CommonUtils.randomString(5)

  @AfterEach
  def tearDown(): Unit = Releasable.close(store)
}
