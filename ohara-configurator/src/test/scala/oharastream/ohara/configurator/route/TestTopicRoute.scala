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

package oharastream.ohara.configurator.route

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.BrokerApi.BrokerClusterInfo
import oharastream.ohara.client.configurator.TopicApi.{CleanupPolicy, Request, TopicInfo, TopicState}
import oharastream.ohara.client.configurator.{BrokerApi, TopicApi, ZookeeperApi}
import oharastream.ohara.common.rule.OharaTest
import oharastream.ohara.common.setting.{ObjectKey, TopicKey}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.configurator.Configurator
import oharastream.ohara.configurator.fake.FakeBrokerCollie
import org.junit.jupiter.api.{AfterEach, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.{DeserializationException, JsArray, JsNumber, JsObject, JsString, JsTrue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
class TestTopicRoute extends OharaTest {
  private[this] val configurator = Configurator.builder.fake(1, 0).build()

  private[this] val zookeeperApi = ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port)
  private[this] val brokerApi    = BrokerApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val topicApi = TopicApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val brokerClusterInfo = result(brokerApi.list()).head

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration("20 seconds"))

  @Test
  def listTopicDeployedOnNonexistentCluster(): Unit = {
    val topicInfo = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(brokerApi.stop(brokerClusterInfo.key))

    result(topicApi.get(topicInfo.key))
  }

  @Test
  def test(): Unit = {
    // test add
    result(topicApi.list()).size shouldBe 0
    val name                        = CommonUtils.randomString(10)
    val numberOfPartitions: Int     = 3
    val numberOfReplications: Short = 3
    val response = result(
      topicApi.request
        .name(name)
        .numberOfPartitions(numberOfPartitions)
        .numberOfReplications(numberOfReplications)
        .brokerClusterKey(brokerClusterInfo.key)
        .create()
    )
    response.name shouldBe name
    response.numberOfPartitions shouldBe numberOfPartitions
    response.numberOfReplications shouldBe numberOfReplications

    // test get
    val response2 = result(topicApi.get(response.key))
    response.name shouldBe response2.name
    response.brokerClusterKey shouldBe response2.brokerClusterKey
    response.numberOfPartitions shouldBe response2.numberOfPartitions
    response.numberOfReplications shouldBe response2.numberOfReplications

    // test update
    val numberOfPartitions3: Int = 5
    val response3                = result(topicApi.request.name(name).numberOfPartitions(numberOfPartitions3).update())
    response3.numberOfPartitions shouldBe numberOfPartitions3

    // test delete
    result(topicApi.list()).size shouldBe 1
    result(topicApi.delete(response.key))
    result(topicApi.list()).size shouldBe 0

    // test nonexistent data
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.get(TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
    )
  }

  @Test
  def removeTopicFromNonexistentBrokerCluster(): Unit = {
    val name = CommonUtils.randomString(10)
    val bk   = result(configurator.serviceCollie.brokerCollie.clusters()).head
    result(
      topicApi.request
        .name(name)
        .brokerClusterKey(brokerClusterInfo.key)
        .create()
        .flatMap { topicInfo =>
          BrokerApi.access
            .hostname(configurator.hostname)
            .port(configurator.port)
            .stop(bk.key)
            .flatMap(_ => topicApi.delete(topicInfo.key))
        }
        .flatMap(_ => topicApi.list())
        .map(topics => topics.exists(_.name == name))
    ) shouldBe false
  }

  @Test
  def createTopicOnNonexistentCluster(): Unit =
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(ObjectKey.of(CommonUtils.randomString(), CommonUtils.randomString()))
        .create()
    )

  @Test
  def createTopicWithoutBrokerClusterName(): Unit = {
    val zk = result(ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).list()).head

    val zk2 = result(
      ZookeeperApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .nodeNames(zk.nodeNames)
        .create()
    )
    result(ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).start(zk2.key))

    val bk2 = result(
      BrokerApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .zookeeperClusterKey(zk2.key)
        .nodeNames(zk2.nodeNames)
        .create()
    )
    result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).start(bk2.key))

    an[DeserializationException] should be thrownBy result(topicApi.request.name(CommonUtils.randomString(10)).create())

    result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(bk2.key).create())
  }

  @Test
  def testUpdateBrokerClusterKey(): Unit = {
    val zk = result(ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).list()).head

    val zk2 = result(
      ZookeeperApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .nodeNames(zk.nodeNames)
        .create()
    )
    result(ZookeeperApi.access.hostname(configurator.hostname).port(configurator.port).start(zk2.key))

    val bk = result(
      BrokerApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .zookeeperClusterKey(zk2.key)
        .nodeNames(zk2.nodeNames)
        .create()
    )
    result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).start(bk.key))

    val topicInfo = result(
      topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(brokerClusterInfo.key).create()
    )

    result(topicApi.request.key(topicInfo.key).brokerClusterKey(bk.key).update()).brokerClusterKey shouldBe bk.key
  }

  @Test
  def testPartitions(): Unit = {
    val topic0 = result(
      topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(brokerClusterInfo.key).create()
    )

    // we can't reduce number of partitions
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request.name(topic0.name).numberOfPartitions(topic0.numberOfPartitions - 1).update()
    )

    val topic1 = result(topicApi.request.name(topic0.name).numberOfPartitions(topic0.numberOfPartitions + 1).update())

    topic0.name shouldBe topic1.name
    topic0.name shouldBe topic1.name
    topic0.numberOfPartitions + 1 shouldBe topic1.numberOfPartitions
    topic0.numberOfReplications shouldBe topic1.numberOfReplications
  }

  @Test
  def testReplications(): Unit = {
    val topicInfo = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .numberOfReplications(3)
        .brokerClusterKey(brokerClusterInfo.key)
        .create()
    )
    topicInfo.state shouldBe None

    result(topicApi.start(topicInfo.key))

    // we can't reduce number of replications
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request
        .key(topicInfo.key)
        .numberOfReplications((topicInfo.numberOfReplications - 1).asInstanceOf[Short])
        .update()
    )

    // we can't add number of replications
    an[IllegalArgumentException] should be thrownBy result(
      topicApi.request
        .key(topicInfo.key)
        .numberOfReplications((topicInfo.numberOfReplications + 1).asInstanceOf[Short])
        .update()
    )
  }

  @Test
  def duplicateDelete(): Unit =
    (0 to 10).foreach(
      _ => result(topicApi.delete(TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5))))
    )

  @Test
  def duplicateUpdate(): Unit =
    (0 to 10).foreach(
      _ => result(topicApi.request.name(CommonUtils.randomString(10)).brokerClusterKey(brokerClusterInfo.key).update())
    )

  @Test
  def testUpdateNumberOfPartitions(): Unit = {
    val numberOfPartitions = 2
    updatePartOfField(
      _.numberOfPartitions(numberOfPartitions),
      topicInfo =>
        topicInfo.copy(
          settings = TopicApi.access.request
            .settings(topicInfo.settings)
            .numberOfPartitions(numberOfPartitions)
            .creation
            .raw
        )
    )
  }

  @Test
  def testUpdateNumberOfReplications(): Unit = {
    val numberOfReplications: Short = 2
    updatePartOfField(
      _.numberOfReplications(numberOfReplications),
      topicInfo =>
        topicInfo.copy(
          settings = TopicApi.access.request
            .settings(topicInfo.settings)
            .numberOfReplications(numberOfReplications)
            .creation
            .raw
        )
    )
  }

  private[this] def updatePartOfField(req: Request => Request, _expected: TopicInfo => TopicInfo): Unit = {
    val previous = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .numberOfReplications(1)
        .numberOfPartitions(1)
        .brokerClusterKey(brokerClusterInfo.key)
        .create()
    )
    val updated  = result(req(topicApi.request.name(previous.name)).update())
    val expected = _expected(previous)
    updated.name shouldBe expected.name
    updated.brokerClusterKey shouldBe expected.brokerClusterKey
    updated.numberOfReplications shouldBe expected.numberOfReplications
    updated.numberOfPartitions shouldBe expected.numberOfPartitions
  }

  @Test
  def deleteAnTopicRemovedFromKafka(): Unit = {
    val topicName = CommonUtils.randomString(10)

    val topic = result(topicApi.request.name(topicName).brokerClusterKey(brokerClusterInfo.key).create())

    val topicAdmin = result(configurator.serviceCollie.brokerCollie.topicAdmin(brokerClusterInfo))
    try {
      topicAdmin.deleteTopic(topic.key).toCompletableFuture.get()
      // the topic is removed but we don't throw exception.
      result(topicApi.delete(topic.key))
    } finally topicAdmin.close()
  }

  @Test
  def updateTags(): Unit = {
    val tags = Map(
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsNumber(CommonUtils.randomInteger())
    )
    val topicDesc = result(topicApi.request.tags(tags).brokerClusterKey(brokerClusterInfo.key).create())
    topicDesc.tags shouldBe tags

    val tags2 = Map(
      CommonUtils.randomString(10) -> JsString(CommonUtils.randomString(10)),
      CommonUtils.randomString(10) -> JsNumber(CommonUtils.randomInteger())
    )
    val topicDesc2 = result(topicApi.request.key(topicDesc.key).tags(tags2).update())
    topicDesc2.tags shouldBe tags2

    val topicDesc3 = result(topicApi.request.key(topicDesc.key).update())
    topicDesc3.tags shouldBe tags2

    val topicDesc4 = result(topicApi.request.key(topicDesc.key).tags(Map.empty).update())
    topicDesc4.tags shouldBe Map.empty
  }

  @Test
  def testCustomConfigs(): Unit = {
    val key   = TopicApi.SEGMENT_BYTES_KEY
    val value = 1024 * 1024
    val topicDesc = result(
      topicApi.request.setting(key, JsNumber(value)).brokerClusterKey(brokerClusterInfo.key).create()
    )
    topicDesc.configs(key) shouldBe JsNumber(value)
  }

  @Test
  def testStartAndStop(): Unit = {
    val topicDesc = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    topicDesc.state shouldBe None
    result(topicApi.start(topicDesc.key))
    result(topicApi.get(topicDesc.key)).state should not be None
    result(topicApi.stop(topicDesc.key))
    result(topicApi.get(topicDesc.key)).state shouldBe None
  }

  @Test
  def testGroup(): Unit = {
    val group     = CommonUtils.randomString(10)
    val topicDesc = result(topicApi.request.group(group).brokerClusterKey(brokerClusterInfo.key).create())
    topicDesc.group shouldBe group
    result(topicApi.list()).size shouldBe 1
    result(topicApi.list()).exists(_.key == topicDesc.key) shouldBe true
  }

  @Test
  def testCreateSameTopicAfterCreateWithoutAction(): Unit = {
    // This is the backward-compatibility test
    val name  = CommonUtils.randomString(10)
    val topic = result(topicApi.request.name(name).brokerClusterKey(brokerClusterInfo.key).create())
    result(topicApi.get(topic.key)).name shouldBe name

    result(topicApi.delete(topic.key))
    result(topicApi.list()).size shouldBe 0

    result(topicApi.request.name(name).brokerClusterKey(brokerClusterInfo.key).create()).name shouldBe name
  }

  @Test
  def testCreateSameTopicAfterCreateWithAction(): Unit = {
    val name  = CommonUtils.randomString(10)
    val topic = result(topicApi.request.name(name).brokerClusterKey(brokerClusterInfo.key).create())
    result(topicApi.start(topic.key))
    val res = result(topicApi.get(topic.key))
    res.name shouldBe name
    res.state.get shouldBe TopicState.RUNNING

    // stop and delete action sequentially should remove the topic totally
    result(topicApi.stop(topic.key))
    result(topicApi.get(topic.key)).state shouldBe None
    result(topicApi.delete(topic.key))
    result(topicApi.list()).size shouldBe 0

    // pass
    result(topicApi.request.name(name).brokerClusterKey(brokerClusterInfo.key).create())
    result(topicApi.start(topic.key))
    result(topicApi.get(topic.key)).state.get shouldBe TopicState.RUNNING

    result(topicApi.stop(topic.key))
    result(topicApi.get(topic.key)).state shouldBe None
  }

  @Test
  def failToDeleteRunningTopic(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(topicApi.start(topic.key))
    an[IllegalArgumentException] should be thrownBy result(topicApi.delete(topic.key))

    result(topicApi.stop(topic.key))
    result(topicApi.delete(topic.key))
    result(topicApi.list()).exists(_.key == topic.key) shouldBe false
  }

  @Test
  def stopTopicFromStoppingBrokerCluster(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    val bk    = result(brokerApi.list()).head
    result(topicApi.start(topic.key))

    // remove broker cluster
    // we can't use APIs to remove broker since it is used by topic
    val cache      = configurator.serviceCollie.brokerCollie.asInstanceOf[FakeBrokerCollie].clusterCache
    val adminCache = configurator.serviceCollie.brokerCollie.asInstanceOf[FakeBrokerCollie].fakeAdminCache
    import scala.jdk.CollectionConverters._
    val cluster = cache.values().asScala.find(_.key == bk.key).get
    cache.remove(cluster.key)
    adminCache.remove(bk)
    result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list())
      .find(_.key == bk.key)
      .get
      .state shouldBe None
    result(topicApi.stop(topic.key))
    result(topicApi.delete(topic.key))
  }

  @Test
  def stopTopicFromNonexistentBrokerCluster(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    val bk    = result(brokerApi.list()).head
    result(topicApi.start(topic.key))

    // remove broker cluster
    // we can't use APIs to remove broker since it is used by topic
    val cache      = configurator.serviceCollie.brokerCollie.asInstanceOf[FakeBrokerCollie].clusterCache
    val adminCache = configurator.serviceCollie.brokerCollie.asInstanceOf[FakeBrokerCollie].fakeAdminCache
    import scala.jdk.CollectionConverters._
    val cluster = cache.values().asScala.find(_.key == bk.key).get
    cache.remove(cluster.key)
    adminCache.remove(bk)
    result(configurator.store.remove[BrokerClusterInfo](bk.key))
    result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list())
      .find(_.key == bk.key) shouldBe None
    result(topicApi.stop(topic.key))
    result(topicApi.delete(topic.key))
  }

  @Test
  def deleteTopicFromNonexistentBrokerCluster(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    val bk    = result(brokerApi.list()).head
    result(topicApi.start(topic.key))

    // remove broker cluster
    // we can't use APIs to remove broker since it is used by topic
    val cache      = configurator.serviceCollie.brokerCollie.asInstanceOf[FakeBrokerCollie].clusterCache
    val adminCache = configurator.serviceCollie.brokerCollie.asInstanceOf[FakeBrokerCollie].fakeAdminCache
    import scala.jdk.CollectionConverters._
    val cluster = cache.values().asScala.find(_.key == bk.key).get
    cache.remove(cluster.key)
    adminCache.remove(bk)
    result(configurator.store.remove[BrokerClusterInfo](bk.key))
    result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list())
      .find(_.key == bk.key) shouldBe None
    result(topicApi.delete(topic.key))
  }

  @Test
  def failToUpdateRunningTopic(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    result(topicApi.start(topic.key))
    an[IllegalArgumentException] should be thrownBy result(topicApi.request.key(topic.key).update())

    result(topicApi.stop(topic.key))
    // topic is stopped now.
    result(topicApi.request.key(topic.key).update())
  }

  @Test
  def testNameFilter(): Unit = {
    val name  = CommonUtils.randomString(10)
    val topic = result(topicApi.request.name(name).brokerClusterKey(brokerClusterInfo.key).create())
    (0 until 3).foreach(_ => result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create()))
    result(topicApi.list()).size shouldBe 4
    val topics = result(topicApi.query.name(name).execute())
    topics.size shouldBe 1
    topics.head.key shouldBe topic.key

    result(topicApi.query.name(name).state(TopicState.RUNNING).execute()).size shouldBe 0
  }

  @Test
  def testGroupFilter(): Unit = {
    val group = CommonUtils.randomString(10)
    val topic = result(topicApi.request.group(group).brokerClusterKey(brokerClusterInfo.key).create())
    (0 until 3).foreach(_ => result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create()))
    result(topicApi.list()).size shouldBe 4
    val topics = result(topicApi.query.group(group).execute())
    topics.size shouldBe 1
    topics.head.key shouldBe topic.key

    result(topicApi.query.group(group).state(TopicState.RUNNING).execute()).size shouldBe 0
  }

  @Test
  def testTagsFilter(): Unit = {
    val tags = Map(
      "a" -> JsString("b"),
      "b" -> JsNumber(123),
      "c" -> JsTrue,
      "d" -> JsArray(JsString("B")),
      "e" -> JsObject("a" -> JsNumber(123))
    )
    val topic = result(topicApi.request.tags(tags).brokerClusterKey(brokerClusterInfo.key).create())
    (0 until 3).foreach(_ => result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create()))
    result(topicApi.list()).size shouldBe 4
    val topics = result(topicApi.query.tags(tags).execute())
    topics.size shouldBe 1
    topics.head.key shouldBe topic.key

    result(topicApi.query.tags(tags).state(TopicState.RUNNING).execute()).size shouldBe 0
  }

  @Test
  def testStateFilter(): Unit = {
    val topic = result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create())
    (0 until 3).foreach(_ => result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create()))
    result(topicApi.list()).size shouldBe 4
    result(topicApi.start(topic.key))
    val topics = result(topicApi.query.state(TopicState.RUNNING).execute())
    topics.size shouldBe 1
    topics.head.key shouldBe topic.key

    result(topicApi.query.group(CommonUtils.randomString()).state(TopicState.RUNNING).execute()).size shouldBe 0
    result(topicApi.query.group(topic.group).state(TopicState.RUNNING).execute()).size shouldBe 1
    result(topicApi.query.noState.execute()).size shouldBe 3
  }

  @Test
  def testBrokerClusterKeyFilter(): Unit = {
    val bkKey = result(brokerApi.list()).head.key
    val topic = result(topicApi.request.brokerClusterKey(bkKey).create())
    (0 until 3).foreach(_ => result(topicApi.request.brokerClusterKey(brokerClusterInfo.key).create()))
    result(topicApi.list()).size shouldBe 4
    val topics = result(topicApi.query.brokerClusterKey(bkKey).execute())
    topics.size shouldBe 4
    topics.map(_.key) contains topic.key
  }

  @Test
  def failToRunOnStoppedCluster(): Unit = {
    val zookeeper = result(zookeeperApi.request.nodeNames(brokerClusterInfo.nodeNames).create())
    val broker = result(
      brokerApi.request.nodeNames(brokerClusterInfo.nodeNames).zookeeperClusterKey(zookeeper.key).create()
    )
    result(zookeeperApi.start(zookeeper.key))

    val topic = result(topicApi.request.brokerClusterKey(broker.key).create())
    an[IllegalArgumentException] should be thrownBy result(topicApi.start(topic.key))

    result(brokerApi.start(broker.key))
    result(topicApi.start(topic.key))
  }

  @Test
  def testPartialFilter(): Unit = {
    val tags1 = Map(
      "a" -> JsString("b"),
      "b" -> JsNumber(123),
      "c" -> JsTrue,
      "d" -> JsArray(JsString("B")),
      "e" -> JsObject("a" -> JsNumber(123))
    )
    val tags2 = tags1 - "e"
    val topic = result(topicApi.request.tags(tags1).brokerClusterKey(brokerClusterInfo.key).create())
    result(topicApi.start(topic.key))
    (0 until 3).foreach(_ => result(topicApi.request.tags(tags2).brokerClusterKey(brokerClusterInfo.key).create()))
    result(topicApi.list()).size shouldBe 4
    result(topicApi.query.state(TopicState.RUNNING).execute()).size shouldBe 1
    result(topicApi.query.tags(tags1).execute()).size shouldBe 1
    result(topicApi.query.tags(tags2).execute()).size shouldBe 4
    result(topicApi.query.tags(tags2).name(topic.name).execute()).size shouldBe 1
  }

  @Test
  def testCompactAndRatio(): Unit = {
    val policy = CleanupPolicy.COMPACT
    val ratio  = 0.3
    val topicInfo = result(
      topicApi.request
        .cleanupPolicy(policy)
        .minCleanableDirtyRatio(ratio)
        .brokerClusterKey(brokerClusterInfo.key)
        .create()
    )

    topicInfo.cleanupPolicy shouldBe policy
    topicInfo.minCleanableDirtyRatio shouldBe ratio
  }

  @Test
  def testRetention(): Unit = {
    val topicInfo = result(
      topicApi.request
        .retention(Duration(10, TimeUnit.SECONDS))
        .brokerClusterKey(brokerClusterInfo.key)
        .create()
    )
    topicInfo.retention shouldBe Duration(10, TimeUnit.SECONDS)
    topicInfo.configs(TopicApi.RETENTION_TIME_KEY) shouldBe JsString("10 seconds")
    // the value should be converted to kafka
    topicInfo.kafkaConfigs("retention.ms") shouldBe "10000"
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
