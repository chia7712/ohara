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

package oharastream.ohara.configurator

import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.ConnectorApi.State
import oharastream.ohara.client.configurator.{BrokerApi, ConnectorApi, PipelineApi, TopicApi, WorkerApi}
import oharastream.ohara.client.kafka.ConnectorAdmin
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.connector.ftp.FtpSource
import oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions
import oharastream.ohara.testing.WithBrokerWorker
import org.junit.jupiter.api.{AfterEach, Disabled, Test}
import org.scalatest.matchers.should.Matchers._
import spray.json.{JsArray, JsBoolean, JsNumber, JsString}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class TestControlConnector extends WithBrokerWorker {
  private[this] val configurator =
    Configurator.builder.fake(testUtil.brokersConnProps, testUtil().workersConnProps()).build()

  private[this] val connectorApi = ConnectorApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] val topicApi = TopicApi.access.hostname(configurator.hostname).port(configurator.port)

  private[this] def result[T](f: Future[T]): T = Await.result(f, Duration(10, TimeUnit.SECONDS))

  private[this] def await(f: () => Boolean): Unit = CommonUtils.await(() => f(), java.time.Duration.ofSeconds(20))

  private[this] val workerClusterInfo = result(
    WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list()
  ).head

  @Test
  def keysInSettingShouldBeOverridable(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )

    // test idempotent start
    result(topicApi.start(topic.key))

    val sink = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .numberOfTasks(1)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .setting("tasksStatus", JsArray.empty)
        .create()
    )
    result(connectorApi.start(sink.key))
    await(() => result(connectorApi.get(sink.key)).tasksStatus.nonEmpty)
    await(() => result(connectorApi.get(sink.key)).tasksStatus.forall(_.state == State.RUNNING))
  }

  @Test
  def testNormalCase(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )

    val sink = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .numberOfTasks(1)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    // test idempotent start
    result(topicApi.start(topic.key))
    (0 until 3).foreach(_ => result(connectorApi.start(sink.key)))

    await(() => result(connectorApi.get(sink.key)).state.contains(State.RUNNING))
    result(connectorApi.get(sink.key)).aliveNodes should contain(CommonUtils.hostname())
    result(connectorApi.get(sink.key)).error shouldBe None

    val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)
    try {
      await(
        () =>
          try result(connectorAdmin.exist(sink.key))
          catch {
            case _: Throwable => false
          }
      )
      await(
        () =>
          try result(connectorAdmin.status(sink.key)).connector.state == State.RUNNING.name
          catch {
            case _: Throwable => false
          }
      )
      result(connectorApi.get(sink.key)).state.get shouldBe State.RUNNING

      // test idempotent pause
      (0 until 3).foreach(_ => result(connectorApi.pause(sink.key)))
      await(
        () =>
          try result(connectorAdmin.status(sink.key)).connector.state == State.PAUSED.name
          catch {
            case _: Throwable => false
          }
      )
      result(connectorApi.get(sink.key)).state.get shouldBe State.PAUSED

      // test idempotent resume
      (0 until 3).foreach(_ => result(connectorApi.resume(sink.key)))
      await(
        () =>
          try result(connectorAdmin.status(sink.key)).connector.state == State.RUNNING.name
          catch {
            case _: Throwable => false
          }
      )
      result(connectorApi.get(sink.key)).state.get shouldBe State.RUNNING

      // test idempotent stop. the connector should be removed
      (0 until 3).foreach(_ => result(connectorApi.stop(sink.key)))
      await(() => result(connectorAdmin.nonExist(sink.key)))
      await(() => result(connectorApi.get(sink.key)).state.isEmpty)
    } finally if (result(connectorAdmin.exist(sink.key))) result(connectorAdmin.delete(sink.key))
  }

  @Test
  def testUpdateRunningConnector(): Unit = {
    val topicName = CommonUtils.randomString(10)
    val topic = result(
      topicApi.request
        .name(topicName)
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )
    val sink = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .topicKey(topic.key)
        .numberOfTasks(1)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    // test start
    result(topicApi.start(topic.key))
    result(connectorApi.start(sink.key))
    val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)
    try {
      await(
        () =>
          try result(connectorAdmin.exist(sink.key))
          catch {
            case _: Throwable => false
          }
      )
      await(
        () =>
          try result(connectorAdmin.status(sink.key)).connector.state == State.RUNNING.name
          catch {
            case _: Throwable => false
          }
      )

      an[IllegalArgumentException] should be thrownBy result(
        connectorApi.request
          .name(sink.name)
          .group(sink.group)
          .className(classOf[FallibleSink].getName)
          .numberOfTasks(1)
          .topicKey(topic.key)
          .workerClusterKey(
            Await
              .result(
                WorkerApi.access.hostname(configurator.hostname).port(configurator.port).list(),
                Duration(20, TimeUnit.SECONDS)
              )
              .head
              .key
          )
          .create()
      )

      // test stop. the connector should be removed
      result(connectorApi.stop(sink.key))
      await(() => result(connectorAdmin.nonExist(sink.key)))
      await(() => result(connectorApi.get(sink.key)).state.isEmpty)
    } finally if (result(connectorAdmin.exist(sink.key))) result(connectorAdmin.delete(sink.key))
  }

  @Test
  def deleteRunningConnector(): Unit = {
    val topicName = CommonUtils.randomString(10)
    val topic = result(
      topicApi.request
        .name(topicName)
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )
    val sink = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .topicKey(topic.key)
        .numberOfTasks(1)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )
    // test start
    result(topicApi.start(topic.key))
    result(connectorApi.start(sink.key))
    val connectorAdmin = ConnectorAdmin(testUtil.workersConnProps)
    try {
      await(
        () =>
          try result(connectorAdmin.exist(sink.key))
          catch {
            case _: Throwable => false
          }
      )
      result(connectorAdmin.delete(sink.key))
      result(connectorAdmin.exist(sink.key)) shouldBe false
    } finally if (result(connectorAdmin.exist(sink.key))) result(connectorAdmin.delete(sink.key))
  }

  @Test
  def testAliveNodes(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )

    val sink = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .numberOfTasks(1)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(topicApi.start(topic.key))
    result(connectorApi.start(sink.key))
    await(() => result(connectorApi.get(sink.key)).state.nonEmpty)
    await(() => result(connectorApi.get(sink.key)).tasksStatus.nonEmpty)
    result(connectorApi.get(sink.key)).aliveNodes should contain(CommonUtils.hostname())
    await(() => result(connectorApi.get(sink.key)).tasksStatus.size == 2) // connector + task
    result(connectorApi.get(sink.key)).tasksStatus.count(_.coordinator) shouldBe 1
    result(connectorApi.get(sink.key)).tasksStatus.filterNot(_.coordinator).size shouldBe 1
    result(connectorApi.get(sink.key)).tasksStatus.foreach(_.nodeName shouldBe CommonUtils.hostname())
  }

  @Test
  def failToRun(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )
    result(topicApi.start(topic.key))
    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .topicKey(topic.key)
        .numberOfTasks(1)
        .setting(FallibleSink.FAILURE_FLAG, JsBoolean(true))
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(connectorApi.start(connector.key))

    CommonUtils.await(() => result(connectorApi.get(connector.key)).state.isDefined, java.time.Duration.ofSeconds(10))

    result(connectorApi.get(connector.key)).state.get shouldBe State.FAILED
    result(connectorApi.get(connector.key)).error should not be None

    // test state in pipeline
    val pipeline = result(
      PipelineApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .endpoints(
          Seq(topic, connector)
        )
        .create()
    )

    result(PipelineApi.access.hostname(configurator.hostname).port(configurator.port).get(pipeline.key)).objects
      .filter(_.key == connector.key)
      .head
      .state
      .get shouldBe State.FAILED.name

    result(PipelineApi.access.hostname(configurator.hostname).port(configurator.port).get(pipeline.key)).objects
      .filter(_.key == connector.key)
      .head
      .error
      .isDefined shouldBe true
  }

  @Test
  def succeedToRun(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )
    result(topicApi.start(topic.key))

    val connector = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .topicKey(topic.key)
        .numberOfTasks(1)
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(connectorApi.start(connector.key))

    CommonUtils.await(() => result(connectorApi.get(connector.key)).state.isDefined, java.time.Duration.ofSeconds(10))

    await(() => result(connectorApi.get(connector.key)).state.contains(State.RUNNING))
    result(connectorApi.get(connector.key)).error shouldBe None

    // test state in pipeline
    val pipeline = result(
      PipelineApi.access
        .hostname(configurator.hostname)
        .port(configurator.port)
        .request
        .name(CommonUtils.randomString(10))
        .endpoints(
          Seq(topic, connector)
        )
        .create()
    )

    result(PipelineApi.access.hostname(configurator.hostname).port(configurator.port).get(pipeline.key)).objects
      .filter(_.key == connector.key)
      .head
      .state
      .get shouldBe State.RUNNING.name

    result(PipelineApi.access.hostname(configurator.hostname).port(configurator.port).get(pipeline.key)).objects
      .filter(_.key == connector.key)
      .head
      .error
      .isEmpty shouldBe true
  }

  @Test
  def testFtpSourceWithIllegalArguments(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )
    result(topicApi.start(topic.key))

    val source = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FtpSource].getName)
        .numberOfTasks(1)
        .topicKey(topic.key)
        .workerClusterKey(workerClusterInfo.key)
        .setting(oharastream.ohara.connector.ftp.FTP_HOSTNAME_KEY, JsString(CommonUtils.randomString()))
        .setting(oharastream.ohara.connector.ftp.FTP_PORT_KEY, JsNumber(CommonUtils.availablePort()))
        .setting(oharastream.ohara.connector.ftp.FTP_USER_NAME_KEY, JsString(CommonUtils.randomString()))
        .setting(oharastream.ohara.connector.ftp.FTP_PASSWORD_KEY, JsString(CommonUtils.randomString()))
        .setting(CsvConnectorDefinitions.INPUT_FOLDER_KEY, JsString(CommonUtils.randomString()))
        .create()
    )

    source.state shouldBe None

    result(connectorApi.start(source.key))
    await(() => result(connectorApi.get(source.key)).state.contains(State.FAILED))
    await(() => result(connectorApi.get(source.key)).error.nonEmpty)
    result(connectorApi.get(source.key)).tasksStatus.isEmpty
  }

  @Test
  def testOnlyTaskFails(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )
    result(topicApi.start(topic.key))
    val source = result(
      connectorApi.request
        .name(CommonUtils.randomString(10))
        .className(classOf[FallibleSink].getName)
        .topicKey(topic.key)
        .numberOfTasks(1)
        .setting(FallibleSinkTask.FAILURE_FLAG, JsBoolean(true))
        .workerClusterKey(workerClusterInfo.key)
        .create()
    )

    result(connectorApi.start(source.key))
    await(() => result(connectorApi.get(source.key)).state.contains(State.FAILED))
    await(() => result(connectorApi.get(source.key)).error.nonEmpty)
    await(() => result(connectorApi.get(source.key)).tasksStatus.nonEmpty)
    result(connectorApi.get(source.key)).tasksStatus.filter(_.coordinator).foreach(_.state shouldBe State.RUNNING)
    result(connectorApi.get(source.key)).tasksStatus.filterNot(_.coordinator).foreach(_.state shouldBe State.FAILED)
  }

  @Disabled("this test case should be enabled by https://github.com/oharastream/ohara/issues/4506")
  @Test
  def testMaximumNumberOfLines(): Unit = {
    val topic = result(
      topicApi.request
        .name(CommonUtils.randomString(10))
        .brokerClusterKey(
          result(BrokerApi.access.hostname(configurator.hostname).port(configurator.port).list()).head.key
        )
        .create()
    )
    result(topicApi.start(topic.key))
    intercept[IllegalArgumentException] {
      result(
        connectorApi.request
          .className(classOf[FtpSource].getName)
          .topicKey(topic.key)
          .numberOfTasks(1)
          .workerClusterKey(workerClusterInfo.key)
          .setting(oharastream.ohara.connector.ftp.FTP_HOSTNAME_KEY, JsString("hostname"))
          .setting(oharastream.ohara.connector.ftp.FTP_PORT_KEY, JsNumber(22))
          .setting(oharastream.ohara.connector.ftp.FTP_USER_NAME_KEY, JsString("user"))
          .setting(oharastream.ohara.connector.ftp.FTP_PASSWORD_KEY, JsString("password"))
          .setting(CsvConnectorDefinitions.INPUT_FOLDER_KEY, JsString("input"))
          .setting(CsvConnectorDefinitions.MAXIMUM_NUMBER_OF_LINES_KEY, JsNumber(-1))
          .create()
      )
    }.getMessage should include(CsvConnectorDefinitions.MAXIMUM_NUMBER_OF_LINES_KEY)

    intercept[IllegalArgumentException] {
      result(
        connectorApi.request
          .className(classOf[FtpSource].getName)
          .topicKey(topic.key)
          .numberOfTasks(1)
          .workerClusterKey(workerClusterInfo.key)
          .setting(oharastream.ohara.connector.ftp.FTP_HOSTNAME_KEY, JsString("hostname"))
          .setting(oharastream.ohara.connector.ftp.FTP_PORT_KEY, JsNumber(22))
          .setting(oharastream.ohara.connector.ftp.FTP_USER_NAME_KEY, JsString("user"))
          .setting(oharastream.ohara.connector.ftp.FTP_PASSWORD_KEY, JsString("password"))
          .setting(CsvConnectorDefinitions.INPUT_FOLDER_KEY, JsString("input"))
          .setting(CsvConnectorDefinitions.MAXIMUM_NUMBER_OF_LINES_KEY, JsNumber(0))
          .create()
      )
    }.getMessage should include(CsvConnectorDefinitions.MAXIMUM_NUMBER_OF_LINES_KEY)
  }

  @AfterEach
  def tearDown(): Unit = Releasable.close(configurator)
}
