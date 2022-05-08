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

package oharastream.ohara.client.kafka

import java.net.HttpRetryException
import java.util.Objects
import java.util.concurrent.TimeUnit

import akka.stream.StreamTcpException
import oharastream.ohara.client.HttpExecutor
import oharastream.ohara.client.configurator.FileInfoApi.ClassInfo
import oharastream.ohara.client.configurator.WorkerApi.WorkerClusterInfo
import oharastream.ohara.client.kafka.WorkerJson._
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.data.Column
import oharastream.ohara.common.setting._
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json._
import com.typesafe.scalalogging.Logger
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Duration

/**
  * a helper class used to send the rest request to kafka worker.
  *
  * Noted: the input key is a specific form across whole project and it is NOT supported by kafka. Hence, the response
  * of this class is "pure" kafka response and a astounding fact is the "connector name" in response is a salt name composed by
  * group and name from input key. For example, the input key is ("g0", "n0") and a salt connector name called "g0-n0"
  * ensues from response.
  */
trait ConnectorAdmin {
  /**
    * start a process to create source/sink connector
    * @return connector creator
    */
  def connectorCreator(): ConnectorAdmin.Creator

  /**
    * start a process to verify the connector
    * @return connector validator
    */
  def connectorValidator(): ConnectorAdmin.Validator

  /**
    * delete a connector from worker cluster
    * @param connectorKey connector's key
    * @return async future containing nothing
    */
  def delete(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * pause a running connector
    * @param connectorKey connector's key
    * @return async future containing nothing
    */
  def pause(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * resume a paused connector
    * @param connectorKey connector's key
    * @return async future containing nothing
    */
  def resume(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit]

  /**
    * list ohara's connector.
    * NOTED: the plugins which are not sub class of ohara connector are not included.
    * @return async future containing connector details
    */
  def connectorDefinitions()(implicit executionContext: ExecutionContext): Future[Map[String, ClassInfo]]

  /**
    * get the definitions for specific connector
    * @param className connector's class name
    * @param executionContext thread pool
    * @return definition
    */
  def connectorDefinition(className: String)(implicit executionContext: ExecutionContext): Future[ClassInfo] =
    connectorDefinitions().map(_.getOrElse(className, throw new NoSuchElementException(s"$className does not exist")))

  /**
    * list available plugin's names
    * @return async future containing connector's names
    */
  def activeConnectors()(implicit executionContext: ExecutionContext): Future[Seq[ConnectorKey]]

  /**
    * @return worker's connection props
    */
  def connectionProps: String

  /**
    * @param connectorKey connector's key
    * @return status of connector
    */
  def status(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[KafkaConnectorInfo]

  /**
    * @param connectorKey connector's key
    * @return configuration of connector
    */
  def config(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[KafkaConnectorConfig]

  /**
    * @param connectorKey connector's key
    * @param id task's id
    * @return task status
    */
  def taskStatus(connectorKey: ConnectorKey, id: Int)(
    implicit executionContext: ExecutionContext
  ): Future[KafkaTaskStatus]

  /**
    * Check whether a connector name is used in creating connector (even if the connector fails to start, this method
    * still return true)
    * @param connectorKey connector key
    * @return true if connector exists
    */
  def exist(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Boolean] =
    activeConnectors().map(_.contains(connectorKey))

  def nonExist(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Boolean] =
    exist(connectorKey).map(!_)
}

object ConnectorAdmin {
  /**
    * THIS METHOD SHOULD BE USED BY TESTING ONLY.
    * Worker cluster key is a required config to ohara connectors. However, there are many tests against "connectors"
    * only so there is no Ohara configurator and worker cluster info. Hence, this method fake all required information
    * to make all ohara connectors work well.
    * @param connectionProps connection props
    * @return worker client
    */
  def apply(connectionProps: String): ConnectorAdmin =
    builder.workerClusterKey(ObjectKey.of("fake", "fake")).connectionProps(connectionProps).build

  /**
    * Generate a worker client with worker cluster meta
    * @param workerClusterInfo worker cluster
    * @return worker client
    */
  def apply(workerClusterInfo: WorkerClusterInfo): ConnectorAdmin =
    builder.workerClusterKey(workerClusterInfo.key).connectionProps(workerClusterInfo.connectionProps).build

  def builder: Builder = new Builder

  private[this] val LOG = Logger(ConnectorAdmin.getClass)

  class Builder private[ConnectorAdmin] extends oharastream.ohara.common.pattern.Builder[ConnectorAdmin] {
    /**
      * set the fake key to following operations. This is workaround to avoid we encounter the error of lacking
      * worker key in communicating with kafka connectors.
      */
    private[this] var workerClusterKey: ObjectKey = ObjectKey.of("fake", "fake")
    private[this] var connectionProps: String     = _
    private[this] var retryLimit: Int             = 1
    private[this] var retryInternal: Duration     = Duration(2, TimeUnit.SECONDS)

    def workerClusterKey(workerClusterKey: ObjectKey): Builder = {
      this.workerClusterKey = Objects.requireNonNull(workerClusterKey)
      this
    }

    def connectionProps(connectionProps: String): Builder = {
      this.connectionProps = CommonUtils.requireNonEmpty(connectionProps)
      this
    }

    @Optional("default value is 1")
    def retryLimit(retryLimit: Int): Builder = {
      this.retryLimit = CommonUtils.requirePositiveInt(retryLimit)
      this
    }

    def disableRetry(): Builder = {
      this.retryLimit = 0
      this
    }

    @Optional("default value is 3 seconds")
    def retryInternal(retryInternal: Duration): Builder = {
      this.retryInternal = Objects.requireNonNull(retryInternal)
      this
    }

    /**
      * Create a default implementation of worker client.
      * NOTED: default implementation use a global akka system to handle http request/response. It means the connection
      * sent by this worker client may be influenced by other instances.
      * @return worker client
      */
    override def build: ConnectorAdmin = new ConnectorAdmin() {
      val connectionProps: String = CommonUtils.requireNonEmpty(Builder.this.connectionProps)
      private[this] def workerAddress: String = {
        val hostAddress = connectionProps.split(",")
        hostAddress(Random.nextInt(hostAddress.size))
      }

      /**
        * kafka worker has weakness of doing consistent operation so it is easy to encounter conflict error. Wrapping all operations with
        * retry can relax us... by chia
        * @param exec do request
        * @tparam T response type
        * @return response
        */
      private[this] def retry[T](exec: () => Future[T], msg: String, retryCount: Int = 0)(
        implicit executionContext: ExecutionContext
      ): Future[T] =
        exec().recoverWith {
          case e @ (_: HttpRetryException | _: StreamTcpException) =>
            LOG.info(s"$msg $retryCount/$retryLimit", e)
            if (retryCount < retryLimit) {
              TimeUnit.MILLISECONDS.sleep(retryInternal.toMillis)
              retry(exec, msg, retryCount + 1)
            } else throw e
        }

      /**
        * Generate the format with basic information
        * @return formatter
        */
      private[this] def format = ConnectorFormatter.of().workerClusterKey(Objects.requireNonNull(workerClusterKey))

      override def connectorCreator(): Creator = new Creator(format) {
        override protected def doCreate(
          executionContext: ExecutionContext,
          creation: Creation
        ): Future[ConnectorCreationResponse] = {
          implicit val exec: ExecutionContext = executionContext
          retry(
            () =>
              HttpExecutor.SINGLETON.post[Creation, ConnectorCreationResponse, KafkaError](
                s"http://$workerAddress/connectors",
                creation
              ),
            "connectorCreator"
          )
        }
      }

      override def delete(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] =
        retry(
          () =>
            HttpExecutor.SINGLETON
              .delete[KafkaError](s"http://$workerAddress/connectors/${connectorKey.connectorNameOnKafka()}"),
          s"delete $connectorKey"
        )

      /**
        * list available plugins.
        * This main difference between plugins() and connectors() is that plugins() spend only one request
        * to remote server. In contrast, connectors() needs multi-requests to fetch all details from
        * remote server. Furthermore, connectors list only sub class from ohara's connectors
        * @return async future containing connector details
        */
      private[this] def plugins()(implicit executionContext: ExecutionContext): Future[Seq[KafkaPlugin]] =
        retry(
          () => HttpExecutor.SINGLETON.get[Seq[KafkaPlugin], KafkaError](s"http://$workerAddress/connector-plugins"),
          s"fetch plugins $workerAddress"
        )

      /**
        * list all definitions for connector.
        * This is a helper method which passing "nothing" to validate the connector and then fetch only the definitions from report
        * @param connectorClassName class name
        * @param executionContext thread pool
        * @return definition list
        */
      private[this] def definitions(
        connectorClassName: String
      )(implicit executionContext: ExecutionContext): Future[Seq[SettingDef]] =
        connectorValidator()
          .className(connectorClassName)
          // kafka 2.x requires topic names for all sink connectors so we add a random topic for this request.
          .topicKey(TopicKey.of("fake_group", "fake_name"))
          .run()
          .map(_.settings().asScala.map(_.definition()).toSeq)

      private[this] def connectorDefinitions(
        classNames: Set[String]
      )(implicit executionContext: ExecutionContext): Future[Map[String, ClassInfo]] =
        Future
          .traverse(classNames)(
            className =>
              definitions(className)
                .map(
                  definitions =>
                    Some(
                      ClassInfo(
                        className = className,
                        settingDefinitions = definitions
                      )
                    )
                )
                .recover {
                  // It should fail if we try to parse non-ohara connectors
                  case _: IllegalArgumentException => None
                }
          )
          .map(_.flatten.map(s => s.className -> s).toMap)

      override def connectorDefinition(
        className: String
      )(implicit executionContext: ExecutionContext): Future[ClassInfo] =
        connectorDefinitions(Set(className))
          .map(_.getOrElse(className, throw new NoSuchElementException(s"$className is not ohara connector")))

      override def connectorDefinitions()(implicit executionContext: ExecutionContext): Future[Map[String, ClassInfo]] =
        plugins()
          .map(_.map(_.className).toSet)
          .flatMap(connectorDefinitions)

      override def activeConnectors()(implicit executionContext: ExecutionContext): Future[Seq[ConnectorKey]] =
        retry(
          () => HttpExecutor.SINGLETON.get[Seq[String], KafkaError](s"http://$workerAddress/connectors"),
          "fetch active connectors"
        ).map(_.flatMap(s => ConnectorKey.ofPlain(s).asScala))

      override def status(connectorKey: ConnectorKey)(
        implicit executionContext: ExecutionContext
      ): Future[KafkaConnectorInfo] = retry(
        () =>
          HttpExecutor.SINGLETON.get[KafkaConnectorInfo, KafkaError](
            s"http://$workerAddress/connectors/${connectorKey.connectorNameOnKafka()}/status"
          ),
        s"status of $connectorKey"
      )

      override def config(connectorKey: ConnectorKey)(
        implicit executionContext: ExecutionContext
      ): Future[KafkaConnectorConfig] = retry(
        () =>
          HttpExecutor.SINGLETON.get[KafkaConnectorConfig, KafkaError](
            s"http://$workerAddress/connectors/${connectorKey.connectorNameOnKafka()}/config"
          ),
        s"config of $connectorKey"
      )

      override def taskStatus(connectorKey: ConnectorKey, id: Int)(
        implicit executionContext: ExecutionContext
      ): Future[KafkaTaskStatus] =
        retry(
          () =>
            HttpExecutor.SINGLETON.get[KafkaTaskStatus, KafkaError](
              s"http://$workerAddress/connectors/${connectorKey.connectorNameOnKafka()}/tasks/$id/status"
            ),
          s"status of $connectorKey/$id"
        )
      override def pause(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] =
        retry(
          () =>
            HttpExecutor.SINGLETON
              .put[KafkaError](s"http://$workerAddress/connectors/${connectorKey.connectorNameOnKafka()}/pause"),
          s"pause $connectorKey"
        )

      override def resume(connectorKey: ConnectorKey)(implicit executionContext: ExecutionContext): Future[Unit] =
        retry(
          () =>
            HttpExecutor.SINGLETON
              .put[KafkaError](s"http://$workerAddress/connectors/${connectorKey.connectorNameOnKafka()}/resume"),
          s"resume $connectorKey"
        )

      override def connectorValidator(): Validator = new Validator(format) {
        override protected def doValidate(
          executionContext: ExecutionContext,
          validation: Validation
        ): Future[SettingInfo] = {
          implicit val exec: ExecutionContext = executionContext
          retry(
            () => {
              if (validation.topicNames().isEmpty)
                throw new IllegalArgumentException(
                  "I'm sorry for this error. However, please fill the topics" +
                    "for your validation request in order to test other settings. This prerequisite is introduced by kafka 2.x"
                )
              HttpExecutor.SINGLETON
                .put[Validation, ConfigInfos, KafkaError](
                  s"http://$workerAddress/connector-plugins/${validation.className()}/config/validate",
                  validation
                )
                .map(SettingInfo.of)
            },
            "connectorValidator"
          )
        }
      }
    }
  }

  /**
    * This is a bridge between java and scala.
    * ConfigInfos is serialized to json by jackson so we can implement the RootJsonFormat easily.
    */
  private[this] implicit val CONFIG_INFOS_FORMAT: RootJsonFormat[ConfigInfos] = new RootJsonFormat[ConfigInfos] {
    import spray.json._
    override def write(obj: ConfigInfos): JsValue = KafkaJsonUtils.toString(obj).parseJson

    override def read(json: JsValue): ConfigInfos = KafkaJsonUtils.toConfigInfos(json.toString())
  }

  /**
    * a base class used to collect the setting from source/sink connector when creating
    */
  abstract class Creator(format: ConnectorFormatter)
      extends oharastream.ohara.common.pattern.Creator[Future[ConnectorCreationResponse]] {
    private[this] var executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

    /**
      * set the connector key. It should be a unique key.
      *
      * @param connectorKey connector key
      * @return this one
      */
    def connectorKey(connectorKey: ConnectorKey): Creator = {
      format.connectorKey(connectorKey)
      this
    }

    /**
      * set the connector class. The class must be loaded in class loader otherwise it will fail to create the connector.
      *
      * @param className connector class
      * @return this one
      */
    def className(className: String): Creator = {
      format.className(className)
      this
    }

    /**
      * set the connector class. The class must be loaded in class loader otherwise it will fail to create the connector.
      *
      * @param clz connector class
      * @return this one
      */
    def connectorClass[T](clz: Class[T]): Creator = className(Objects.requireNonNull(clz).getName)

    /**
      * the max number from sink task you want to create
      *
      * @param numberOfTasks max number from sink task
      * @return this one
      */
    @Optional("default is 1")
    def numberOfTasks(numberOfTasks: Int): Creator = {
      format.numberOfTasks(numberOfTasks)
      this
    }

    @Optional("default is empty")
    def setting(key: String, value: String): Creator = {
      format.setting(key, value)
      this
    }

    /**
      * extra setting passed to sink connector. This setting is optional.
      *
      * @param settings setting
      * @return this one
      */
    @Optional("default is empty")
    def settings(settings: Map[String, String]): Creator = {
      format.settings(settings.asJava)
      this
    }

    /**
      * set the columns
      * @param columns columns
      * @return this builder
      */
    @Optional("default is all columns")
    def columns(columns: Seq[Column]): Creator = {
      format.columns(columns.asJava)
      this
    }

    def topicKey(topicKey: TopicKey): Creator = topicKeys(Set(topicKey))

    def topicKeys(topicKeys: Set[TopicKey]): Creator = {
      format.topicKeys(topicKeys.asJava)
      this
    }

    def workerClusterKey(clusterKey: ObjectKey): Creator = {
      this.format.workerClusterKey(clusterKey)
      this
    }

    /**
      * set the thread pool used to execute request
      * @param executionContext thread pool
      * @return this creator
      */
    @Optional("default pool is scala.concurrent.ExecutionContext.Implicits.global")
    def threadPool(executionContext: ExecutionContext): Creator = {
      this.executionContext = Objects.requireNonNull(executionContext)
      this
    }

    /**
      * send the request to create the sink connector.
      *
      * @return this one
      */
    override def create(): Future[ConnectorCreationResponse] =
      doCreate(
        executionContext = Objects.requireNonNull(executionContext),
        creation = format.requestOfCreation()
      )

    /**
      * send the request to kafka worker
      *
      * @return response
      */
    protected def doCreate(
      executionContext: ExecutionContext,
      creation: Creation
    ): Future[ConnectorCreationResponse]
  }

  abstract class Validator(formatter: ConnectorFormatter) {
    def className(className: String): Validator = {
      this.formatter.className(CommonUtils.requireNonEmpty(className))
      this
    }

    def connectorClass(clz: Class[_]): Validator = className(clz.getName)

    @Optional("Default is none")
    def setting(key: String, value: String): Validator =
      settings(Map(CommonUtils.requireNonEmpty(key) -> CommonUtils.requireNonEmpty(value)))

    @Optional("Default is none")
    def settings(settings: Map[String, String]): Validator = {
      this.formatter.settings(CommonUtils.requireNonEmpty(settings.asJava))
      this
    }

    def topicKey(topicKey: TopicKey): Validator = topicKeys(Set(topicKey))

    /**
      * set the topic in which you have interest.
      *
      * @param topicKeys topic keys
      * @return this one
      */
    @Optional("Default is none")
    def topicKeys(topicKeys: Set[TopicKey]): Validator = {
      formatter.topicKeys(topicKeys.asJava)
      this
    }

    /**
      * the max number from sink task you want to create
      *
      * @param numberOfTasks max number from sink task
      * @return this one
      */
    @Optional("Default is none")
    def numberOfTasks(numberOfTasks: Int): Validator = {
      this.formatter.numberOfTasks(CommonUtils.requirePositiveInt(numberOfTasks))
      this
    }

    /**
      * set the columns
      * @param columns columns
      * @return this builder
      */
    @Optional("Default is none")
    def columns(columns: Seq[Column]): Validator = {
      this.formatter.columns(CommonUtils.requireNonEmpty(columns.asJava))
      this
    }

    def connectorKey(connectorKey: ConnectorKey): Validator = {
      this.formatter.connectorKey(connectorKey)
      this
    }

    def run()(implicit executionContext: ExecutionContext): Future[SettingInfo] = doValidate(
      executionContext = Objects.requireNonNull(executionContext),
      validation = formatter.requestOfValidation()
    )

    /**
      * send the request to kafka worker
      *
      * @return response
      */
    protected def doValidate(executionContext: ExecutionContext, validation: Validation): Future[SettingInfo]
  }
}
