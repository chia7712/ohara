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

import oharastream.ohara.client.HttpExecutor
import oharastream.ohara.kafka.connector.json.{Creation, Validation}
import spray.json.DefaultJsonProtocol.{jsonFormat2, jsonFormat3, jsonFormat4, _}
import spray.json.{DeserializationException, JsArray, JsNull, JsObject, JsString, JsValue, RootJsonFormat}

/**
  * a collection from marshalling/unmarshalling connector data to/from json.
  * NOTED: the json format is a part from PUBLIC INTERFACE so please don't change the field names after releasing the ohara.
  */
object WorkerJson {
  final case class KafkaPlugin(className: String, typeName: String, version: String)

  /**
    * this custom format is necessary since some keys in json are keywords in scala also...
    */
  private[kafka] implicit val KAFKA_PLUGIN_FORMAT: RootJsonFormat[KafkaPlugin] = new RootJsonFormat[KafkaPlugin] {
    private[this] val classKey: String   = "class"
    private[this] val typeKey: String    = "type"
    private[this] val versionKey: String = "version"

    override def read(json: JsValue): KafkaPlugin = json.asJsObject.getFields(classKey, typeKey, versionKey) match {
      case Seq(JsString(className), JsString(typeName), JsString(version)) =>
        KafkaPlugin(className, typeName, version)
      case other: Any => throw DeserializationException(s"${classOf[KafkaPlugin].getSimpleName} expected but $other")
    }
    override def write(obj: KafkaPlugin) = JsObject(
      classKey   -> JsString(obj.className),
      typeKey    -> JsString(obj.typeName),
      versionKey -> JsString(obj.version)
    )
  }
  final case class KafkaConnectorTaskId(connector: String, task: Int)
  private[kafka] implicit val KAFKA_CONNECTOR_TASK_ID_FORMAT: RootJsonFormat[KafkaConnectorTaskId] = jsonFormat2(
    KafkaConnectorTaskId
  )

  final case class ConnectorCreationResponse(
    name: String,
    config: Map[String, String],
    tasks: Seq[KafkaConnectorTaskId]
  )

  private[kafka] implicit val CONNECTOR_CREATION_RESPONSE_FORMAT: RootJsonFormat[ConnectorCreationResponse] =
    jsonFormat3(ConnectorCreationResponse)
  final case class KafkaConnectorStatus(state: String, worker_id: String, trace: Option[String]) {
    def workerHostname: String = {
      val splitIndex = worker_id.lastIndexOf(":")
      if (splitIndex < 0) worker_id else worker_id.substring(0, splitIndex)
    }
  }
  private[kafka] implicit val KAFKA_CONNECTOR_STATUS_FORMAT: RootJsonFormat[KafkaConnectorStatus] = jsonFormat3(
    KafkaConnectorStatus
  )
  final case class KafkaTaskStatus(id: Int, state: String, worker_id: String, trace: Option[String]) {
    def workerHostname: String = {
      val splitIndex = worker_id.lastIndexOf(":")
      if (splitIndex < 0) worker_id else worker_id.substring(0, splitIndex)
    }
  }
  private[kafka] implicit val KAFKA_TASK_STATUS_FORMAT: RootJsonFormat[KafkaTaskStatus] = jsonFormat4(
    KafkaTaskStatus
  )
  final case class KafkaConnectorInfo(connector: KafkaConnectorStatus, tasks: Seq[KafkaTaskStatus])
  private[kafka] implicit val KAFKA_CONNECTOR_INFO_FORMAT: RootJsonFormat[KafkaConnectorInfo] = jsonFormat2(
    KafkaConnectorInfo
  )

  final case class KafkaError(error_code: Int, message: String) extends HttpExecutor.Error
  private[kafka] implicit val KAFKA_ERROR_RESPONSE_FORMAT: RootJsonFormat[KafkaError] = jsonFormat2(KafkaError)

  final case class KafkaConnectorConfig(
    tasksMax: Int,
    topicNames: Set[String],
    connectorClass: String,
    args: Map[String, String]
  )

  // open to ohara-configurator
  private[ohara] implicit val KAFKA_CONNECTOR_CONFIG_FORMAT: RootJsonFormat[KafkaConnectorConfig] =
    new RootJsonFormat[KafkaConnectorConfig] {
      private[this] val taskMaxKey: String      = "tasks.max"
      private[this] val topicNamesKey: String   = "topics"
      private[this] val connectClassKey: String = "connector.class"

      override def read(json: JsValue): KafkaConnectorConfig =
        json.asJsObject.getFields(taskMaxKey, topicNamesKey, connectClassKey) match {
          // worker saves tasksMax as string
          case Seq(JsString(tasksMax), JsString(topicNames), JsString(connectorClass)) =>
            KafkaConnectorConfig(
              tasksMax = tasksMax.toInt,
              topicNames = topicNames.split(",").toSet,
              connectorClass = connectorClass,
              args = json.convertTo[Map[String, String]] -- Set(taskMaxKey, topicNamesKey, connectClassKey)
            )
          case other: Any =>
            throw DeserializationException(s"${classOf[KafkaConnectorConfig].getSimpleName} expected but $other")
        }
      override def write(config: KafkaConnectorConfig): JsValue =
        JsObject(
          config.args.map(f => f._1 -> JsString(f._2)) ++ Map(
            taskMaxKey      -> JsString(config.tasksMax.toString),
            topicNamesKey   -> JsString(config.topicNames.mkString(",")),
            connectClassKey -> JsString(config.connectorClass)
          )
        )
    }

  case class KafkaValidatedValue(name: String, value: Option[String], errors: Seq[String])

  private[kafka] implicit val KAFKA_VALIDATED_VALUE_FORMAT: RootJsonFormat[KafkaValidatedValue] =
    new RootJsonFormat[KafkaValidatedValue] {
      private[this] val nameKey: String   = "name"
      private[this] val valueKey: String  = "value"
      private[this] val errorsKey: String = "errors"

      override def read(json: JsValue): KafkaValidatedValue = json.asJsObject.getFields(nameKey, errorsKey) match {
        case Seq(JsString(name), JsArray(errors)) =>
          KafkaValidatedValue(
            name = name,
            value = json.asJsObject.fields
              .get(valueKey)
              .flatMap {
                case v: JsString => Some(v.value)
                case JsNull      => None
                case other: Any  => throw DeserializationException(s"unknown format of $valueKey from $other")
              }
              .filter(_.nonEmpty),
            errors = errors.map {
              case error: JsString => error.value
              case _               => throw DeserializationException(s"unknown format of errors:$errors")
            }
          )
        case other: Any =>
          throw DeserializationException(s"${classOf[KafkaValidatedValue].getSimpleName} expected but $other")
      }
      override def write(obj: KafkaValidatedValue) = JsObject(
        nameKey   -> JsString(obj.name),
        valueKey  -> obj.value.map(JsString(_)).getOrElse(JsNull),
        errorsKey -> JsArray(obj.errors.map(JsString(_)).toVector)
      )
    }

  private[kafka] implicit val CREATION_FORMAT: RootJsonFormat[Creation] = new RootJsonFormat[Creation] {
    import spray.json._
    override def write(obj: Creation): JsValue = obj.toJsonString.parseJson
    override def read(json: JsValue): Creation = Creation.ofJson(json.toString())
  }

  private[kafka] implicit val KAFKA_VALIDATION_FORMAT: RootJsonFormat[Validation] =
    new RootJsonFormat[Validation] {
      import spray.json._
      override def write(obj: Validation): JsValue = obj.toJsonString.parseJson
      override def read(json: JsValue): Validation = Validation.ofJson(json.toString())
    }
}
