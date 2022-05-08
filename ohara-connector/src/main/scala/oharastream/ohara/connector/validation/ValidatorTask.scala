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

package oharastream.ohara.connector.validation

import java.util
import java.util.concurrent.TimeUnit

import oharastream.ohara.client.configurator.InspectApi.{RdbInfo, RdbQuery}
import oharastream.ohara.client.configurator.{ErrorApi, InspectApi}
import oharastream.ohara.client.database.DatabaseClient
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.util.VersionUtils
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import spray.json.{JsObject, _}

import scala.jdk.CollectionConverters._
class ValidatorTask extends SourceTask {
  private[this] var done                       = false
  private[this] var props: Map[String, String] = _
  private[this] val topic: String              = InspectApi.INTERNAL_TOPIC_KEY.topicNameOnKafka
  private[this] var requestId: String          = _
  override def start(props: util.Map[String, String]): Unit = {
    this.props = props.asScala.toMap
    requestId = require(InspectApi.REQUEST_ID)
  }

  override def poll(): util.List[SourceRecord] =
    if (done) {
      // just wait the configurator to close this connector
      TimeUnit.SECONDS.sleep(2)
      null
    } else
      try information match {
        case query: RdbQuery => toSourceRecord(validate(query))
      } catch {
        case e: Throwable => toSourceRecord(ErrorApi.of(e))
      } finally done = true

  override def stop(): Unit = {
    // do nothing
  }

  override def version(): String = VersionUtils.VERSION

  private[this] def validate(query: RdbQuery): RdbInfo = {
    val client = DatabaseClient.builder.url(query.url).user(query.user).password(query.password).build
    try RdbInfo(
      name = client.databaseType,
      tables = client.tableQuery
        .catalog(query.catalogPattern.orNull)
        .schema(query.schemaPattern.orNull)
        .tableName(query.tableName.orNull)
        .execute()
    )
    finally client.close()
  }

  private[this] def toJsObject: JsObject = props(InspectApi.SETTINGS_KEY).parseJson.asJsObject
  private[this] def information = require(InspectApi.TARGET_KEY) match {
    case InspectApi.RDB_PREFIX => InspectApi.RDB_QUERY_FORMAT.read(toJsObject)
    case other: String =>
      throw new IllegalArgumentException(
        s"valid targets are ${InspectApi.RDB_PREFIX}. current is $other"
      )
  }

  private[this] def toSourceRecord(data: Object): util.List[SourceRecord] =
    util.Arrays.asList(
      new SourceRecord(
        null,
        null,
        topic,
        Schema.BYTES_SCHEMA,
        Serializer.STRING.to(requestId),
        Schema.BYTES_SCHEMA,
        Serializer.OBJECT.to(data)
      )
    )

  private[this] def require(key: String): String =
    props.getOrElse(key, throw new IllegalArgumentException(s"the $key is required"))
}
