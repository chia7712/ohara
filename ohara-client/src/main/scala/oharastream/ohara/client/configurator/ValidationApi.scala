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
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.kafka.connector.json.SettingInfo
import spray.json.{RootJsonFormat, _}

import scala.concurrent.{ExecutionContext, Future}
object ValidationApi {
  val VALIDATION_KIND: String = "validate"

  private[this] implicit val SETTING_INFO_FORMAT: RootJsonFormat[SettingInfo] = new RootJsonFormat[SettingInfo] {
    override def write(obj: SettingInfo): JsValue = obj.toJsonString.parseJson

    override def read(json: JsValue): SettingInfo = SettingInfo.ofJson(json.toString())
  }

  /**
    * used to send the request of validating connector to Configurator.
    */
  abstract class ConnectorRequest extends oharastream.ohara.client.configurator.ConnectorApi.BasicRequest {
    /**
      * used to verify the setting of connector on specific worker cluster
      * @return validation reports
      */
    def verify()(implicit executionContext: ExecutionContext): Future[SettingInfo]
  }

  sealed abstract class BasicNodeRequest {
    protected var port: Option[Int] = None
    protected var hostname: String  = _
    protected var user: String      = _
    protected var password: String  = _

    def port(port: Int): BasicNodeRequest.this.type = {
      this.port = Some(CommonUtils.requireConnectionPort(port))
      this
    }

    def hostname(hostname: String): BasicNodeRequest.this.type = {
      this.hostname = CommonUtils.requireNonEmpty(hostname)
      this
    }

    def user(user: String): BasicNodeRequest.this.type = {
      this.user = CommonUtils.requireNonEmpty(user)
      this
    }

    def password(password: String): BasicNodeRequest.this.type = {
      this.password = CommonUtils.requireNonEmpty(password)
      this
    }
  }

  sealed abstract class Access(prefix: String) extends BasicAccess(prefix) {
    /**
      * start a progress to build a request to validate connector
      * @return request of validating connector
      */
    def connectorRequest: ConnectorRequest
  }

  def access: Access = new Access(VALIDATION_KIND) {
    override def connectorRequest: ConnectorRequest = new ConnectorRequest {
      override def verify()(implicit executionContext: ExecutionContext): Future[SettingInfo] =
        exec.put[oharastream.ohara.client.configurator.ConnectorApi.Creation, SettingInfo, ErrorApi.Error](
          s"$url/${ConnectorApi.PREFIX}",
          creation
        )
    }
  }
}
