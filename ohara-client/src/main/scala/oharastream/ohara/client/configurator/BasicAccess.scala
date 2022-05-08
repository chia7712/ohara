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

import java.util.Objects

import oharastream.ohara.client.HttpExecutor
import oharastream.ohara.client.configurator.BasicAccess.UrlBuilder
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.CommonUtils

import scala.concurrent.{ExecutionContext, Future}

/**
  * all accesses in v0 APIs need the hostname and port of remote node. This class implements the basic methods used to store the required
  * information to create the subclass access.
  *
  * @param prefixPath path to remote resource
  */
abstract class BasicAccess private[configurator] (prefixPath: String) {
  protected val exec: HttpExecutor = HttpExecutor.SINGLETON
  // this access is under "v0" package so this field "version" is a constant string.
  private[this] val version      = V0
  protected var hostname: String = _
  protected var port: Int        = -1

  def hostname(hostname: String): BasicAccess.this.type = {
    this.hostname = CommonUtils.requireNonEmpty(hostname)
    this
  }
  def port(port: Int): BasicAccess.this.type = {
    this.port = CommonUtils.requireConnectionPort(port)
    this
  }

  protected final def put(key: ObjectKey, action: String)(implicit executionContext: ExecutionContext): Future[Unit] =
    exec.put[ErrorApi.Error](urlBuilder.key(key).postfix(action).build())

  //-----------------------[URL Helpers]-----------------------//

  /**
    * Compose the url with hostname, port, version and prefix
    * @return url string
    */
  protected final def url: String =
    s"http://${CommonUtils.requireNonEmpty(hostname)}:${CommonUtils
      .requireConnectionPort(port)}/${CommonUtils.requireNonEmpty(version)}/${CommonUtils.requireNonEmpty(prefixPath)}"

  protected def urlBuilder: UrlBuilder = (prefix, key, postfix, params) => {
    var url = BasicAccess.this.url
    prefix.foreach(s => url = s"$url/$s")
    key.foreach(k => url = s"$url/${k.name()}")
    postfix.foreach(s => url = s"$url/$s")
    key.foreach(k => url = s"$url?$GROUP_KEY=${k.group()}")
    val divider = key match {
      case None    => "?"
      case Some(_) => "&"
    }

    if (params.nonEmpty)
      url = url + divider + params
        .map {
          case (key, value) => s"$key=$value"
        }
        .mkString("&")
    url
  }
}

object BasicAccess {
  trait UrlBuilder extends oharastream.ohara.common.pattern.Builder[String] {
    private[this] var prefix: Option[String]      = None
    private[this] var key: Option[ObjectKey]      = None
    private[this] var postfix: Option[String]     = None
    private[this] var params: Map[String, String] = Map.empty

    def prefix(prefix: String): UrlBuilder = {
      this.prefix = Some(CommonUtils.requireNonEmpty(prefix))
      this
    }

    def key(key: ObjectKey): UrlBuilder = {
      this.key = Some(key)
      this
    }

    def postfix(postfix: String): UrlBuilder = {
      this.postfix = Some(CommonUtils.requireNonEmpty(postfix))
      this
    }

    def param(key: String, value: String): UrlBuilder = {
      this.params += (key -> value)
      this
    }

    def params(params: Map[String, String]): UrlBuilder = {
      this.params ++= Objects.requireNonNull(params)
      this
    }

    override def build(): String = doBuild(
      prefix = prefix,
      key = key,
      postfix = postfix,
      params = params
    )

    protected def doBuild(
      prefix: Option[String],
      key: Option[ObjectKey],
      postfix: Option[String],
      params: Map[String, String]
    ): String
  }
}
