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

package oharastream.ohara.agent

import oharastream.ohara.agent.ArgumentsBuilder.FileAppender
import oharastream.ohara.common.util.CommonUtils
import spray.json.{JsNull, JsNumber, JsString, JsValue}

import scala.collection.mutable

/**
  * used to generate acceptable arguments for all ohara services.
  * the available formats are shown below:
  * 1) --file path=line0,line1
  *    the lines are written to a specific file
  * 2) --env key=value
  *    this prop is exposed to env variable
  */
trait ArgumentsBuilder extends oharastream.ohara.common.pattern.Builder[Seq[String]] {
  /**
    * define single property to single line for specific file
    * @param path file path (in container)
    * @return this builder
    */
  def file(path: String): FileAppender

  /**
    * The script used to start service needs a file having all settings for service.
    * If you call this method, this file will be used to set up the service.
    * @return this appender
    */
  def mainConfigFile(path: String): ArgumentsBuilder

  override def build: Seq[String]
}

object ArgumentsBuilder {
  trait FileAppender {
    private[this] val props                = mutable.Buffer[String]()
    def append(prop: Int): FileAppender    = append(prop.toString)
    def append(prop: String): FileAppender = append(Seq(prop))
    def append(props: Seq[String]): FileAppender = {
      this.props ++= props
      this
    }
    def append(key: String, value: Boolean): FileAppender = append(s"$key=$value")
    def append(key: String, value: Short): FileAppender   = append(s"$key=$value")
    def append(key: String, value: Int): FileAppender     = append(s"$key=$value")
    def append(key: String, value: Long): FileAppender    = append(s"$key=$value")
    def append(key: String, value: String): FileAppender  = append(s"$key=$value")
    def append(key: String, value: JsValue): FileAppender = append(
      key,
      value match {
        case JsString(value) => value
        case JsNumber(value) => value.toString
        case JsNull          => throw new IllegalArgumentException(s"JsNull is not legal")
        case _               => value.toString()
      }
    )

    def done: ArgumentsBuilder = done(props.toSeq)

    protected def done(props: Seq[String]): ArgumentsBuilder
  }
  def apply(): ArgumentsBuilder = new ArgumentsBuilder {
    private[this] val files                  = mutable.Map[String, Seq[String]]()
    private[this] var mainConfigFile: String = _

    override def build: Seq[String] =
      if (CommonUtils.isEmpty(mainConfigFile))
        throw new IllegalArgumentException("you have to define the main configs")
      else
        // format: --file path=line0,line1 --file path1=line0,line1
        // NOTED: the path and props must be in different line. otherwise, k8s will merge them into single line and our
        // script will fail to parse the command-line arguments
        files.flatMap {
          case (path, props) => Seq("--file", s"$path=${props.mkString(",")}")
        }.toSeq ++ Seq("--config", mainConfigFile)

    override def file(path: String): FileAppender = (props: Seq[String]) => {
      this.files += (path -> props)
      this
    }

    override def mainConfigFile(path: String): ArgumentsBuilder = {
      this.mainConfigFile = CommonUtils.requireNonEmpty(path)
      this
    }
  }
}
