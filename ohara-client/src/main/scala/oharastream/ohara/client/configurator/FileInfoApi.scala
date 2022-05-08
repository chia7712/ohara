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
import java.net.URL
import java.util.Objects

import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import oharastream.ohara.client.configurator.Data
import oharastream.ohara.client.configurator.InspectApi.FileContent
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.setting.{ClassType, ObjectKey, SettingDef, WithDefinitions}
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsString, JsValue, RootJsonFormat, _}

import scala.concurrent.{ExecutionContext, Future}
object FileInfoApi {
  val KIND: String   = SettingDef.Reference.FILE.name().toLowerCase
  val PREFIX: String = "files"

  /**
    * the key used in formData to point out the data.
    */
  val FIELD_NAME: String                           = "file"
  def toString(tags: Map[String, JsValue]): String = JsObject(tags).toString

  /**
    * parse the input string to json representation for tags
    * @param string input string
    * @return json representation of tags
    */
  def toTags(string: String): Map[String, JsValue] = string.parseJson.asJsObject.fields

  case class Updating(override val tags: Option[Map[String, JsValue]]) extends BasicUpdating {
    override def raw: Map[String, JsValue] = UPDATING_FORMAT.write(this).asJsObject.fields
  }
  final implicit val UPDATING_FORMAT: RootJsonFormat[Updating] = jsonFormat1(Updating)

  private[this] implicit val CLASS_TYPE_FORMAT: RootJsonFormat[ClassType] = new RootJsonFormat[ClassType] {
    override def write(obj: ClassType): JsValue = JsString(obj.key())
    override def read(json: JsValue): ClassType = json match {
      case JsString(v) => ClassType.valueOf(v.toUpperCase())
      case _           => throw DeserializationException(s"unmatched type: ${json.getClass.getName}. expected: String")
    }
  }

  case class ClassInfo(classType: ClassType, className: String, settingDefinitions: Seq[SettingDef])
  object ClassInfo {
    /**
      * create connector class information. The type is from setting definitions.
      *
      * @param className class name
      * @param settingDefinitions setting definitions
      * @return class information
      */
    def apply(className: String, settingDefinitions: Seq[SettingDef]): ClassInfo =
      ClassInfo(
        classType = settingDefinitions.find(_.key() == WithDefinitions.KIND_KEY).map(_.defaultString()) match {
          case Some(kind) => ClassType.valueOf(kind.toUpperCase())
          case None       => ClassType.UNKNOWN
        },
        className = className,
        settingDefinitions = settingDefinitions
      )
  }
  implicit val CLASS_INFO_FORMAT: RootJsonFormat[ClassInfo] = jsonFormat3(ClassInfo.apply)

  /**
    * file information
    * @param name file name
    * @param group group name
    * @param url download url
    * @param lastModified last modified time
    */
  final class FileInfo(
    override val group: String,
    override val name: String,
    val url: Option[URL],
    override val lastModified: Long,
    val bytes: Array[Byte],
    val size: Int,
    val classInfos: Seq[ClassInfo],
    override val tags: Map[String, JsValue]
  ) extends Data
      with Serializable {
    def this(
      group: String,
      name: String,
      url: Option[URL],
      lastModified: Long,
      bytes: Array[Byte],
      classInfos: Seq[ClassInfo],
      tags: Map[String, JsValue]
    ) = {
      this(
        group = group,
        name = name,
        url = url,
        lastModified = lastModified,
        bytes = bytes,
        size = bytes.length,
        classInfos = classInfos,
        tags = tags
      )
    }
    private[this] implicit def _classInfos(classInfos: Seq[ClassInfo]): FileContent = FileContent(classInfos)

    def sourceClassInfos: Seq[ClassInfo]      = classInfos.sourceClassInfos
    def sinkClassInfos: Seq[ClassInfo]        = classInfos.sinkClassInfos
    def partitionerClassInfos: Seq[ClassInfo] = classInfos.partitionerClassInfos
    def streamClassInfos: Seq[ClassInfo]      = classInfos.streamClassInfos

    override def kind: String = KIND

    override def raw: Map[String, JsValue] = FILE_INFO_FORMAT.write(this).asJsObject.fields
  }

  private[this] val URL_KEY         = "url"
  private[this] val CLASS_INFOS_KEY = "classInfos"
  private[this] val SIZE_KEY        = "size"

  implicit val FILE_INFO_FORMAT: RootJsonFormat[FileInfo] = JsonRefiner(new RootJsonFormat[FileInfo] {
    override def read(json: JsValue): FileInfo = {
      val fields = json.asJsObject.fields
      def value(key: String): JsValue =
        fields.getOrElse(key, throw DeserializationException(s"key is required", fieldNames = List(key)))
      def string(key: String): String = value(key) match {
        case JsString(s) => s
        case j: JsValue =>
          throw DeserializationException(s"$key must be mapped to JsString but actual is ${j.getClass.getName}")
      }
      def number(key: String): Long = value(key) match {
        case JsNumber(i) => i.toLong
        case j: JsValue =>
          throw DeserializationException(s"$key must be mapped to JsNumber but actual is ${j.getClass.getName}")
      }
      new FileInfo(
        group = string(GROUP_KEY),
        name = string(NAME_KEY),
        url = fields.get(URL_KEY).map(_.convertTo[String]).map(s => new URL(s)),
        lastModified = number(LAST_MODIFIED_KEY),
        /**
          * the json string generated by this format NEVER carries the "bytes" so we assign the empty array to bytes.
          */
        bytes = Array.empty,
        // casting is ok here since the size of file is always smaller than GB.
        size = number(SIZE_KEY).toInt,
        classInfos = value(CLASS_INFOS_KEY) match {
          case JsArray(es) => es.map(CLASS_INFO_FORMAT.read).toList
          case j: JsValue =>
            throw DeserializationException(
              s"$CLASS_INFOS_KEY must be mapped to array but actual is ${j.getClass.getName}"
            )
        },
        tags = value(TAGS_KEY) match {
          case JsObject(fs) => fs
          case j: JsValue =>
            throw DeserializationException(s"$TAGS_KEY must be mapped to JsObject but actual is ${j.getClass.getName}")
        }
      )
    }

    /**
      * skip to serialize the "bytes" since it is a bigger value.
      * @param obj file info
      * @return js object
      */
    override def write(obj: FileInfo): JsValue =
      JsObject(
        Map(
          GROUP_KEY -> JsString(obj.group),
          NAME_KEY  -> JsString(obj.name),
          // the null is removed by json refiner
          URL_KEY           -> obj.url.map(_.toString).map(JsString(_)).getOrElse(JsNull),
          SIZE_KEY          -> JsNumber(obj.size),
          LAST_MODIFIED_KEY -> JsNumber(obj.lastModified),
          CLASS_INFOS_KEY   -> JsArray(obj.classInfos.map(CLASS_INFO_FORMAT.write).toVector),
          TAGS_KEY          -> JsObject(obj.tags)
        )
      )
  })

  sealed trait Request {
    private[this] var group: String              = GROUP_DEFAULT
    private[this] var name: String               = _
    private[this] var file: File                 = _
    private[this] var tags: Map[String, JsValue] = _

    @Optional("default group is GROUP_DEFAULT")
    def group(group: String): Request = {
      this.group = CommonUtils.requireNonEmpty(group)
      this
    }

    @Optional("default will use file name")
    def name(name: String): Request = {
      this.name = CommonUtils.requireNonEmpty(name)
      this
    }

    @Optional("This field is useless in updating")
    def file(file: File): Request = {
      this.file = CommonUtils.requireFile(file)
      this
    }

    @Optional("default is empty tags in creating. And default value is null in updating.")
    def tags(tags: Map[String, JsValue]): Request = {
      this.tags = Objects.requireNonNull(tags)
      this
    }

    def upload()(implicit executionContext: ExecutionContext): Future[FileInfo] = doUpload(
      group = CommonUtils.requireNonEmpty(group),
      file = CommonUtils.requireFile(file),
      name = if (name == null) file.getName else name,
      tags = if (tags == null) Map.empty else tags
    )

    def update()(implicit executionContext: ExecutionContext): Future[FileInfo] = doUpdate(
      group = CommonUtils.requireNonEmpty(group),
      name = CommonUtils.requireNonEmpty(name),
      update = Updating(tags = Option(tags))
    )

    protected def doUpload(group: String, name: String, file: File, tags: Map[String, JsValue])(
      implicit executionContext: ExecutionContext
    ): Future[FileInfo]

    protected def doUpdate(group: String, name: String, update: Updating)(
      implicit executionContext: ExecutionContext
    ): Future[FileInfo]
  }

  final class Access private[configurator] extends BasicAccess(PREFIX) {
    def list()(implicit executionContext: ExecutionContext): Future[Seq[FileInfo]] =
      exec.get[Seq[FileInfo], ErrorApi.Error](url)

    /**
      * get file info mapped to specific name. The group is GROUP_DEFAULT by default.
      * @param key file key
      * @param executionContext thread pool
      * @return file info
      */
    def get(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[FileInfo] =
      exec.get[FileInfo, ErrorApi.Error](urlBuilder.key(key).build())

    /**
      * delete file info mapped to specific name. The group is GROUP_DEFAULT by default.
      * @param key file key
      * @param executionContext thread pool
      * @return file info
      */
    def delete(key: ObjectKey)(implicit executionContext: ExecutionContext): Future[Unit] =
      exec.delete[ErrorApi.Error](urlBuilder.key(key).build())

    /**
      * start a progress to upload file to remote Configurator
      * @return request to process upload
      */
    def request: Request = new Request {
      override protected def doUpload(group: String, name: String, file: File, tags: Map[String, JsValue])(
        implicit executionContext: ExecutionContext
      ): Future[FileInfo] =
        Marshal(
          Multipart.FormData(
            // add file
            Multipart.FormData.BodyPart(
              FIELD_NAME,
              HttpEntity.fromFile(MediaTypes.`application/octet-stream`, file),
              Map("filename" -> name)
            ),
            // add group
            Multipart.FormData.BodyPart(GROUP_KEY, group),
            // add tags
            Multipart.FormData.BodyPart(TAGS_KEY, FileInfoApi.toString(tags))
          )
        ).to[RequestEntity]
          .map(e => HttpRequest(HttpMethods.POST, uri = url, entity = e))
          .flatMap(exec.request[FileInfo, ErrorApi.Error])

      override protected def doUpdate(group: String, name: String, update: Updating)(
        implicit executionContext: ExecutionContext
      ): Future[FileInfo] =
        exec.put[Updating, FileInfo, ErrorApi.Error](urlBuilder.key(ObjectKey.of(group, name)).build(), update)
    }
  }

  def access: Access = new Access
}
