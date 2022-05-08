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

import java.util.Objects

import oharastream.ohara.client.configurator.Data
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.{CommonUtils, Releasable}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * A key-value store. It is used to save the component information
  * NOTED: All implementation from Store should be thread-safe.
  */
trait DataStore extends Releasable {
  /**
    * Noted, the type of stored data must be equal to input type.
    * @param key data key
    * @param executor thread pool
    * @return data associated to type and name
    */
  def get[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Option[T]]

  /**
    * Noted, the type of stored data must be equal to input type.
    * @param key data key
    * @param executor thread pool
    * @return data associated to type and name
    */
  def value[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[T]

  /**
    * Noted, the type of stored data must be equal to input type.
    * @param executor thread pool
    * @return all data associated to type
    */
  def values[T <: Data: ClassTag]()(implicit executor: ExecutionContext): Future[Seq[T]]

  /**
    * @param executor thread pool
    * @return all data
    */
  def raws()(implicit executor: ExecutionContext): Future[Seq[Data]]

  /**
    * @param key data key
    * @param executor thread pool
    * @return all data associated to input name
    */
  def raws(key: ObjectKey)(implicit executor: ExecutionContext): Future[Seq[Data]]

  /**
    * delete an object matching following conditions.
    * 1) type is equal to data type
    * 2) key is equal to data key
    * @param data used to indicate the object to delete
    * @param executor thread pool
    * @return true if it does remove something. Otherwise, false
    */
  def remove(data: Data)(implicit executor: ExecutionContext): Future[Boolean]

  /**
    * Remove a "specified" sublcass from ohara data mapping the name. If the data mapping to the name is not the specified
    * type, an exception will be thrown.
    *
    * @param key from ohara key
    * @tparam T subclass type
    * @return the removed data
    */
  def remove[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Boolean]

  /**
    * add an object in the store. If the name doesn't  exists, an exception will be thrown.
    * Noted, the new one replaces the previous stuff if the data returned by updater has the same group and name.
    * @param newOne used to update data
    * @tparam T type from data
    * @return the updated data
    */
  def addIfPresent[T <: Data: ClassTag](newOne: T)(implicit executor: ExecutionContext): Future[T] =
    addIfPresent[T](newOne.key, (_: T) => newOne)

  /**
    * add an object in the store. If the name doesn't  exists, an exception will be thrown.
    * Noted, the new one replaces the previous stuff if the data returned by updater has the same group and name.
    * @param updater used to update data
    * @tparam T type from data
    * @return the updated data
    */
  def addIfPresent[T <: Data: ClassTag](key: ObjectKey, updater: T => T)(implicit executor: ExecutionContext): Future[T]

  /**
    * add a data associated to name to store. Noted, it throw exception if the input data is already associated to
    * a value.
    * @param data data
    * @param executor thread pool
    * @tparam T data type
    * @return the input data
    */
  def addIfAbsent[T <: Data](data: T)(implicit executor: ExecutionContext): Future[T]

  /**
    * add the key-value even if there is already an existent key-value.
    * @param data data
    * @param executor thread pool
    * @tparam T data type
    * @return the input data
    */
  def add[T <: Data](data: T)(implicit executor: ExecutionContext): Future[T]

  /**
    * Noted, the type of stored data must be equal to input type.
    * @param key data's key
    * @param executor thread pool
    * @tparam T data type
    * @return true if there is an existed data matching type. Otherwise, false
    */
  def exist[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Boolean]

  /**
    * Noted, the type of stored data must be equal to input type.
    * @param key data's key
    * @param executor thread pool
    * @tparam T data type
    * @return false if there is an existed data matching type. Otherwise, true
    */
  def nonExist[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Boolean]

  /**
    * @return the number of stored data
    */
  def size(): Int

  /**
    * @return number of stored data types.
    */
  def numberOfTypes(): Int
}

object DataStore {
  def builder: Builder = new Builder

  def apply(): DataStore = builder.build()

  class Builder private[DataStore] extends oharastream.ohara.common.pattern.Builder[DataStore] {
    private[this] var dataSerializer: Serializer[Data] = new Serializer[Data] {
      override def to(obj: Data): Array[Byte] = Serializer.OBJECT.to(obj)
      override def from(bytes: Array[Byte]): Data =
        Serializer.OBJECT.from(bytes).asInstanceOf[Data]
    }
    private[this] var persistentFolder: String = CommonUtils.createTempFolder("store").getCanonicalPath

    @Optional("default implementation is Serializer.OBJECT")
    def dataSerializer(dataSerializer: Serializer[Data]): Builder = {
      this.dataSerializer = Objects.requireNonNull(dataSerializer)
      this
    }

    @Optional("Default value is a random folder")
    def persistentFolder(persistentFolder: String): Builder = {
      this.persistentFolder = CommonUtils.requireNonEmpty(persistentFolder)
      this
    }

    override def build(): DataStore =
      new RocksDataStore(CommonUtils.requireNonEmpty(persistentFolder), Objects.requireNonNull(dataSerializer))
  }
}
