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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util
import java.util.Objects
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import oharastream.ohara.client.configurator.Data
import oharastream.ohara.common.data.Serializer
import oharastream.ohara.common.setting.ObjectKey
import oharastream.ohara.common.util.Releasable
import org.rocksdb.{ColumnFamilyDescriptor, _}

import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.{ClassTag, classTag}

/**
  * RocksStore is based on Facebook RocksDB. The different type is stored in different column family.
  * @param folder used to store data in disk
  * @param dataSerializer value serializer
  */
private[store] class RocksDataStore(folder: String, dataSerializer: Serializer[Data]) extends DataStore {
  /**
    * ObjectKey is an interface without Serializable mark. Hence, we do the serialization manually.
    * TODO: Should we have a specific case class for ObjectKey to complete the serialization ??? by chia
    */
  private[this] val keySerializer: Serializer[ObjectKey] = new Serializer[ObjectKey] {
    override def to(obj: ObjectKey): Array[Byte] = {
      val bytesBuf = new ByteArrayOutputStream();
      try {
        val writer = new DataOutputStream(bytesBuf)
        try {
          writer.writeUTF(obj.group())
          writer.writeUTF(obj.name())
        } finally writer.close()
        bytesBuf.toByteArray
      } finally bytesBuf.close()
    }
    override def from(bytes: Array[Byte]): ObjectKey = {
      val bytesBuf = new ByteArrayInputStream(bytes)
      try {
        val reader = new DataInputStream(bytesBuf)
        try ObjectKey.of(reader.readUTF(), reader.readUTF())
        finally reader.close()
      } finally bytesBuf.close()
    }
  }
  private[this] val closed            = new AtomicBoolean(false)
  private[this] val classesAndHandles = new ConcurrentHashMap[String, ColumnFamilyHandle]()

  private[this] val db = {
    RocksDB.loadLibrary()
    val cfs = {
      val options = new Options().setCreateIfMissing(true)
      try RocksDB.listColumnFamilies(options, folder)
      finally options.close()
    }.asScala
    val lists   = new util.ArrayList[ColumnFamilyHandle]()
    val options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
    try RocksDB.open(
      options,
      folder,
      (cfs.map(name => new ColumnFamilyDescriptor(name, new ColumnFamilyOptions))
      // RocksDB demands us to define Default column family
        ++ Seq(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions))).asJava,
      lists
    )
    finally {
      options.close()
      lists.asScala.foreach(handler => classesAndHandles.put(new String(handler.getName), handler))
    }
  }

  private[this] def doIfNotClosed[T](action: => T): T =
    if (closed.get())
      throw new RuntimeException("RocksDataStore is closed!!!")
    else action

  private[this] def getOrCreateHandler[T <: Data: ClassTag]: ColumnFamilyHandle =
    getOrCreateHandler(classTag[T].runtimeClass)

  /**
    * get a existent handles or create an new one if the input class is not associated to a existtent handles.
    * Noted: it throws exception if this RocksDB is closed!!!
    * @return handles
    */
  private[this] def getOrCreateHandler(clz: Class[_]): ColumnFamilyHandle =
    doIfNotClosed(
      classesAndHandles
        .computeIfAbsent(clz.getName, name => db.createColumnFamily(new ColumnFamilyDescriptor(name.getBytes)))
    )

  /**
    * collect all handles if RocksDB is not closed.
    * @return collection of handles
    */
  private[this] def handlers(): Seq[ColumnFamilyHandle] = doIfNotClosed(classesAndHandles.values().asScala.toList)

  private[this] def toMap(iter: RocksIterator, firstKey: ObjectKey, endKey: ObjectKey): Map[ObjectKey, Data] =
    try {
      if (firstKey == null) iter.seekToFirst() else iter.seek(toBytes(firstKey))
      Iterator
        .continually(
          if (iter.isValid)
            try Some((toKey(iter.key()), toData(iter.value())))
            finally iter.next()
          else None
        )
        .takeWhile(_.exists {
          case (k, _) => endKey == null || k == endKey
        })
        .flatten
        .toList
        .toMap
    } finally iter.close()

  private[this] def toBytes(key: ObjectKey): Array[Byte] = keySerializer.to(Objects.requireNonNull(key))
  private[this] def toKey(key: Array[Byte]): ObjectKey   = keySerializer.from(Objects.requireNonNull(key))
  private[this] def toBytes(value: Data): Array[Byte]    = dataSerializer.to(Objects.requireNonNull(value))
  private[this] def toData(value: Array[Byte]): Data     = dataSerializer.from(Objects.requireNonNull(value))

  private[this] def _get(handler: ColumnFamilyHandle, key: ObjectKey): Option[Data] =
    Option(db.get(handler, toBytes(key))).map(toData)

  override def get[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Option[T]] =
    Future.successful(_get(getOrCreateHandler[T], key).map(_.asInstanceOf[T]))

  override def value[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[T] =
    get[T](key)
      .map(_.getOrElse(throw new NoSuchElementException(s"$key doesn't exist in ${classTag[T].runtimeClass.getName}")))

  override def values[T <: Data: ClassTag]()(implicit executor: ExecutionContext): Future[Seq[T]] =
    Future.successful(
      toMap(db.newIterator(getOrCreateHandler[T]), null, null)
        .map {
          case (k, v) => k -> v.asInstanceOf[T]
        }
        .values
        .toList
    )

  override def remove(data: Data)(implicit executor: ExecutionContext): Future[Boolean] =
    Future.successful(
      _get(getOrCreateHandler(data.getClass), data.key)
        .map(_ => db.delete(getOrCreateHandler(data.getClass), toBytes(data.key)))
        .isDefined
    )

  override def remove[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Boolean] =
    get[T](key).map { obj =>
      if (obj.isDefined) db.delete(getOrCreateHandler[T], toBytes(key))
      obj.isDefined
    }

  override def addIfPresent[T <: Data: ClassTag](key: ObjectKey, updater: T => T)(
    implicit executor: ExecutionContext
  ): Future[T] =
    value[T](key)
      .map(updater)
      .map(newValue => {
        if (newValue.group != key.group)
          throw new IllegalArgumentException(
            s"""the new group:\"${newValue.group}\" is not equal to group:\"${key.group}\""""
          )
        if (newValue.name != key.name)
          throw new IllegalArgumentException(
            s"""the new name:\"${newValue.name}\" is not equal to name:\"${key.name}\""""
          )
        db.put(getOrCreateHandler(newValue.getClass), toBytes(key), toBytes(newValue))
        newValue
      })

  override def addIfAbsent[T <: Data](data: T)(implicit executor: ExecutionContext): Future[T] =
    if (_get(getOrCreateHandler(data.getClass), ObjectKey.of(data.group, data.name)).isDefined)
      Future.failed(
        new IllegalStateException(s"(${data.group}, ${data.name}} already exists on ${data.getClass.getName}")
      )
    else add(data)

  override def add[T <: Data](data: T)(implicit executor: ExecutionContext): Future[T] =
    Future.successful {
      db.put(getOrCreateHandler(data.getClass), toBytes(ObjectKey.of(data.group, data.name)), toBytes(data))
      data
    }

  override def exist[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Boolean] =
    get[T](key).map(_.isDefined)

  override def nonExist[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[Boolean] =
    get[T](key).map(_.isEmpty)

  override def size(): Int =
    handlers().map { handler =>
      // TODO: is there a counter for rocksdb ???  by chia
      val iter = db.newIterator(handler)
      iter.seekToFirst()
      try {
        var count = 0
        while (iter.isValid) {
          count = count + 1
          iter.next()
        }
        count
      } finally iter.close()
    }.sum

  override def close(): Unit = if (closed.compareAndSet(false, true)) {
    classesAndHandles.values().asScala.foreach(Releasable.close)
    Releasable.close(db)
    classesAndHandles.clear()
  }

  override def raws()(implicit executor: ExecutionContext): Future[Seq[Data]] =
    Future.successful(handlers().flatMap(handler => toMap(db.newIterator(handler), null, null).values.toList))

  override def raws(key: ObjectKey)(implicit executor: ExecutionContext): Future[Seq[Data]] =
    Future.successful(handlers().flatMap(handler => toMap(db.newIterator(handler), key, key).values.toList))

  /**
    * RocksDB has a default cf in creating, and the cf is useless to us so we should not count it.
    * @return number of stored data types.
    */
  override def numberOfTypes(): Int = doIfNotClosed(classesAndHandles.size() - 1)
}
