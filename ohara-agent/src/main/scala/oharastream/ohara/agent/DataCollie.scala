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

import oharastream.ohara.client.configurator.Data
import oharastream.ohara.common.setting.ObjectKey

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.{ClassTag, classTag}

/**
  * A readonly interface to DataStore. The write access should be hosted by Configurator only, and hence we have this interface
  * to other components to avoid updating unintentionally.
  */
trait DataCollie {
  /**
    * Noted, the type of stored data must be equal to input type.
    * @param name data name. the group is "default"
    * @param executor thread pool
    * @return data associated to type and name
    */
  def value[T <: Data: ClassTag](name: String)(implicit executor: ExecutionContext): Future[T] =
    value[T](ObjectKey.of("default", name))

  /**
    * Noted, the type of stored data must be equal to input type.
    * @param key data key
    * @param executor thread pool
    * @return data associated to type and name
    */
  def value[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[T]

  /**
    * Noted, the type of stored data must be equal to input type.
    * NOTED: the group is "default"
    * @param executor thread pool
    * @return all data associated to type
    */
  def valuesByNames[T <: Data: ClassTag](names: Set[String])(implicit executor: ExecutionContext): Future[Seq[T]] =
    values[T](names.map(n => ObjectKey.of("default", n)))

  /**
    * Noted, the type of stored data must be equal to input type.
    * @param executor thread pool
    * @return all data associated to type
    */
  def values[T <: Data: ClassTag](keys: Set[ObjectKey])(implicit executor: ExecutionContext): Future[Seq[T]] =
    values[T]().map { ds =>
      keys.foreach { key =>
        if (!ds.exists(_.key == key)) throw new NoSuchElementException(s"$key does not exist")
      }
      ds.filter(d => keys.contains(d.key))
    }

  /**
    * Noted, the type of stored data must be equal to input type.
    * @param executor thread pool
    * @return all data associated to type
    */
  def values[T <: Data: ClassTag]()(implicit executor: ExecutionContext): Future[Seq[T]]
}

object DataCollie {
  /**
    * instantiate a readonly store simply. It is good to our testing.
    * @param objs readonly objs.
    * @return a readonly store
    */
  def apply(objs: Seq[Data]): DataCollie = new DataCollie {
    private[this] def filter[T <: Data: ClassTag]: Seq[T] =
      objs.filter(_.getClass == classTag[T].runtimeClass).map(_.asInstanceOf[T])
    override def value[T <: Data: ClassTag](key: ObjectKey)(implicit executor: ExecutionContext): Future[T] =
      Future.successful(filter[T].find(_.key == key).get)
    override def values[T <: Data: ClassTag]()(implicit executor: ExecutionContext): Future[Seq[T]] =
      Future.successful(filter[T])
  }
}
