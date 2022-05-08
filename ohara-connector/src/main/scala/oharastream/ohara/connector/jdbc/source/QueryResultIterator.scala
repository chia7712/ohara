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

package oharastream.ohara.connector.jdbc.source

import java.sql.ResultSet

import oharastream.ohara.client.configurator.InspectApi.RdbColumn
import oharastream.ohara.connector.jdbc.datatype.RDBDataTypeConverter
import oharastream.ohara.connector.jdbc.util.ColumnInfo

class QueryResultIterator(
  rdbDataTypeConverter: RDBDataTypeConverter,
  var resultSet: ResultSet,
  columns: Seq[RdbColumn]
) extends Iterator[Seq[ColumnInfo[_]]] {
  private[this] var cache: Seq[ColumnInfo[_]] = _

  /**
    * this method bring side effect the first time since we have to "touch" remote db to retrieve the "data information"
    * to check the existence from data...
    * @return true if there are some data. otherwise false
    */
  override def hasNext: Boolean = {
    if (cache == null && resultSet.next())
      cache = ResultSetDataConverter.converterRecord(rdbDataTypeConverter, resultSet, columns)
    cache != null
  }

  override def next(): Seq[ColumnInfo[_]] = {
    if (!hasNext) throw new NoSuchElementException("Cache no data")
    else
      try cache
      finally cache = null
  }
}
