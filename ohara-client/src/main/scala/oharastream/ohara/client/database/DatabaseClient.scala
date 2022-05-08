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

package oharastream.ohara.client.database

import java.sql.{Connection, DriverManager, ResultSet}

import oharastream.ohara.client.configurator.InspectApi.{RdbColumn, RdbTable}
import oharastream.ohara.client.database.DatabaseClient.TableQuery
import oharastream.ohara.common.annotations.{Nullable, Optional}
import oharastream.ohara.common.util.{CommonUtils, Releasable}

import scala.collection.mutable.ArrayBuffer

/**
  * A scala wrap of jdbc connection.
  */
trait DatabaseClient extends Releasable {
  /**
    * a helper method to fetch all table from remote database
    * @return all database readable to user
    */
  def tables(): Seq[RdbTable] = tableQuery.execute()

  /**
    * Query the table from remote database. Please fill the related arguments to reduce the size of data sent by remote database
    * @return query executor
    */
  def tableQuery: TableQuery

  /**
    * @return user name
    */
  def databaseType: String

  def createTable(name: String, schema: Seq[RdbColumn]): Unit

  def dropTable(name: String): Unit

  def connection: Connection
}

object DatabaseClient {
  def builder: Builder = new Builder

  class Builder private[DatabaseClient] extends oharastream.ohara.common.pattern.Builder[DatabaseClient] {
    private[this] var url: String      = _
    private[this] var user: String     = _
    private[this] var password: String = _

    def url(url: String): Builder = {
      this.url = CommonUtils.requireNonEmpty(url)
      this
    }

    @Optional("default is null")
    @Nullable
    def user(user: String): Builder = {
      this.user = user
      this
    }

    @Optional("default is null")
    @Nullable
    def password(password: String): Builder = {
      this.password = password
      this
    }

    override def build: DatabaseClient = new DatabaseClient {
      private[this] def toTableCatalog(rs: ResultSet): String = rs.getString("TABLE_CAT")
      private[this] def toTableSchema(rs: ResultSet): String  = rs.getString("TABLE_SCHEM")
      private[this] def toTableName(rs: ResultSet): String    = rs.getString("TABLE_NAME")
      private[this] def toColumnName(rs: ResultSet): String   = rs.getString("COLUMN_NAME")
      private[this] def toTableType(rs: ResultSet): Seq[String] = {
        val r = rs.getString("TABLE_TYPE")
        if (r == null) Seq.empty
        else r.split(" ").toSeq
      }
      private[this] def systemTable(types: Seq[String]): Boolean = types.contains("SYSTEM")
      private[this] val conn                                     = DriverManager.getConnection(CommonUtils.requireNonEmpty(url), user, password)
      override def databaseType: String = {
        val l = url.indexOf(":")
        if (l < 0) return url
        val r = url.indexOf(":", l + 1)
        if (r < 0) return url
        url.substring(l + 1, r)
      }
      override def createTable(name: String, columns: Seq[RdbColumn]): Unit =
        if (columns.map(_.name).toSet.size != columns.size)
          throw new IllegalArgumentException(s"duplicate order!!!")
        else
          execute(
            s"""CREATE TABLE \"$name\" (""" + columns
              .map(c => s"""\"${c.name}\" ${c.dataType}""")
              .mkString(",") + ", PRIMARY KEY (" + columns
              .filter(_.pk)
              .map(c => s"""\"${c.name}\"""")
              .mkString(",") + "))"
          )

      override def dropTable(name: String): Unit = execute(s"""DROP TABLE \"$name\"""")

      private[this] def execute(query: String): Unit = {
        val state = conn.createStatement()
        try state.execute(query)
        finally state.close()
      }

      override def connection: Connection = conn

      override def close(): Unit = conn.close()

      override def tableQuery: TableQuery = new TableQuery {
        private[this] var catalog: Option[String]   = None
        private[this] var schema: Option[String]    = None
        private[this] var tableName: Option[String] = None

        override def catalog(catalog: Option[String]): this.type = {
          this.catalog = catalog
          this
        }

        override def schema(schema: Option[String]): this.type = {
          this.schema = schema
          this
        }

        override def tableName(tableName: Option[String]): this.type = {
          this.tableName = tableName
          this
        }

        override def execute(): Seq[RdbTable] = {
          val md = conn.getMetaData

          // catalog, schema, tableName
          val data: Seq[(String, String, String)] = {
            implicit val rs: ResultSet = md.getTables(catalog.orNull, schema.orNull, tableName.orNull, null)
            try {
              val buf = new ArrayBuffer[(String, String, String)]()
              while (rs.next()) if (!systemTable(toTableType(rs)))
                buf.append((toTableCatalog(rs), toTableSchema(rs), toTableName(rs)))
              buf.toSeq
            } finally rs.close()
          }

          // catalog, schema, tableName, pks
          val data2 = data.map {
            case (c, s, t) =>
              (c, s, t, {
                implicit val rs: ResultSet = md.getPrimaryKeys(c, null, t)
                try {
                  val buf = new ArrayBuffer[String]()
                  while (rs.next()) buf += toColumnName(rs)
                  buf.toSet
                } finally rs.close()
              })
          }

          data2
            .map {
              case (c, s, t, pks) =>
                implicit val rs: ResultSet = md.getColumns(c, null, t, null)
                val columns = try {
                  val buf                                 = new ArrayBuffer[RdbColumn]()
                  def toColumnType(rs: ResultSet): String = rs.getString("TYPE_NAME")
                  while (rs.next()) buf += RdbColumn(
                    name = toColumnName(rs),
                    dataType = toColumnType(rs),
                    pk = pks.contains(toColumnName(rs))
                  )
                  buf
                } finally rs.close()
                RdbTable(Option(c), Option(s), t, columns.toSeq)
            }
            .filterNot(_.columns.isEmpty)
        }
      }
    }
  }

  /**
    * a simple builder to create a suitable query by fluent pattern.
    */
  trait TableQuery {
    @Optional("default value is null")
    @Nullable
    def catalog(catalog: String): TableQuery.this.type = this.catalog(Option(catalog))

    @Optional("default value is null")
    def catalog(catalog: Option[String]): TableQuery.this.type

    @Optional("default value is null")
    @Nullable
    def schema(schema: String): TableQuery.this.type = this.schema(Option(schema))

    @Optional("default value is null")
    def schema(schema: Option[String]): TableQuery.this.type

    @Optional("default value is null")
    @Nullable
    def tableName(tableName: String): TableQuery.this.type = this.tableName(Option(tableName))

    @Optional("default value is null")
    def tableName(tableName: Option[String]): TableQuery.this.type

    def execute(): Seq[RdbTable]
  }
}
