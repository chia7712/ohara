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

package oharastream.ohara.connector.jdbc.datatype

import oharastream.ohara.client.Enum

sealed abstract class DataTypeEnum
object DataTypeEnum extends Enum[DataTypeEnum] {
  case object INTEGER    extends DataTypeEnum
  case object LONG       extends DataTypeEnum
  case object BOOLEAN    extends DataTypeEnum
  case object FLOAT      extends DataTypeEnum
  case object DOUBLE     extends DataTypeEnum
  case object BIGDECIMAL extends DataTypeEnum
  case object STRING     extends DataTypeEnum
  case object DATE       extends DataTypeEnum
  case object TIME       extends DataTypeEnum
  case object TIMESTAMP  extends DataTypeEnum
  case object BYTES      extends DataTypeEnum
  case object NONETYPE   extends DataTypeEnum
}
