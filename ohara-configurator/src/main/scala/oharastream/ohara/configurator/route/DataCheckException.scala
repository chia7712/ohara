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

package oharastream.ohara.configurator.route

import oharastream.ohara.common.setting.ObjectKey

final class DataCheckException(
  val objectType: String,
  val nonexistent: Set[ObjectKey],
  val illegalObjs: Map[ObjectKey, DataCondition]
) extends RuntimeException(
      s"type:$objectType ${nonexistent.map(k => s"$k does not exist").mkString(",")} ${illegalObjs
        .map {
          case (key, condition) =>
            condition match {
              case DataCondition.STOPPED => s"$key MUST be stopped"
              case DataCondition.RUNNING => s"$key MUST be running"
            }
        }
        .mkString(",")}"
    )
