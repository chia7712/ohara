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

package oharastream.ohara.agent.k8s

import oharastream.ohara.client.Enum

sealed abstract class RestartPolicy
object RestartPolicy extends Enum[RestartPolicy] {
  case object Always extends RestartPolicy {
    override def toString: String = "Always"
  }

  case object OnFailure extends RestartPolicy {
    override def toString: String = "OnFailure"
  }

  case object Never extends RestartPolicy {
    override def toString: String = "Never"
  }
}
