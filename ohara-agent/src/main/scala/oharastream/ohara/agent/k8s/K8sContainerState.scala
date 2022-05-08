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

/**
  * The name is a part of "Restful APIs" so "DON'T" change it arbitrarily
  */
sealed abstract class K8sContainerState(val name: String)
object K8sContainerState extends Enum[K8sContainerState] {
  case object PENDING extends K8sContainerState("PENDING")

  case object RUNNING extends K8sContainerState("RUNNING")

  case object SUCCEEDED extends K8sContainerState("SUCCEEDED")

  case object FAILED extends K8sContainerState("FAILED")

  case object UNKNOWN extends K8sContainerState("UNKNOWN")
}
