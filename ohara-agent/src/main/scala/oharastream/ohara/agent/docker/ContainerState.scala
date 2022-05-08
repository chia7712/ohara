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

package oharastream.ohara.agent.docker
import oharastream.ohara.client.Enum

/**
  * The name is a part of "Restful APIs" so "DON'T" change it arbitrarily
  */
abstract sealed class ContainerState(val name: String)
object ContainerState extends Enum[ContainerState] {
  case object PENDING extends ContainerState("PENDING")

  case object CREATED extends ContainerState("CREATED")

  case object RESTARTING extends ContainerState("RESTARTING")

  case object RUNNING extends ContainerState("RUNNING")

  case object REMOVING extends ContainerState("REMOVING")

  case object PAUSED extends ContainerState("PAUSED")

  case object EXITED extends ContainerState("EXITED")

  case object DEAD extends ContainerState("DEAD")
}
