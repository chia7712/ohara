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

package oharastream.ohara.configurator.route.hook

import oharastream.ohara.client.configurator.Data
import oharastream.ohara.client.configurator.BasicCreation

import scala.concurrent.Future

/**
  * this hook is invoked after http request is parsed and converted to scala object.
  *
  * @tparam Creation creation object
  * @tparam Res result to response
  */
@FunctionalInterface
trait HookOfCreation[Creation <: BasicCreation, Res <: Data] {
  def apply(creation: Creation): Future[Res]
}
