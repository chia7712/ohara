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

import oharastream.ohara.common.setting.ObjectKey

import scala.concurrent.Future

/**
  * Do something before the objects does be removed from store actually.
  *
  * Noted: the returned (group, name) can differ from input. And the removed object is associated to the returned stuff.
  */
@FunctionalInterface
trait HookBeforeDelete {
  def apply(key: ObjectKey): Future[Unit]
}
