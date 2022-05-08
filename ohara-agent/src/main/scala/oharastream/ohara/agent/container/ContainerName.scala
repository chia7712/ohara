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

package oharastream.ohara.agent.container

/**
  * Getting full information of container is a expensive operation. And most cases requires the name, id, image name
  * and node name only. Hence, we separate a class to contain less data to reduce the cost of fetching whole container
  * from remote
  * @param id container id
  * @param name container name
  * @param imageName image name
  * @param nodeName node running this container
  */
final class ContainerName(val id: String, val name: String, val imageName: String, val nodeName: String)
