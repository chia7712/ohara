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

package oharastream.ohara.configurator.fake

import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.agent.{ClusterKind, DataCollie, ShabondiCollie}

import scala.jdk.CollectionConverters._
import scala.concurrent.Future

private[configurator] class FakeShabondiCollie(val containerClient: ContainerClient, dataCollie: DataCollie)
    extends FakeCollie(dataCollie)
    with ShabondiCollie {
  override def creator: ShabondiCollie.ClusterCreator =
    (_, creation) => {
      Future.successful(
        addCluster(
          key = creation.key,
          kind = ClusterKind.SHABONDI,
          nodeNames = creation.nodeNames ++ clusterCache.asScala
            .find(_._1 == creation.key)
            .map(_._2.nodeNames)
            .getOrElse(Set.empty),
          imageName = creation.imageName,
          ports = creation.ports
        )
      )
    }
}
