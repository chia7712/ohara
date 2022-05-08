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

import oharastream.ohara.agent._
import oharastream.ohara.client.configurator.NodeApi.Node
import scala.concurrent.{ExecutionContext, Future}

// accessible to configurator
private[ohara] class K8SServiceCollieImpl(dataCollie: DataCollie, val containerClient: K8SClient)
    extends ServiceCollie {
  override val zookeeperCollie: ZookeeperCollie = new K8SBasicCollieImpl(dataCollie, containerClient)
    with ZookeeperCollie

  override val brokerCollie: BrokerCollie = new K8SBasicCollieImpl(dataCollie, containerClient) with BrokerCollie

  override val workerCollie: WorkerCollie = new K8SBasicCollieImpl(dataCollie, containerClient) with WorkerCollie

  override val streamCollie: StreamCollie = new K8SBasicCollieImpl(dataCollie, containerClient) with StreamCollie

  override val shabondiCollie: ShabondiCollie = new K8SBasicCollieImpl(dataCollie, containerClient) with ShabondiCollie

  override def verifyNode(node: Node)(implicit executionContext: ExecutionContext): Future[String] =
    containerClient
      .checkNode(node.name)
      .map(report => {
        val statusInfo = report.statusInfo.getOrElse(K8SStatusInfo(false, s"${node.name} node doesn't exists."))
        if (statusInfo.isHealth) s"${node.name} node is running."
        else
          throw new IllegalStateException(s"${node.name} node doesn't running container. cause: ${statusInfo.message}")
      })

  override def close(): Unit = {
    // do nothing
  }
}
