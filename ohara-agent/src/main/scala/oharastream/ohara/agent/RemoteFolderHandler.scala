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

package oharastream.ohara.agent

import oharastream.ohara.client.configurator.NodeApi.Node
import oharastream.ohara.common.util.Releasable

import scala.concurrent.{ExecutionContext, Future}

trait RemoteFolderHandler {
  /**
    * Test whether the exist folder
    * @param hostname remote host name
    * @param path folder path
    * @param executionContext thread pool
    * @return True is exist, false is not exist
    */
  def exist(hostname: String, path: String)(implicit executionContext: ExecutionContext): Future[Boolean]

  /**
    * Create folder for the remote node
    * @param hostname remote host name
    * @param path new folder path
    * @param executionContext thread pool
    * @return true if it does create a folder. Otherwise, false
    */
  def create(hostname: String, path: String)(implicit executionContext: ExecutionContext): Future[Boolean]

  /**
    * List folder info for the remote node
    * @param hostname remote host name
    * @param path remote folder path
    * @param executionContext thread pool
    * @return folder names
    */
  def list(hostname: String, path: String)(implicit executionContext: ExecutionContext): Future[Set[String]]

  /**
    * get the details of folder
    * @param hostname hostname
    * @param path folder path
    * @param executionContext thread pool
    * @return folder information
    */
  def inspect(hostname: String, path: String)(implicit executionContext: ExecutionContext): Future[FolderInfo]

  /**
    * Delete folder for the remote node
    * @param hostname remote host name
    * @param path delete folder path
    * @param executionContext thread pool
    * @return true if it does delete a folder. Otherwise, false
    */
  def delete(hostname: String, path: String)(implicit executionContext: ExecutionContext): Future[Boolean]
}

object RemoteFolderHandler {
  def apply(dataCollie: DataCollie): RemoteFolderHandler = new RemoteFolderHandler {
    private[this] def exist(agent: Agent, path: String): Boolean =
      // it returns the "path" if the "path" is a file
      try !agent
      // export LANG=en_US.UTF-8: The return message is always english.
        .execute(s"ls $path")
        .map(_.trim)
        .contains(path)
      catch {
        case e: Throwable if e.getMessage.contains(path) => false
      }

    override def exist(hostname: String, path: String)(
      implicit executionContext: ExecutionContext
    ): Future[Boolean] = agent(hostname)(agent => exist(agent, path))

    override def create(hostname: String, path: String)(
      implicit executionContext: ExecutionContext
    ): Future[Boolean] =
      agent(hostname) { agent =>
        if (!exist(agent, path)) {
          agent.execute(s"mkdir -p $path")
          true
        } else false
      }

    override def inspect(hostname: String, path: String)(
      implicit executionContext: ExecutionContext
    ): Future[FolderInfo] = agent(hostname) { agent =>
      if (!exist(agent, path)) throw new NoSuchElementException(s"$path does not exist")
      // ex: drwx------ 16 chia7712 chia7712
      val detail = agent
        .execute(s"ls -ld $path")
        .getOrElse(throw new IllegalArgumentException(s"failed to get permission of $path"))

      // ex: drwx------
      val permission = detail.substring(0, 3) match {
        case "drw" => FolderPermission.READWRITE
        case "dr-" => FolderPermission.READONLY
        case _     => FolderPermission.UNKNOWN
      }

      val splits = detail.split(" ")
      if (splits.length < 4) throw new IllegalArgumentException(s"illegal permission:$detail of $path")
      val owner = splits(2)
      val group = splits(3)

      val size = try agent
        .execute(s"du -sb $path")
        // take last string since something inaccessible happens before size count
        .map(_.split("\n").last)
        // ex: 613878290	/home/chia7712
        .map(_.split("\t").head.toLong)
      catch {
        case _: Throwable =>
          // TODO: recursively count folder is a dangerous operation but we can't handle all exception currently.
          None
      }

      val name = agent
        .execute(s"basename $path")
        .getOrElse(throw new IllegalArgumentException(s"failed to get name of $path"))
        .replaceAll("\n", "")

      val uid = agent
        .execute(s"id -u $owner")
        .getOrElse(throw new IllegalArgumentException(s"failed to get uid of $owner"))
        .replaceAll("\n", "")
        .toInt

      FolderInfo(
        permission = permission,
        owner = owner,
        group = group,
        uid = uid,
        size = size,
        name = name
      )
    }

    override def list(hostname: String, path: String)(
      implicit executionContext: ExecutionContext
    ): Future[Set[String]] =
      agent(hostname) { agent =>
        if (exist(agent, path)) {
          val subFolders = agent
            .execute(s"find $path -maxdepth 1 -type d")
            .getOrElse(throw new IllegalArgumentException(s"failed to get name of $path"))
            .split("\n")
            .toSeq
          subFolders.slice(1, subFolders.size).toSet
        } else throw new NoSuchElementException(s"$path is not a folder")
      }

    override def delete(hostname: String, path: String)(
      implicit executionContext: ExecutionContext
    ): Future[Boolean] =
      agent(hostname)(
        agent =>
          if (exist(agent, path)) {
            agent
              .execute(s"rm -rf $path")
              .map(message => throw new IllegalArgumentException(s"Delete folder error: $message"))
            true
          } else false
      )

    private[this] def agent[T](
      hostname: String
    )(f: Agent => T)(implicit executionContext: ExecutionContext): Future[T] =
      dataCollie
        .value[Node](hostname)
        .map { node =>
          Agent.builder
            .hostname(node.hostname)
            .user(node.user)
            .password(node.password)
            .port(node.port)
            .build
        }
        .map(
          agent =>
            try f(agent)
            finally Releasable.close(agent)
        )
  }
}
