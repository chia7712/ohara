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

package oharastream.ohara.client.filesystem.ftp

import java.io.{InputStream, OutputStream}
import java.nio.file.Paths
import java.util
import java.util.Objects
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import oharastream.ohara.client.filesystem.{FileFilter, FileSystem}
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.exception.NoSuchFileException
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.connector.storage.FileType

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

private[filesystem] object FtpFileSystem {
  private[this] lazy val LOG = Logger(getClass.getName)
  def builder: Builder       = new Builder

  class Builder private[filesystem] extends oharastream.ohara.common.pattern.Builder[FileSystem] {
    // private[this] val LOG = Logger(classOf[Ftp])
    private[this] var hostname: String = _

    /**
      * port 21 is used by ftp as default
      */
    private[this] var port: Int              = 21
    private[this] var user: String           = _
    private[this] var password: String       = _
    private[this] var retryTimeout: Duration = Duration(0, TimeUnit.SECONDS)
    private[this] var retryBackoff: Duration = Duration(1, TimeUnit.SECONDS)

    /**
      * ftp server's hostname
      * @return this builder
      */
    def hostname(hostname: String): Builder = {
      this.hostname = CommonUtils.requireNonEmpty(hostname)
      this
    }

    /**
      * ftp server's port
      * @return this builder
      */
    @Optional("default value is 21")
    def port(port: Int): Builder = {
      this.port = CommonUtils.requireConnectionPort(port)
      this
    }

    /**
      * a accessible user
      * @return this builder
      */
    def user(user: String): Builder = {
      this.user = CommonUtils.requireNonEmpty(user)
      this
    }

    /**
      * a accessible password
      * @return this builder
      */
    def password(password: String): Builder = {
      this.password = CommonUtils.requireNonEmpty(password)
      this
    }

    /**
      * disable the retry in connecting to ftp server.
      * @return this builder
      */
    def disableRetry(): Builder = retryTimeout(Duration(0, TimeUnit.SECONDS))

    /**
      * timeout of retrying connection to ftp
      * @param retryTimeout retry timeout
      * @return this builder
      */
    @Optional("default value is zero")
    def retryTimeout(retryTimeout: Duration): Builder = {
      this.retryTimeout = Objects.requireNonNull(retryTimeout)
      this
    }

    /**
      * the time to sleep before retrying
      * @param retryBackoff time to backoff
      * @return this builder
      */
    @Optional("default value is 1 second")
    def retryBackoff(retryBackoff: Duration): Builder = {
      this.retryBackoff = Objects.requireNonNull(retryBackoff)
      this
    }

    override def build: FileSystem = {
      val hostname     = CommonUtils.requireNonEmpty(Builder.this.hostname, () => "hostname can't be null or empty")
      val port         = CommonUtils.requireConnectionPort(Builder.this.port)
      val user         = CommonUtils.requireNonEmpty(Builder.this.user, () => "user can't be null or empty")
      val password     = CommonUtils.requireNonEmpty(Builder.this.password, () => "password can't be null or empty")
      val retryTimeout = Objects.requireNonNull(Builder.this.retryTimeout)
      val retryBackoff = Objects.requireNonNull(Builder.this.retryBackoff)

      new FtpFileSystemImpl(
        FtpClient.builder
          .hostname(hostname)
          .port(port)
          .user(user)
          .password(password)
          .retryTimeout(retryTimeout)
          .retryBackoff(retryBackoff)
          .build
      )
    }

    private[this] class FtpFileSystemImpl(client: FtpClient) extends FileSystem {
      /**
        * Returns whether a file or folder exists
        *
        * @param path the path of the file or folder
        * @return true if file or folder exists, false otherwise
        */
      override def exists(path: String): Boolean = wrap { () =>
        client.exist(path)
      }

      /**
        * List the file names of the file system at a given path
        *
        * @param dir the path of the folder
        * @throws NoSuchFileException if the path does not exist
        * @return the listing of the folder
        */
      override def listFileNames(dir: String): util.Iterator[String] =
        listFileNames(dir, FileFilter.EMPTY).iterator.asJava

      /**
        * Filter files in the given path using the user-supplied path filter
        *
        * @param dir the path of folder
        * @param filter the user-supplied file name filter
        * @return an array of file names
        */
      override def listFileNames(dir: String, filter: FileFilter): Seq[String] = wrap { () =>
        if (nonExists(dir)) throw new NoSuchFileException(s"The path $dir doesn't exist")
        client.listFileNames(dir).filter(filter.accept)
      }

      /**
        * Creates a new file in the given path
        *
        * @param path the path of the file
        * @throws IllegalArgumentException if a file of that path already exists
        * @return an output stream associated with the new file
        */
      override def create(path: String): OutputStream = wrap { () =>
        if (exists(path)) throw new IllegalArgumentException(s"The path ${path} already exists")
        val parent = Paths.get(path).getParent.toString
        if (nonExists(parent)) mkdirs(parent)
        client.create(path)
      }

      /**
        * Append data to an existing file at the given path
        *
        * @param path the path of the file
        * @throws NoSuchFileException if the file does not exist
        * @return an output stream associated with the existing file
        */
      override def append(path: String): OutputStream = wrap { () =>
        if (nonExists(path)) throw new NoSuchFileException(s"The path ${path} doesn't exist")
        client.append(path)
      }

      /**
        * Open for reading an file at the given path
        *
        * @param path the path of the file
        * @throws NoSuchFileException if the file does not exist
        * @return an input stream with the requested file
        */
      override def open(path: String): InputStream = wrap { () =>
        if (nonExists(path)) throw new NoSuchFileException(s"The path ${path} doesn't exist")
        client.open(path)
      }

      /**
        * Delete the given file for folder (If empty)
        *
        * @param path path the path to the file or folder to delete
        */
      override def delete(path: String): Unit = wrap { () =>
        if (exists(path)) client.delete(path)
      }

      /**
        * Delete the given file folder. If recursive is true, **recursively** delete sub folders and
        * files
        *
        * @param path      path path the path to the file or folder to delete
        * @param recursive if path is a folder and set to true, the folder is deleted else throws an
        *                  exception
        */
      override def delete(path: String, recursive: Boolean): Unit = wrap { () =>
        if (exists(path)) if (recursive) {
          if (fileType(path) == FileType.FOLDER)
            listFileNames(path, FileFilter.EMPTY).foreach(fileName => {
              val child = CommonUtils.path(path, fileName)
              delete(child, recursive)
            })
          delete(path)
        } else delete(path)
      }

      /**
        * Move or rename a file from source path to target path
        *
        * @param sourcePath the path of the file to move
        * @param targetPath the path of the target file
        * @throws NoSuchFileException if the source or target file does not exist
        * @return true if object have moved to target path, false otherwise
        */
      override def moveFile(sourcePath: String, targetPath: String): Boolean = wrap { () =>
        if (nonExists(sourcePath)) {
          val errorMessage = s"The source path $sourcePath doesn't exist"
          throw new RuntimeException(errorMessage)
        }
        if (exists(targetPath)) {
          val errorMessage = s"The target path $targetPath already exists"
          throw new RuntimeException(errorMessage)
        }
        if (sourcePath == targetPath) {
          LOG.error("The source path equals the target path")
          false
        } else {
          client.moveFile(sourcePath, targetPath)
          exists(targetPath)
        }
      }

      /**
        * Creates folder, including any necessary but nonexistent parent folders
        *
        * @param dir the path of folder
        */
      override def mkdirs(dir: String): Unit = wrap { () =>
        if (nonExists(dir)) {
          val parent = Paths.get(dir).getParent
          if (parent != null && nonExists(parent.toString)) mkdirs(parent.toString)
          client.mkdir(dir)
        }
      }

      /**
        * Get type of the given path
        *
        * @param path the path of file or folder
        * @return a type of the given path
        */
      override def fileType(path: String): FileType = wrap { () =>
        client.fileType(path)
      }

      /**
        * Get the working folder of account. An exception will be thrown if it fails to get working folder.
        *
        * @return current working folder
        */
      override def workingFolder(): String = wrap { () =>
        client.workingFolder()
      }

      /** Stop using this file system */
      override def close(): Unit = wrap { () =>
        Releasable.close(client)
      }
    }
  }
}
