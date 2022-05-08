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

package oharastream.ohara.client.filesystem.hdfs

import java.io.{InputStream, OutputStream}
import java.nio.file.Paths
import java.util

import com.typesafe.scalalogging.Logger
import oharastream.ohara.client.filesystem.{FileFilter, FileSystem}
import oharastream.ohara.common.exception.NoSuchFileException
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.connector.storage.FileType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter, RemoteIterator}

import scala.jdk.CollectionConverters._

private[filesystem] object HdfsFileSystem {
  def builder: Builder = new Builder

  class Builder private[filesystem] extends oharastream.ohara.common.pattern.Builder[FileSystem] {
    private[this] val LOG                    = Logger(classOf[FileSystem])
    private[this] var url: String            = _
    private[this] var replicationNumber: Int = 3

    def url(value: String): Builder = {
      this.url = CommonUtils.requireNonEmpty(value)
      this
    }

    def replicationNumber(number: Int): Builder = {
      this.replicationNumber = CommonUtils.requirePositiveInt(number);
      this
    }

    override def build: FileSystem = {
      val config = new Configuration()
      config.set("fs.defaultFS", url)
      config.set("dfs.replication", this.replicationNumber.toString())
      new HdfsFileSystemImpl(org.apache.hadoop.fs.FileSystem.get(config))
    }

    private[this] class HdfsFileSystemImpl(hadoopFS: org.apache.hadoop.fs.FileSystem) extends FileSystem {
      /**
        * Returns whether a file or folder exists
        *
        * @param path the path of the file or folder
        * @return true if file or folder exists, false otherwise
        */
      override def exists(path: String): Boolean = wrap { () =>
        hadoopFS.exists(new Path(path))
      }

      /**
        * List the file names of the file system at a given path
        *
        * @param dir the path of the folder
        * @throws IllegalArgumentException if the path does not exist
        * @return the listing of the folder
        */
      override def listFileNames(dir: String): util.Iterator[String] = wrap { () =>
        implicit def convertToScalaIterator[T](underlying: RemoteIterator[T]): Iterator[T] = {
          case class wrapper(underlying: RemoteIterator[T]) extends Iterator[T] {
            override def hasNext: Boolean = underlying.hasNext
            override def next(): T        = underlying.next()
          }
          wrapper(underlying)
        }

        if (nonExists(dir)) throw new NoSuchFileException(s"The path $dir doesn't exist")
        hadoopFS.listLocatedStatus(new Path(dir)).map(fileStatus => fileStatus.getPath.getName).asJava
      }

      /**
        * Filter files in the given path using the user-supplied path filter
        *
        * @param dir the path of folder
        * @param filter the user-supplied file name filter
        * @return an array of file names
        */
      override def listFileNames(dir: String, filter: FileFilter): Seq[String] = wrap { () =>
        if (nonExists(dir)) throw new NoSuchFileException(s"The path $dir doesn't exist")
        val fileFilter: PathFilter = (path: Path) => filter.accept(path.getName)
        hadoopFS.listStatus(new Path(dir), fileFilter).map(_.getPath.getName).toSeq
      }

      /**
        * Creates a new file in the given path
        *
        * @param path the path of the file
        * @throws IllegalArgumentException if a file of that path already exists
        * @return an output stream associated with the new file
        */
      override def create(path: String): OutputStream = wrap { () =>
        LOG.debug(s"HdfsFileSystem.create(${path})")
        if (exists(path)) throw new IllegalArgumentException(s"The path $path already exists")
        val parent = Paths.get(path).getParent.toString
        if (nonExists(parent)) mkdirs(parent)
        hadoopFS.create(new Path(path), false)
      }

      /**
        * Append data to an existing file at the given path
        *
        * @param path the path of the file
        * @throws IllegalArgumentException if the file does not exist
        * @return an output stream associated with the existing file
        */
      override def append(path: String): OutputStream = wrap { () =>
        if (nonExists(path)) throw new NoSuchFileException(s"The path $path doesn't exist")
        hadoopFS.append(new Path(path))
      }

      /**
        * Open for reading an file at the given path
        *
        * @param path the path of the file
        * @throws IllegalArgumentException if the file does not exist
        * @return an input stream with the requested file
        */
      override def open(path: String): InputStream = wrap { () =>
        if (nonExists(path)) throw new NoSuchFileException(s"The path $path doesn't exist")
        hadoopFS.open(new Path(path))
      }

      /**
        * Delete the given file for folder (If empty)
        *
        * @param path path the path to the file or folder to delete
        */
      override def delete(path: String): Unit = wrap { () =>
        delete(path, false)
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
        if (exists(path)) hadoopFS.delete(new Path(path), recursive)
      }

      /**
        * Move or rename a file from source path to target path
        *
        * @param sourcePath the path of the file to move
        * @param targetPath the path of the target file
        * @throws IllegalArgumentException if the source or target file does not exist
        * @return true if object have moved to target path, false otherwise
        */
      override def moveFile(sourcePath: String, targetPath: String): Boolean = wrap { () =>
        if (exists(targetPath)) throw new IllegalArgumentException(s"The target path: $targetPath already exists")
        if (nonExists(sourcePath)) throw new NoSuchFileException(s"The source path: $sourcePath doesn't exist")

        if (sourcePath == targetPath) {
          LOG.error("The source path equals the target path")
          false
        } else hadoopFS.rename(new Path(sourcePath), new Path(targetPath))
      }

      /**
        * Creates folder, including any necessary but nonexistent parent folders
        *
        * @param dir the path of folder
        */
      override def mkdirs(dir: String): Unit = wrap { () =>
        println(s"Harry: HdfsFileSystem.mkdirs(${dir})")
        if (nonExists(dir)) {
          val parent = Paths.get(dir).getParent
          if (parent != null && nonExists(parent.toString)) mkdirs(parent.toString)
          hadoopFS.mkdirs(new Path(dir))
        }
      //if (nonExists(dir)) hadoopFS.mkdirs(new Path(dir))
      }

      /**
        * Get type of the given path
        *
        * @param path the path of file or folder
        * @return a type of the given path
        */
      override def fileType(path: String): FileType = wrap { () =>
        val p = new Path(path)
        if (!hadoopFS.exists(p)) throw new NoSuchFileException(s"$path doesn't exist")
        if (hadoopFS.getFileStatus(p).isDirectory) FileType.FOLDER else FileType.FILE
      }

      /**
        * Get the working folder of account. An exception will be thrown if it fails to get working folder.
        *
        * @return current working folder
        */
      override def workingFolder(): String = wrap { () =>
        hadoopFS.getHomeDirectory.toString
      }

      /** Stop using this file system */
      override def close(): Unit = wrap { () =>
        Releasable.close(hadoopFS)
      }
    }
  }
}
