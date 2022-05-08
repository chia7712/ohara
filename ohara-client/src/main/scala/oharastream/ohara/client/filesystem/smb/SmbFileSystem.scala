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

package oharastream.ohara.client.filesystem.smb

import java.io.{IOException, InputStream, OutputStream}
import java.nio.file.Paths
import java.util
import java.util.concurrent.TimeUnit

import com.hierynomus.msdtyp.AccessMask
import com.hierynomus.msfscc.FileAttributes.FILE_ATTRIBUTE_DIRECTORY
import com.hierynomus.mssmb2.{SMB2CreateDisposition, SMB2ShareAccess}
import com.hierynomus.protocol.commons.EnumWithValue
import com.hierynomus.smbj.auth.AuthenticationContext
import com.hierynomus.smbj.common.SMBRuntimeException
import com.hierynomus.smbj.connection.Connection
import com.hierynomus.smbj.session.Session
import com.hierynomus.smbj.share.DiskShare
import com.hierynomus.smbj.{SMBClient, SmbConfig}
import oharastream.ohara.client.filesystem.{FileFilter, FileSystem}
import oharastream.ohara.common.annotations.Optional
import oharastream.ohara.common.exception.{NoSuchFileException, FileSystemException}
import oharastream.ohara.common.util.{CommonUtils, Releasable}
import oharastream.ohara.kafka.connector.storage.FileType

import scala.jdk.CollectionConverters._

private[filesystem] object SmbFileSystem {
  def builder: Builder = new Builder

  class Builder private[filesystem] extends oharastream.ohara.common.pattern.Builder[FileSystem] {
    private[this] var hostname: String  = _
    private[this] var port: Int         = 445
    private[this] var user: String      = _
    private[this] var password: String  = _
    private[this] var shareName: String = _

    /**
      * smb server's hostname
      * @return this builder
      */
    def hostname(hostname: String): Builder = {
      this.hostname = CommonUtils.requireNonEmpty(hostname)
      this
    }

    /**
      * smb server's port
      * @return this builder
      */
    @Optional("default value is 445")
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

    def shareName(shareName: String): Builder = {
      this.shareName = CommonUtils.requireNonEmpty(shareName)
      this
    }

    override def build: FileSystem = {
      CommonUtils.requireNonEmpty(hostname, () => "hostname can't be null or empty")
      CommonUtils.requireConnectionPort(port)
      CommonUtils.requireNonEmpty(user, () => "user can't be null or empty")
      CommonUtils.requireNonEmpty(password, () => "password can't be null or empty")
      CommonUtils.requireNonEmpty(shareName, () => "shareName can't be null or empty")
      new SmbFileSystemImpl(hostname, port, user, password, shareName)
    }

    private[this] class SmbFileSystemImpl(
      hostname: String,
      port: Int,
      user: String,
      password: String,
      shareName: String
    ) extends FileSystem {
      private[this] val config: SmbConfig = SmbConfig
        .builder()
        .withTimeout(120, TimeUnit.SECONDS)   // Timeout sets Read, Write, and Transact timeouts (default is 60 seconds)
        .withSoTimeout(180, TimeUnit.SECONDS) // Socket Timeout (default is 0 seconds, blocks forever)
        .build()
      private[this] val client: SMBClient         = new SMBClient(config)
      private[this] val ac: AuthenticationContext = new AuthenticationContext(user, password.toCharArray, null)
      private[this] var connection: Connection    = _
      private[this] var session: Session          = _

      override def wrap[T](f: () => T): T =
        try {
          f()
        } catch {
          case e: IOException           => throw new FileSystemException(e.getMessage, e)
          case e: IllegalStateException => throw new FileSystemException(e.getMessage, e)
          case e: SMBRuntimeException   => throw new FileSystemException(e.getMessage, e)
        }

      private[this] def connectShare[T](f: (DiskShare) => T): T = wrap { () =>
        if (connection == null || !connection.isConnected) {
          connection = client.connect(hostname, port)
          session = connection.authenticate(ac)
        }
        val shareRoot = session.connectShare(shareName).asInstanceOf[DiskShare]
        f(shareRoot)
      }

      /**
        * Returns whether a file or folder exists
        *
        * @param path the path of the file or folder
        * @return true if file or folder exists, false otherwise
        */
      override def exists(path: String): Boolean = connectShare { shareRoot =>
        shareRoot.fileExists(path) || shareRoot.folderExists(path)
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
        * @param dir    the path of folder
        * @param filter the user-supplied file name filter
        * @return an array of file names
        */
      override def listFileNames(dir: String, filter: FileFilter): Seq[String] = connectShare { shareRoot =>
        if (nonExists(dir)) throw new NoSuchFileException(s"${dir} doesn't exist")
        shareRoot
          .list(dir)
          .asScala
          .map(f => f.getFileName)
          .filterNot(_ == ".")
          .filterNot(_ == "..")
          .filter(filter.accept)
          .toSeq
      }

      /**
        * Creates a new file in the given path, including any necessary but nonexistent parent folders
        *
        * @param path the path of the file
        * @throws IllegalArgumentException if a file of that path already exists
        * @return an output stream associated with the new file
        */
      override def create(path: String): OutputStream = connectShare { shareRoot =>
        if (exists(path)) throw new IllegalArgumentException(s"$path exists")

        // create any necessary but nonexistent parent folders
        val parent = Paths.get(path).getParent
        if (parent != null && nonExists(parent.toString)) mkdirs(parent.toString)

        val accessMask        = util.EnumSet.of(AccessMask.GENERIC_WRITE)
        val createDisposition = SMB2CreateDisposition.FILE_CREATE
        val smbFile           = shareRoot.openFile(path, accessMask, null, SMB2ShareAccess.ALL, createDisposition, null)
        val os                = smbFile.getOutputStream()

        // wrap OutputStream. upon a close, also close the File object and Share object.
        new OutputStream {
          override def close(): Unit = {
            Releasable.close(os)
            Releasable.close(smbFile)
            Releasable.close(shareRoot)
          }

          override def write(b: Int): Unit = os.write(b)

          override def flush(): Unit = os.flush()

          override def write(b: Array[Byte]): Unit = os.write(b)

          override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)

          override def equals(obj: scala.Any): Boolean = os.equals(obj)

          override def hashCode(): Int = os.hashCode()

          override def toString: String = os.toString
        }
      }

      /**
        * Append data to an existing file at the given path
        *
        * @param path the path of the file
        * @throws NoSuchFileException if the file does not exist
        * @return an output stream associated with the existing file
        */
      override def append(path: String): OutputStream = connectShare { shareRoot =>
        if (nonExists(path)) throw new NoSuchFileException(s"$path doesn't exist")
        val accessMask: util.Set[AccessMask]         = util.EnumSet.of(AccessMask.GENERIC_WRITE)
        val createDisposition: SMB2CreateDisposition = SMB2CreateDisposition.FILE_OPEN
        val smbFile                                  = shareRoot.openFile(path, accessMask, null, SMB2ShareAccess.ALL, createDisposition, null)
        val os                                       = smbFile.getOutputStream(true)

        // wrap OutputStream. upon a close, also close the File object and Share object.
        new OutputStream {
          override def close(): Unit = {
            Releasable.close(os)
            Releasable.close(smbFile)
            Releasable.close(shareRoot)
          }

          override def write(b: Int): Unit = os.write(b)

          override def flush(): Unit = os.flush()

          override def write(b: Array[Byte]): Unit = os.write(b)

          override def write(b: Array[Byte], off: Int, len: Int): Unit = os.write(b, off, len)

          override def equals(obj: scala.Any): Boolean = os.equals(obj)

          override def hashCode(): Int = os.hashCode()

          override def toString: String = os.toString
        }
      }

      /**
        * Open for reading an file at the given path
        *
        * @param path the path of the file
        * @throws NoSuchFileException if the file does not exist
        * @return an input stream with the requested file
        */
      override def open(path: String): InputStream = connectShare { shareRoot =>
        if (nonExists(path)) throw new NoSuchFileException(s"$path doesn't exist")
        val accessMask: util.Set[AccessMask]         = util.EnumSet.of(AccessMask.GENERIC_READ)
        val createDisposition: SMB2CreateDisposition = SMB2CreateDisposition.FILE_OPEN
        val smbFile                                  = shareRoot.openFile(path, accessMask, null, SMB2ShareAccess.ALL, createDisposition, null)
        val is                                       = smbFile.getInputStream()

        // wrap InputStream. upon a close, also close the File object and Share object.
        new InputStream {
          override def close(): Unit = {
            Releasable.close(is)
            Releasable.close(smbFile)
            Releasable.close(shareRoot)
          }

          override def read(): Int = is.read()

          override def available(): Int = is.available()

          override def mark(readlimit: Int): Unit = is.mark(readlimit)

          override def markSupported(): Boolean = is.markSupported()

          override def read(b: Array[Byte]): Int = is.read(b)

          override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)

          override def reset(): Unit = is.reset()

          override def skip(n: Long): Long = is.skip(n)

          override def equals(obj: scala.Any): Boolean = is.equals(obj)

          override def hashCode(): Int = is.hashCode()

          override def toString: String = is.toString
        }
      }

      /**
        * Delete the given file for folder (If empty)
        *
        * @param path path the path to the file or folder to delete
        */
      override def delete(path: String): Unit = connectShare { shareRoot =>
        if (exists(path)) {
          try {
            fileType(path) match {
              case FileType.FILE   => shareRoot.rm(path)
              case FileType.FOLDER => shareRoot.rmdir(path, false)
            }
          } finally Releasable.close(shareRoot)
        }
      }

      /**
        * Delete the given file folder. If recursive is true, **recursively** delete sub folders and
        * files
        *
        * @param path      path path the path to the file or folder to delete
        * @param recursive if path is a folder and set to true, the folder is deleted else throws an
        *                  exception
        */
      override def delete(path: String, recursive: Boolean): Unit = connectShare { _ =>
        if (recursive) {
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
        * @throws IllegalArgumentException if the source or target file does not exist
        * @return true if object have moved to target path, false otherwise
        */
      override def moveFile(sourcePath: String, targetPath: String): Boolean = connectShare { shareRoot =>
        try {
          val accessMask        = util.EnumSet.of(AccessMask.DELETE, AccessMask.GENERIC_WRITE)
          val createDisposition = SMB2CreateDisposition.FILE_OPEN
          val smbFile           = shareRoot.openFile(sourcePath, accessMask, null, SMB2ShareAccess.ALL, createDisposition, null)
          smbFile.rename(targetPath)
          exists(targetPath)
        } finally Releasable.close(shareRoot)
      }

      /**
        * Creates folder, including any necessary but nonexistent parent folders
        *
        * @param dir the path of folder
        */
      def mkdirs(dir: String): Unit = connectShare { shareRoot =>
        if (nonExists(dir)) {
          val parent = Paths.get(dir).getParent
          if (parent != null) mkdirs(parent.toString)
          shareRoot.mkdir(dir)
        }
      }

      /**
        * Get type of the given path
        *
        * @param path the path of file or folder
        * @return a type of the given path
        */
      override def fileType(path: String): FileType = connectShare { shareRoot =>
        if (!exists(path)) throw new NoSuchFileException(s"$path doesn't exist")
        val fi = shareRoot.getFileInformation(path)
        val isFolder =
          EnumWithValue.EnumUtils.isSet(fi.getBasicInformation.getFileAttributes, FILE_ATTRIBUTE_DIRECTORY)
        if (isFolder) FileType.FOLDER else FileType.FILE
      }

      /**
        * Get the working folder of account. An exception will be thrown if it fails to get working folder.
        *
        * @return current working folder
        */
      override def workingFolder(): String = throw new UnsupportedOperationException

      /** Stop using this file system */
      override def close(): Unit = wrap { () =>
        Releasable.close(client)
      }
    }
  }
}
