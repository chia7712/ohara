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

package oharastream.ohara.client.filesystem

import java.io.{BufferedReader, BufferedWriter, IOException, InputStreamReader, OutputStreamWriter}
import java.nio.charset.{Charset, StandardCharsets}

import oharastream.ohara.client.filesystem.ftp.FtpFileSystem
import oharastream.ohara.client.filesystem.hdfs.HdfsFileSystem
import oharastream.ohara.client.filesystem.smb.SmbFileSystem
import oharastream.ohara.common.exception.FileSystemException

trait FileSystem extends oharastream.ohara.kafka.connector.storage.FileSystem {
  /**
    * Filter files in the given path using the user-supplied path filter
    *
    * @param dir the path of folder
    * @param filter the user-supplied file name filter
    * @return an array of file names
    */
  def listFileNames(dir: String, filter: FileFilter): Seq[String]

  /**
    * Get the working folder of account. An exception will be thrown if it fails to get working folder.
    * @return current working folder
    */
  def workingFolder(): String

  def nonExists(path: String): Boolean = !exists(path)

  /**
    * recreate a folder. It will delete all stuff under the path.
    * @param dir folder path
    */
  def reMkdirs(dir: String): Unit = {
    if (exists(dir)) delete(dir, true)
    mkdirs(dir)
  }

  /**
    * append message to the end from file. If the file doesn't exist, it will create an new file.
    * @param path file path
    * @param message message
    */
  def attach(path: String, message: String): Unit = attach(path, Seq(message))

  /**
    * append messages to the end from file. If the file doesn't exist, it will create an new file.
    * @param path file path
    * @param messages messages
    */
  def attach(path: String, messages: Seq[String]): Unit = {
    val writer = new BufferedWriter(
      new OutputStreamWriter(if (exists(path)) append(path) else create(path), StandardCharsets.UTF_8),
      messages.map(_.length).sum * 2
    )
    try messages.foreach(line => {
      writer.append(line)
      writer.newLine()
    })
    finally writer.close()
  }

  /**
    * read all content from the path.
    * @param path file path
    * @param encode encode (UTF-8 is default)
    * @return an array from lines
    */
  def readLines(path: String, encode: String = "UTF-8"): Array[String] = {
    val reader = new BufferedReader(new InputStreamReader(open(path), Charset.forName(encode)))
    try Iterator.continually(reader.readLine()).takeWhile(_ != null).toArray
    finally reader.close()
  }

  def wrap[T](f: () => T): T =
    try {
      f()
    } catch {
      case e: IOException           => throw new FileSystemException(e.getMessage, e)
      case e: IllegalStateException => throw new FileSystemException(e.getMessage, e)
    }
}

object FileSystem {
  def hdfsBuilder: HdfsFileSystem.Builder = HdfsFileSystem.builder
  def ftpBuilder: FtpFileSystem.Builder   = FtpFileSystem.builder
  def smbBuilder: SmbFileSystem.Builder   = SmbFileSystem.builder
}
