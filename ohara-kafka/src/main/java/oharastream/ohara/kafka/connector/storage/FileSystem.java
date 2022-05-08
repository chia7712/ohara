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

package oharastream.ohara.kafka.connector.storage;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import oharastream.ohara.common.util.Releasable;

/** Interface to file system */
public interface FileSystem extends Releasable {

  /**
   * Returns whether a file or folder exists
   *
   * @param path the path of the file or folder
   * @return true if file or folder exists, false otherwise
   */
  boolean exists(String path);

  /**
   * List the file names of the file system at a given path
   *
   * @param dir the path of the folder
   * @throws IllegalArgumentException if the path does not exist
   * @return the listing of the folder
   */
  Iterator<String> listFileNames(String dir);

  /**
   * Get type of the given path
   *
   * @param path the path of file or folder
   * @return a type of the given path
   */
  FileType fileType(String path);

  /**
   * Creates a new file in the given path, including any necessary but nonexistent parent folders
   *
   * @param path the path of the file
   * @throws IllegalArgumentException if a file of that path already exists
   * @return an output stream associated with the new file
   */
  OutputStream create(String path);

  /**
   * Append data to an existing file at the given path
   *
   * @param path the path of the file
   * @throws IllegalArgumentException if the file does not exist
   * @return an output stream associated with the existing file
   */
  OutputStream append(String path);

  /**
   * Open for reading an file at the given path
   *
   * @param path the path of the file
   * @throws IllegalArgumentException if the file does not exist
   * @return an input stream with the requested file
   */
  InputStream open(String path);

  /**
   * Delete the given file for folder (If empty)
   *
   * @param path path the path to the file or folder to delete
   */
  void delete(String path);

  /**
   * Delete the given file folder. If recursive is true, **recursively** delete sub folders and
   * files
   *
   * @param path path path the path to the file or folder to delete
   * @param recursive if path is a folder and set to true, the folder is deleted else throws an
   *     exception
   */
  void delete(String path, boolean recursive);

  /**
   * Move or rename a file from source path to target path
   *
   * @param sourcePath the path of the file to move
   * @param targetPath the path of the target file
   * @throws IllegalArgumentException if the source or target file does not exist
   * @return true if object have moved to target path, false otherwise
   */
  boolean moveFile(String sourcePath, String targetPath);

  /**
   * Creates folder, including any necessary but nonexistent parent folders
   *
   * @param dir the path of folder
   */
  void mkdirs(String dir);

  /** Stop using this file system */
  void close();
}
