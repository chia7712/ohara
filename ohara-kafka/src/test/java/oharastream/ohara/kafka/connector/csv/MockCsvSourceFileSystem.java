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

package oharastream.ohara.kafka.connector.csv;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import oharastream.ohara.kafka.connector.storage.FileType;

class MockCsvSourceFileSystem implements FileSystem {

  @Override
  public boolean exists(String path) {
    return false;
  }

  @Override
  public Iterator<String> listFileNames(String dir) {
    return IntStream.range(1, 100)
        .boxed()
        .map(i -> "file" + i)
        .collect(Collectors.toUnmodifiableList())
        .iterator();
  }

  @Override
  public FileType fileType(String path) {
    return FileType.FILE;
  }

  @Override
  public OutputStream create(String path) {
    throw new UnsupportedOperationException("Mock not support this function");
  }

  @Override
  public OutputStream append(String path) {
    throw new UnsupportedOperationException("Mock not support this function");
  }

  @Override
  public InputStream open(String path) {
    throw new UnsupportedOperationException("Mock not support this function");
  }

  @Override
  public void delete(String path) {
    throw new UnsupportedOperationException("Mock not support this function");
  }

  @Override
  public void delete(String path, boolean recursive) {
    throw new UnsupportedOperationException("Mock not support this function");
  }

  @Override
  public boolean moveFile(String sourcePath, String targetPath) {
    throw new UnsupportedOperationException("Mock not support this function");
  }

  @Override
  public void mkdirs(String dir) {
    throw new UnsupportedOperationException("Mock not support this function");
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException("Mock not support this function");
  }
}
