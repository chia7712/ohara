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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Collection;
import java.util.stream.Collectors;
import oharastream.ohara.common.exception.Exception;
import oharastream.ohara.kafka.connector.storage.FileSystem;

public abstract class WithMockStorage extends CsvSinkTestBase {
  protected FileSystem fs;

  @Override
  protected void setUp() {
    super.setUp();
    fs = LocalFileSystem.of();
  }

  protected File createTemporaryFolder() {
    try {
      return Files.createTempDirectory("createTemporaryFolder").toFile();
    } catch (IOException e) {
      throw new Exception(e);
    }
  }

  protected Collection<String> readData(String path) {
    InputStream in = fs.open(path);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    return reader.lines().collect(Collectors.toUnmodifiableList());
  }
}
