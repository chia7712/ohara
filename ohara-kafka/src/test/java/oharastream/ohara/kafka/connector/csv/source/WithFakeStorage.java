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

package oharastream.ohara.kafka.connector.csv.source;

import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.COMPLETED_FOLDER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.ERROR_FOLDER_KEY;
import static oharastream.ohara.kafka.connector.csv.CsvConnectorDefinitions.INPUT_FOLDER_KEY;

import com.google.common.collect.Iterators;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.exception.Exception;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.csv.LocalFileSystem;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import org.junit.jupiter.api.Assertions;

public abstract class WithFakeStorage extends CsvSourceTestBase {
  protected static final Path ROOT_FOLDER = createTemporaryFolder();
  protected static final Path INPUT_FOLDER = Paths.get(ROOT_FOLDER.toString(), "input");
  protected static final Path COMPLETED_FOLDER = Paths.get(ROOT_FOLDER.toString(), "completed");
  protected static final Path ERROR_FOLDER = Paths.get(ROOT_FOLDER.toString(), "error");
  protected static final Path INPUT_FILE =
      Paths.get(INPUT_FOLDER.toString(), CommonUtils.randomString(5));

  protected FileSystem storage;

  @Override
  protected Map<String, String> createProps() {
    Map<String, String> props = super.createProps();
    props.put(INPUT_FOLDER_KEY, INPUT_FOLDER.toString());
    props.put(COMPLETED_FOLDER_KEY, COMPLETED_FOLDER.toString());
    props.put(ERROR_FOLDER_KEY, ERROR_FOLDER.toString());
    return props;
  }

  @Override
  protected void setup() {
    super.setup();

    storage = LocalFileSystem.of();
    cleanFolders();
    setupInputFile();
  }

  private static Path createTemporaryFolder() {
    try {
      return Files.createTempDirectory("createTemporaryFolder");
    } catch (IOException e) {
      throw new Exception(e);
    }
  }

  protected void verifyFileSizeInFolder(int expected, Path folder) {
    Assertions.assertEquals(expected, Iterators.size(storage.listFileNames(folder.toString())));
  }

  private void cleanFolders() {
    storage.delete(INPUT_FOLDER.toString(), true);
    storage.delete(COMPLETED_FOLDER.toString(), true);
    storage.delete(ERROR_FOLDER.toString(), true);
    storage.mkdirs(INPUT_FOLDER.toString());
    storage.mkdirs(COMPLETED_FOLDER.toString());
    storage.mkdirs(ERROR_FOLDER.toString());
  }

  protected void setupInputFile() {
    try {
      if (storage.exists(INPUT_FILE.toString())) {
        storage.delete(INPUT_FILE.toString());
      }

      BufferedWriter writer =
          new BufferedWriter(new OutputStreamWriter(storage.create(INPUT_FILE.toString())));

      String header = SCHEMA.stream().map(Column::name).collect(Collectors.joining(","));
      writer.append(header);
      writer.newLine();

      for (String line : INPUT_DATA) {
        writer.append(line);
        writer.newLine();
      }
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
