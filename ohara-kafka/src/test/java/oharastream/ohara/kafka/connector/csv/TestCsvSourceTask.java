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

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import oharastream.ohara.common.exception.NoSuchFileException;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ConnectorKey;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.TaskSetting;
import oharastream.ohara.kafka.connector.csv.source.CsvDataReader;
import oharastream.ohara.kafka.connector.json.ConnectorFormatter;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import oharastream.ohara.kafka.connector.storage.FileType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCsvSourceTask extends OharaTest {
  private final Map<String, String> settings = new HashMap<>();

  @BeforeEach
  public void before() {
    // The setting is fake
    settings.put(CsvConnectorDefinitions.INPUT_FOLDER_KEY, "/input");
    settings.put(CsvConnectorDefinitions.COMPLETED_FOLDER_KEY, "/completed");
    settings.put(CsvConnectorDefinitions.ERROR_FOLDER_KEY, "/error");
    settings.put(CsvConnectorDefinitions.TASK_TOTAL_KEY, "1");
    settings.put(CsvConnectorDefinitions.TASK_HASH_KEY, "10");
    settings.put(CsvConnectorDefinitions.SIZE_OF_FILE_CACHE_KEY, "3");
    settings.put(MockCsvSourceTask.MOCK_HOST_NAME_KEY, "host://");
  }

  @Test
  public void testFileQueue() {
    CsvSourceTask sourceTask = new MockCsvSourceTask();

    sourceTask.run(TaskSetting.of(settings));
    Assertions.assertEquals(sourceTask.fileNameCacheSize(), 0);

    sourceTask.pollRecords();
    // MockCsvSourceTask can't find any data from queue so it should drain all cached files
    Assertions.assertEquals(sourceTask.fileNameCacheSize(), 0);
  }

  @Test
  public void testGetDataReader() {
    CsvSourceTask task = createTask(settings);
    Assertions.assertTrue(task.dataReader() instanceof CsvDataReader);
  }

  @Test
  public void testNoSuchFile() {
    CsvSourceTask sourceTask =
        new MockCsvSourceTask() {
          @Override
          public FileSystem fileSystem(TaskSetting settings) {
            settings.stringValue(MOCK_HOST_NAME_KEY); // For get config test
            return new MockCsvSourceFileSystem() {
              @Override
              public FileType fileType(String path) {
                throw new NoSuchFileException("File doesn't exists");
              }
            };
          }
        };

    // Test continue to process other file
    sourceTask.run(TaskSetting.of(settings));
    sourceTask.pollRecords();
    // MockCsvSourceTask can't find any data from queue so it should drain all cached files
    Assertions.assertEquals(sourceTask.fileNameCacheSize(), 0);
  }

  @Test
  public void testOtherRuntimeException() {
    CsvSourceTask sourceTask =
        new MockCsvSourceTask() {
          @Override
          public FileSystem fileSystem(TaskSetting settings) {
            settings.stringValue(MOCK_HOST_NAME_KEY); // For get config test
            return new MockCsvSourceFileSystem() {
              @Override
              public FileType fileType(String path) {
                throw new RuntimeException("runinng exception");
              }
            };
          }
        };
    sourceTask.run(TaskSetting.of(settings));
    Assertions.assertThrows(RuntimeException.class, sourceTask::pollRecords);
  }

  @Test
  public void testGetDataReader_WithEmptyConfig() {
    Map<String, String> settings = new HashMap<>();
    Assertions.assertThrows(NoSuchElementException.class, () -> createTask(settings));
  }

  private CsvSourceTask createTask(Map<String, String> settings) {
    CsvSourceTask task = new MockCsvSourceTask();
    task.start(
        ConnectorFormatter.of()
            .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
            .topicKey(TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
            .settings(settings)
            .raw());
    return task;
  }
}
