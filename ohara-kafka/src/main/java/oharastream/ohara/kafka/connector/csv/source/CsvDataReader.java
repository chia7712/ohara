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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.RowSourceContext;
import oharastream.ohara.kafka.connector.RowSourceRecord;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of DataReader for CSV file */
public class CsvDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(CsvDataReader.class);

  private final FileSystem fs;
  private final CsvSourceConfig config;
  private final RowSourceContext context;
  private final CsvOffsetCache offsetCache;

  public static CsvDataReader of(FileSystem fs, CsvSourceConfig config, RowSourceContext context) {
    return new CsvDataReader(fs, config, context);
  }

  public CsvDataReader(FileSystem fs, CsvSourceConfig config, RowSourceContext context) {
    this.fs = fs;
    this.config = config;
    this.context = context;
    this.offsetCache = new CsvOffsetCache();
  }

  @Override
  public List<RowSourceRecord> read(String path) {
    try {
      offsetCache.loadIfNeed(context, path);
      CsvRecordConverter converter =
          CsvRecordConverter.builder()
              .path(path)
              .topicKeys(config.topicKeys())
              .offsetCache(offsetCache)
              .schema(config.columns())
              .maximumNumberOfLines(config.maximumNumberOfLines())
              .build();

      List<RowSourceRecord> records;
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(fs.open(path), Charset.forName(config.encode())))) {
        records = converter.convert(reader.lines());
      }

      // eof so we mark the file as "completed"
      if (records.isEmpty()) handleCompletedFile(path);
      return records;
    } catch (Exception e) {
      LOG.error("failed to handle " + path, e);
      handleErrorFile(path);
      return List.of();
    }
  }

  /**
   * Move the file to the completed folder, or delete it directly.
   *
   * @param path the file path
   */
  private void handleCompletedFile(String path) {
    if (config.completedFolder().isPresent()) {
      try {
        moveToAnotherFolder(path, config.completedFolder().get());
      } catch (Exception e) {
        throw new IllegalStateException(
            "failed to moveFile " + path + " to " + config.completedFolder(), e);
      }
    } else {
      try {
        fs.delete(path);
      } catch (Exception e) {
        throw new IllegalStateException("failed to delete " + path, e);
      }
    }
  }

  /**
   * Move the file to the error folder. If any error occur, only log the error reason. Noted: it
   * does nothing if user don't define the error folder to collect incorrect csv files.
   *
   * @param path the file path
   */
  private void handleErrorFile(String path) {
    config
        .errorFolder()
        .ifPresent(
            folder -> {
              try {
                moveToAnotherFolder(path, folder);
              } catch (Exception e) {
                LOG.error("failed to moveFile " + path + " to " + config.errorFolder(), e);
              }
            });
  }

  private void moveToAnotherFolder(String path, String targetFolder) {
    if (!fs.exists(targetFolder)) fs.mkdirs(targetFolder);
    String fileName = Paths.get(path).getFileName().toString();
    String outputPath = Paths.get(targetFolder, fileName).toString();
    if (fs.exists(outputPath)) {
      String newPath = outputPath + "." + CommonUtils.randomString();
      if (fs.exists(newPath)) {
        throw new IllegalStateException("duplicate file? " + path);
      } else {
        fs.moveFile(path, newPath);
      }
    } else {
      fs.moveFile(path, outputPath);
    }
  }
}
