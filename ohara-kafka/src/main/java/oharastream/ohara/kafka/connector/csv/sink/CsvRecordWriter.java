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

package oharastream.ohara.kafka.connector.csv.sink;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.util.Releasable;
import oharastream.ohara.kafka.connector.RowSinkRecord;
import oharastream.ohara.kafka.connector.storage.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvRecordWriter implements RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CsvRecordWriter.class);

  private final FileSystem fileSystem;
  private final List<Column> schema;
  private final boolean needHeader;
  private final String encode;
  private final Path committedFile;
  private final Path temporaryFile;

  private BufferedWriter bufferedWriter;

  public CsvRecordWriter(
      final CsvSinkConfig config, final String filePath, final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
    this.schema = config.columns();
    this.needHeader = config.needHeader();
    this.encode = config.encode();
    this.committedFile = Paths.get(filePath);
    this.temporaryFile = FileUtils.temporaryFile(committedFile);
  }

  public void write(RowSinkRecord record) {
    LOG.trace("Sink record: {}", record);
    try {
      List<Column> newSchema = RecordUtils.newSchema(schema, record);
      String line = RecordUtils.toLine(newSchema, record);
      if (RecordUtils.isNonEmpty(line)) {
        if (bufferedWriter == null) {
          OutputStream out = fileSystem.create(temporaryFile.toString());
          bufferedWriter = new BufferedWriter(new OutputStreamWriter(out, Charset.forName(encode)));

          if (needHeader) {
            bufferedWriter.append(RecordUtils.toHeader(newSchema));
            bufferedWriter.newLine();
          }
        }

        bufferedWriter.append(line);
        bufferedWriter.newLine();
      }
    } catch (IOException e) {
      LOG.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  public void commit() {
    try {
      bufferedWriter.flush();
      Releasable.close(bufferedWriter);
      fileSystem.moveFile(temporaryFile.toString(), committedFile.toString());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    Releasable.close(bufferedWriter);
  }
}
