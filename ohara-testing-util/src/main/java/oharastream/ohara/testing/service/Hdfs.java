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

package oharastream.ohara.testing.service;

import java.io.File;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.Releasable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/** HDFS client Using external HDFS or local FileSystem */
public interface Hdfs extends Releasable {

  String hdfsURL();

  String tmpDirectory();

  boolean isLocal();

  FileSystem fileSystem();

  static Hdfs local() {
    return new Hdfs() {
      private final File tempDir = CommonUtils.createTempFolder("local_hdfs");

      @Override
      public void close() {
        CommonUtils.deleteFiles(tempDir);
      }

      @Override
      public String hdfsURL() {
        return new File(tmpDirectory()).toURI().toString();
      }

      @Override
      public String tmpDirectory() {
        return tempDir.getAbsolutePath();
      }

      @Override
      public boolean isLocal() {
        return true;
      }

      @Override
      public FileSystem fileSystem() {
        Configuration config = new Configuration();
        config.set("fs.defaultFS", hdfsURL());
        try {
          return FileSystem.get(config);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
