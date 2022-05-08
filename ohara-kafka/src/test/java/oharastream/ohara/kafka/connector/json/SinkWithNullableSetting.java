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

package oharastream.ohara.kafka.connector.json;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.setting.WithDefinitions;
import oharastream.ohara.kafka.connector.RowSinkConnector;
import oharastream.ohara.kafka.connector.RowSinkTask;
import oharastream.ohara.kafka.connector.TaskSetting;

public class SinkWithNullableSetting extends RowSinkConnector {
  private final String version;
  private final String revision;
  private final String author;

  /** open to kafka broker. the inaccessible connector can break the construction of broker. */
  public SinkWithNullableSetting() {
    this("unknown", "unknown", "unknown");
  }

  SinkWithNullableSetting(String version, String revision, String author) {
    this.version = version;
    this.revision = revision;
    this.author = author;
  }

  @Override
  public Class<? extends RowSinkTask> taskClass() {
    return null;
  }

  @Override
  protected List<TaskSetting> taskSettings(int maxTasks) {
    return null;
  }

  @Override
  protected void run(TaskSetting config) {}

  @Override
  protected void terminate() {}

  @Override
  public Map<String, SettingDef> customSettingDefinitions() {
    return Stream.of(
            WithDefinitions.authorDefinition(author),
            WithDefinitions.versionDefinition(version),
            WithDefinitions.revisionDefinition(revision))
        .collect(Collectors.toUnmodifiableMap(SettingDef::key, Function.identity()));
  }
}
