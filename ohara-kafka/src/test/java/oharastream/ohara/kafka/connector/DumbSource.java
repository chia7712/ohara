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

package oharastream.ohara.kafka.connector;

import java.util.Arrays;
import java.util.List;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.setting.ConnectorKey;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.json.ConnectorFormatter;

public class DumbSource extends RowSourceConnector {
  private final List<Column> columns =
      Arrays.asList(
          Column.builder().name("cf0").dataType(DataType.BOOLEAN).order(0).build(),
          Column.builder().name("cf1").dataType(DataType.BOOLEAN).order(1).build());

  @Override
  public Class<? extends RowSourceTask> taskClass() {
    return DumbSourceTask.class;
  }

  @Override
  protected List<TaskSetting> taskSettings(int maxTasks) {
    return List.of(
        TaskSetting.of(
            ConnectorFormatter.of()
                .connectorKey(
                    ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
                .topicKey(TopicKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
                .columns(columns)
                .raw()));
  }

  @Override
  protected void run(TaskSetting config) {}

  @Override
  protected void terminate() {}
}
