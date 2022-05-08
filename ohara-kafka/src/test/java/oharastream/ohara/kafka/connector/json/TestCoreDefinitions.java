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

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.setting.WithDefinitions;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.DumbSink;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCoreDefinitions extends OharaTest {

  @Test
  public void testClassNameDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key =
        sink.config().configKeys().get(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key());
    Assertions.assertEquals(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key(), key.name);
    Assertions.assertEquals(
        ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.orderInGroup(), key.orderInGroup);
    Assertions.assertEquals(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.group(), key.group);
    Assertions.assertEquals(
        ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.valueType().name(), key.type.name());
  }

  @Test
  public void testTopicNamesDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key =
        sink.config().configKeys().get(ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key());
    Assertions.assertEquals(ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key(), key.name);
    Assertions.assertEquals(
        ConnectorDefUtils.TOPIC_NAMES_DEFINITION.orderInGroup(), key.orderInGroup);
    Assertions.assertEquals(ConnectorDefUtils.TOPIC_NAMES_DEFINITION.group(), key.group);
    Assertions.assertEquals(
        ConnectorDefUtils.TOPIC_NAMES_DEFINITION.valueType(), SettingDef.Type.ARRAY);
  }

  @Test
  public void testNumberOfTasksDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key =
        sink.config().configKeys().get(ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key());
    Assertions.assertEquals(ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key(), key.name);
    Assertions.assertEquals(
        ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.orderInGroup(), key.orderInGroup);
    Assertions.assertEquals(ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.group(), key.group);
    Assertions.assertEquals(
        ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.valueType().name(),
        SettingDef.Type.POSITIVE_INT.name());
  }

  @Test
  public void testWorkerClusterNameDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key =
        sink.config().configKeys().get(ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key());
    Assertions.assertEquals(ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key(), key.name);
    Assertions.assertEquals(
        ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.orderInGroup(), key.orderInGroup);
    Assertions.assertEquals(ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.group(), key.group);
  }

  @Test
  public void testColumnsDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key =
        sink.config().configKeys().get(ConnectorDefUtils.COLUMNS_DEFINITION.key());
    Assertions.assertEquals(ConnectorDefUtils.COLUMNS_DEFINITION.key(), key.name);
    Assertions.assertEquals(ConnectorDefUtils.COLUMNS_DEFINITION.orderInGroup(), key.orderInGroup);
    Assertions.assertEquals(ConnectorDefUtils.COLUMNS_DEFINITION.group(), key.group);
    // the TABLE is mapped to STRING
    Assertions.assertEquals(SettingDef.Type.STRING.name(), key.type.name());
  }

  @Test
  public void testVersionDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key = sink.config().configKeys().get(WithDefinitions.VERSION_KEY);
    Assertions.assertEquals(WithDefinitions.VERSION_KEY, key.name);
    Assertions.assertEquals(WithDefinitions.VERSION_ORDER, key.orderInGroup);
  }

  @Test
  public void testRevisionDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key = sink.config().configKeys().get(WithDefinitions.REVISION_KEY);
    Assertions.assertEquals(WithDefinitions.REVISION_KEY, key.name);
    Assertions.assertEquals(WithDefinitions.REVISION_ORDER, key.orderInGroup);
  }

  @Test
  public void testAuthorDefinition() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key = sink.config().configKeys().get(WithDefinitions.AUTHOR_KEY);
    Assertions.assertEquals(WithDefinitions.AUTHOR_KEY, key.name);
    Assertions.assertEquals(WithDefinitions.AUTHOR_ORDER, key.orderInGroup);
  }

  @Test
  public void testIdSetting() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key =
        sink.config().configKeys().get(ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key());
    Assertions.assertEquals(ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key(), key.name);
    Assertions.assertEquals(
        ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.orderInGroup(), key.orderInGroup);
    Assertions.assertEquals(ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.group(), key.group);
    Assertions.assertEquals(
        ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.valueType().name(), key.type.name());
  }

  @Test
  public void testConnectorType() {
    DumbSink sink = new DumbSink();
    ConfigDef.ConfigKey key = sink.config().configKeys().get(WithDefinitions.KIND_KEY);
    Assertions.assertEquals(WithDefinitions.KIND_KEY, key.name);
    Assertions.assertEquals(WithDefinitions.KIND_ORDER, key.orderInGroup);
    Assertions.assertEquals(WithDefinitions.META_GROUP, key.group);
    Assertions.assertEquals(SettingDef.Type.STRING.name(), key.type.name());
  }

  @Test
  public void nullVersionInSource() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new SourceWithNullableSetting(
                    null, CommonUtils.randomString(), CommonUtils.randomString())
                .config());
  }

  @Test
  public void nullRevisionInSource() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new SourceWithNullableSetting(
                    CommonUtils.randomString(), null, CommonUtils.randomString())
                .config());
  }

  @Test
  public void nullAuthorInSource() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new SourceWithNullableSetting(
                    CommonUtils.randomString(), CommonUtils.randomString(), null)
                .config());
  }

  @Test
  public void nullVersionInSink() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new SourceWithNullableSetting(
                    null, CommonUtils.randomString(), CommonUtils.randomString())
                .config());
  }

  @Test
  public void nullRevisionInSink() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new SinkWithNullableSetting(
                    CommonUtils.randomString(), null, CommonUtils.randomString())
                .config());
  }

  @Test
  public void nullAuthorInSink() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new SinkWithNullableSetting(
                    CommonUtils.randomString(), CommonUtils.randomString(), null)
                .config());
  }
}
