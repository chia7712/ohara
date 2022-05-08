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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.PropGroup;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.util.CommonUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.rest.entities.ConfigKeyInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestConnectorDefUtils extends OharaTest {

  @Test
  public void testToConfigDefKey() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .displayName(CommonUtils.randomString())
            .group(CommonUtils.randomString())
            .reference(SettingDef.Reference.WORKER)
            .orderInGroup(111)
            .optional(CommonUtils.randomString())
            .documentation(CommonUtils.randomString())
            .build();
    Assertions.assertEquals(settingDef.key(), ConnectorDefUtils.toConfigKey(settingDef).name);
    Assertions.assertEquals(
        settingDef.valueType().name(), ConnectorDefUtils.toConfigKey(settingDef).type.name());
    Assertions.assertEquals(settingDef.group(), ConnectorDefUtils.toConfigKey(settingDef).group);
    Assertions.assertEquals(
        settingDef.orderInGroup(), ConnectorDefUtils.toConfigKey(settingDef).orderInGroup);
    Assertions.assertEquals(
        settingDef.defaultString(), ConnectorDefUtils.toConfigKey(settingDef).defaultValue);
    Assertions.assertEquals(
        settingDef.documentation(), ConnectorDefUtils.toConfigKey(settingDef).documentation);
  }

  @Test
  public void testToConfigDefKey2() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .displayName(CommonUtils.randomString())
            .group(CommonUtils.randomString())
            .reference(SettingDef.Reference.WORKER)
            .orderInGroup(111)
            .optional(CommonUtils.randomString())
            .documentation(CommonUtils.randomString())
            .build();

    SettingDef another =
        ConnectorDefUtils.of(convertConfigKey(ConnectorDefUtils.toConfigKey(settingDef)));
    Assertions.assertEquals(settingDef, another);
  }

  /**
   * This method is clone from kafka 1.0.2 source code. We need this method to test our conversion.
   */
  private static ConfigKeyInfo convertConfigKey(ConfigDef.ConfigKey configKey) {
    String name = configKey.name;
    ConfigDef.Type type = configKey.type;
    String typeName = configKey.type.name();

    boolean required = false;
    String defaultValue;
    if (ConfigDef.NO_DEFAULT_VALUE.equals(configKey.defaultValue)) {
      defaultValue = null;
      required = true;
    } else {
      defaultValue = ConfigDef.convertToString(configKey.defaultValue, type);
    }
    String importance = configKey.importance.name();
    String documentation = configKey.documentation;
    String group = configKey.group;
    int orderInGroup = configKey.orderInGroup;
    String width = configKey.width.name();
    String displayName = configKey.displayName;
    List<String> dependents = configKey.dependents;
    return new ConfigKeyInfo(
        name,
        typeName,
        required,
        defaultValue,
        importance,
        documentation,
        group,
        orderInGroup,
        width,
        displayName,
        dependents);
  }

  @Test
  public void testDefaultKeyConverter() {
    Assertions.assertEquals(
        ConverterType.NONE.clz.getName(),
        ConnectorDefUtils.DEFAULT
            .get(ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key())
            .defaultString());
  }

  @Test
  public void testDefaultValueConverter() {
    Assertions.assertEquals(
        ConverterType.NONE.clz.getName(),
        ConnectorDefUtils.DEFAULT
            .get(ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key())
            .defaultString());
  }

  @Test
  public void testDefaultHeaderConverter() {
    Assertions.assertEquals(
        ConverterType.NONE.clz.getName(),
        ConnectorDefUtils.DEFAULT
            .get(ConnectorDefUtils.HEADER_CONVERTER_DEFINITION.key())
            .defaultString());
  }

  @Test
  public void parseStaleConfigKeyInfo() {
    ConfigKeyInfo fake = Mockito.mock(ConfigKeyInfo.class);
    Mockito.when(fake.displayName()).thenReturn(CommonUtils.randomString());
    Assertions.assertThrows(IllegalArgumentException.class, () -> ConnectorDefUtils.of(fake));
  }

  @Test
  public void parseStaleConfigKeyInfo2() {
    ConfigKeyInfo fake = Mockito.mock(ConfigKeyInfo.class);
    Mockito.when(fake.displayName()).thenReturn(null);
    Assertions.assertThrows(NullPointerException.class, () -> ConnectorDefUtils.of(fake));
  }

  @Test
  public void testTableValidator() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(SettingDef.Type.TABLE)
            .build();
    ConfigDef.ConfigKey key = ConnectorDefUtils.toConfigKey(settingDef);
    Assertions.assertNotNull(key.validator);
    key.validator.ensureValid(settingDef.key(), null);
    Assertions.assertThrows(
        ConfigException.class, () -> key.validator.ensureValid(settingDef.key(), 123));
    Assertions.assertThrows(
        ConfigException.class,
        () -> key.validator.ensureValid(settingDef.key(), List.of(CommonUtils.randomString())));

    key.validator.ensureValid(
        settingDef.key(),
        PropGroup.ofColumns(
                List.of(
                    Column.builder()
                        .name(CommonUtils.randomString())
                        .dataType(DataType.BOOLEAN)
                        .build()))
            .toJsonString());
  }

  @Test
  public void testTypeConversion() {
    Stream.of(SettingDef.Type.values()).forEach(ConnectorDefUtils::toType);
  }

  @Test
  public void checkReturnBySettingDefShouldBeSame() {
    ConnectorDefUtils.DEFAULT
        .values()
        .forEach(setting -> Assertions.assertNotNull(setting.checker()));
  }

  @Test
  public void testSerialization() {
    ConnectorDefUtils.DEFAULT
        .values()
        .forEach(
            setting -> {
              SettingDef copy = (SettingDef) Serializer.OBJECT.from(Serializer.OBJECT.to(setting));
              Assertions.assertEquals(setting, copy);
            });
  }

  @Test
  public void testConnectorClass() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION))
            .count());
  }

  @Test
  public void testTopics() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.TOPIC_NAMES_DEFINITION))
            .count());
  }

  @Test
  public void testColumns() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.COLUMNS_DEFINITION))
            .count());

    SettingDef def =
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.COLUMNS_DEFINITION))
            .findFirst()
            .get();
    Assertions.assertEquals(4, def.tableKeys().size());
    Assertions.assertEquals(
        Set.of(),
        def.tableKeys().stream()
            .filter(d -> d.name().equals(SettingDef.COLUMN_NEW_NAME_KEY))
            .findFirst()
            .get()
            .recommendedValues());

    Assertions.assertEquals(
        Set.of(),
        def.tableKeys().stream()
            .filter(d -> d.name().equals(SettingDef.COLUMN_NAME_KEY))
            .findFirst()
            .get()
            .recommendedValues());

    Assertions.assertEquals(
        Set.of(),
        def.tableKeys().stream()
            .filter(d -> d.name().equals(SettingDef.COLUMN_ORDER_KEY))
            .findFirst()
            .get()
            .recommendedValues());

    Assertions.assertEquals(
        Stream.of(DataType.values()).map(DataType::name).collect(Collectors.toUnmodifiableSet()),
        def.tableKeys().stream()
            .filter(d -> d.name().equals(SettingDef.COLUMN_DATA_TYPE_KEY))
            .findFirst()
            .get()
            .recommendedValues());
  }

  @Test
  public void testKeyConverter() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.KEY_CONVERTER_DEFINITION))
            .count());
  }

  @Test
  public void testValueConverter() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.VALUE_CONVERTER_DEFINITION))
            .count());
  }

  @Test
  public void testWorkerClusterName() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION))
            .count());
  }

  @Test
  public void testNumberOfTasks() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(d -> d.equals(ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION))
            .count());
  }

  @Test
  public void mustHaveTable() {
    Assertions.assertEquals(
        1,
        ConnectorDefUtils.DEFAULT.values().stream()
            .filter(definition -> definition.valueType().equals(SettingDef.Type.TABLE))
            .count());
  }

  @Test
  public void testPropKeys() {
    ConnectorDefUtils.DEFAULT
        .values()
        .forEach(
            definition -> {
              if (definition.valueType().equals(SettingDef.Type.TABLE)) {
                Assertions.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.COLUMN_ORDER_KEY)));
                Assertions.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.COLUMN_DATA_TYPE_KEY)));
                Assertions.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.COLUMN_NAME_KEY)));
                Assertions.assertTrue(
                    definition.tableKeys().stream()
                        .anyMatch(k -> k.name().equals(SettingDef.COLUMN_NEW_NAME_KEY)));
              } else Assertions.assertTrue(definition.tableKeys().isEmpty());
            });
  }

  @Test
  public void checkDuplicate() {
    Assertions.assertEquals(
        ConnectorDefUtils.DEFAULT.size(),
        ConnectorDefUtils.DEFAULT.values().stream()
            .map(SettingDef::key)
            .collect(Collectors.toUnmodifiableSet())
            .size());
  }

  @Test
  public void testConnectorNameSetting() {
    SettingDef setting =
        ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key());
    Assertions.assertEquals(setting.necessary(), SettingDef.Necessary.RANDOM_DEFAULT);
    Assertions.assertFalse(setting.internal());
    Assertions.assertFalse(setting.hasDefault());
    Assertions.assertEquals(SettingDef.Reference.NONE, setting.reference());
    Assertions.assertTrue(setting.tableKeys().isEmpty());
    Assertions.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }

  @Test
  public void testConnectorKeySetting() {
    SettingDef setting =
        ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.CONNECTOR_KEY_DEFINITION.key());
    Assertions.assertEquals(setting.necessary(), SettingDef.Necessary.REQUIRED);
    Assertions.assertTrue(setting.internal());
    Assertions.assertFalse(setting.hasDefault());
    Assertions.assertEquals(SettingDef.Reference.NONE, setting.reference());
    Assertions.assertEquals(SettingDef.Type.OBJECT_KEY, setting.valueType());
    Assertions.assertTrue(setting.tableKeys().isEmpty());
    Assertions.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }

  @Test
  public void testTagsSetting() {
    SettingDef setting = ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.TAGS_DEFINITION.key());
    Assertions.assertEquals(setting.necessary(), SettingDef.Necessary.OPTIONAL);
    Assertions.assertFalse(setting.internal());
    Assertions.assertFalse(setting.hasDefault());
    Assertions.assertEquals(SettingDef.Reference.NONE, setting.reference());
    Assertions.assertTrue(setting.tableKeys().isEmpty());
    Assertions.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }

  @Test
  public void testTopicKeysSetting() {
    SettingDef setting =
        ConnectorDefUtils.DEFAULT.get(ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key());
    Assertions.assertEquals(setting.necessary(), SettingDef.Necessary.OPTIONAL);
    Assertions.assertFalse(setting.internal());
    Assertions.assertFalse(setting.hasDefault());
    Assertions.assertEquals(SettingDef.Reference.TOPIC, setting.reference());
    Assertions.assertTrue(setting.tableKeys().isEmpty());
    Assertions.assertEquals(ConnectorDefUtils.CORE_GROUP, setting.group());
  }
}
