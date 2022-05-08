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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.setting.ConnectorKey;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.setting.PropGroup;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;

/**
 * Kafka worker accept json and then unmarshal it to Map[String, String]. In most cases we can't
 * just use string to store our configuration. Hence, we needs a unified way to serialize non-string
 * type to string value. For example, number, list, and table.
 *
 * <p>The output of this formatter includes 1) TaskSetting -- used by Connector and Task 2) Request
 * of creating connector 3) Request of validating connector
 */
public final class ConnectorFormatter {
  public static ConnectorFormatter of() {
    return new ConnectorFormatter();
  }

  @VisibleForTesting final Map<String, String> settings = new HashMap<>();

  private ConnectorFormatter() {
    // ohara has custom serializeration so the json converter is useless for ohara
    setting(
        ConnectorDefUtils.KEY_CONVERTER_DEFINITION.key(),
        ConnectorDefUtils.KEY_CONVERTER_DEFINITION.defaultString());
    setting(
        ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.key(),
        ConnectorDefUtils.VALUE_CONVERTER_DEFINITION.defaultString());
    setting(
        ConnectorDefUtils.HEADER_CONVERTER_DEFINITION.key(),
        ConnectorDefUtils.HEADER_CONVERTER_DEFINITION.defaultString());
  }

  /**
   * the group is not exposed now since we have a salted connector group. This connector group is
   * defined when user input connector key.
   *
   * @param group connector group on kafka
   * @return this formatter
   */
  private ConnectorFormatter group(String group) {
    return setting(ConnectorDefUtils.CONNECTOR_GROUP_DEFINITION.key(), group);
  }

  /**
   * the name is not exposed now since we have a salted connector name. This connectorName is
   * defined when user input connector key.
   *
   * @param name connector name on kafka
   * @return this formatter
   */
  private ConnectorFormatter name(String name) {
    return setting(ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key(), name);
  }

  public ConnectorFormatter connectorKey(ConnectorKey connectorKey) {
    setting(
        ConnectorDefUtils.CONNECTOR_KEY_DEFINITION.key(), ConnectorKey.toJsonString(connectorKey));
    group(connectorKey.group());
    return name(connectorKey.connectorNameOnKafka());
  }

  public ConnectorFormatter checkRule(SettingDef.CheckRule rule) {
    return setting(ConnectorDefUtils.CHECK_RULE_DEFINITION.key(), rule.name());
  }

  public ConnectorFormatter workerClusterKey(ObjectKey classKey) {
    return setting(
        ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key(), ObjectKey.toJsonString(classKey));
  }

  public ConnectorFormatter setting(String key, String value) {
    CommonUtils.requireNonEmpty(key, () -> "key can't be either empty or null");
    CommonUtils.requireNonEmpty(
        value, () -> "it is illegal to assign empty/null value to key:" + key);
    // Kafka has specific list format so we need to convert the string list ...
    try {
      List<String> ss = StringList.ofJson(value);
      // yep, this value is in json array
      // the empty string list and empty object list have same json representation "[]"
      // so we can't distinguish them here. In order to reduce the ambiguation, we DON'T put the
      // empty list to the
      // setting by default.
      if (!ss.isEmpty()) settings.put(key, StringList.toKafkaString(ss));
    } catch (IllegalArgumentException e) {
      settings.put(key, value);
    }
    return this;
  }

  public ConnectorFormatter settings(Map<String, String> settings) {
    settings.forEach(this::setting);
    return this;
  }

  public ConnectorFormatter className(String className) {
    return setting(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key(), className);
  }

  /**
   * topic names, now, are formatted by group and name.
   *
   * @param topicNames topic names
   * @return this formatter
   */
  private ConnectorFormatter topicNames(Set<String> topicNames) {
    // the array value required by kafka is a,b,c,d,e than json array ...
    return setting(
        ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key(), StringList.toKafkaString(topicNames));
  }

  public ConnectorFormatter topicKey(TopicKey key) {
    return topicKeys(Set.of(key));
  }

  public ConnectorFormatter topicKeys(Set<TopicKey> topicKeys) {
    setting(ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key(), TopicKey.toJsonString(topicKeys));
    return topicNames(
        topicKeys.stream().map(TopicKey::topicNameOnKafka).collect(Collectors.toUnmodifiableSet()));
  }

  public ConnectorFormatter numberOfTasks(int numberOfTasks) {
    return setting(
        ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key(),
        String.valueOf(CommonUtils.requirePositiveInt(numberOfTasks)));
  }

  public ConnectorFormatter partitionClassName(String className) {
    return setting(
        ConnectorDefUtils.PARTITIONER_CLASS_DEFINITION.key(),
        CommonUtils.requireNonEmpty(className));
  }

  public ConnectorFormatter propGroup(String key, PropGroup propGroup) {
    return setting(key, propGroup.toJsonString());
  }

  public ConnectorFormatter column(Column column) {
    return columns(List.of(Objects.requireNonNull(column)));
  }

  public ConnectorFormatter columns(List<Column> columns) {
    return propGroup(
        ConnectorDefUtils.COLUMNS_DEFINITION.key(),
        PropGroup.ofColumns(CommonUtils.requireNonEmpty(columns)));
  }

  public Creation requestOfCreation() {
    return Creation.of(settings);
  }

  public Validation requestOfValidation() {
    return Validation.of(settings);
  }

  /**
   * Return the settings in kafka representation
   *
   * @return an readonly settings
   */
  public Map<String, String> raw() {
    return Collections.unmodifiableMap(settings);
  }
}
