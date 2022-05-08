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

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.*;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.util.CommonUtils;

public class Validation implements JsonObject {
  private final Map<String, String> settings;

  public static Validation ofJson(String json) {
    return of(JsonUtils.toObject(json, new TypeReference<Map<String, String>>() {}));
  }

  /**
   * Construct a Validation with basic required arguments only.
   *
   * @param className class name
   * @param topicsNames topic names
   * @return validation
   */
  public static Validation of(String className, Set<String> topicsNames) {
    return new Validation(
        Map.of(
            ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key(),
                CommonUtils.requireNonEmpty(className),
            ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key(), StringList.toKafkaString(topicsNames)));
  }

  public static Validation of(Map<String, String> settings) {
    return new Validation(settings);
  }

  private Validation(Map<String, String> settings) {
    CommonUtils.requireNonEmpty(settings)
        .forEach(
            (k, v) -> {
              CommonUtils.requireNonEmpty(k, () -> "empty key is illegal");
              // the list format supported by kafka is not JSON representation. the form is a string
              // splitten by
              // dot so the empty string MUST be legal since it is used to represent empty list ...
              // by chia
              Objects.requireNonNull(v, () -> "the value of " + k + " can't be empty");
            });
    this.settings = Map.copyOf(settings);
  }

  public Map<String, String> settings() {
    return settings;
  }

  @Override
  public String toJsonString() {
    return JsonUtils.toString(settings);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Validation) return toJsonString().equals(((Validation) obj).toJsonString());
    return false;
  }

  @Override
  public int hashCode() {
    return toJsonString().hashCode();
  }

  // ------------------------------------------[helper
  // methods]------------------------------------------//
  private String value(SettingDef settingDefinition) {
    if (settings.containsKey(settingDefinition.key())) return settings.get(settingDefinition.key());
    else throw new NoSuchElementException(settingDefinition.key() + " doesn't exist");
  }

  /**
   * Kafka validation requires the class name so this class offers this helper method.
   *
   * @return class name
   */
  public String className() {
    return value(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION);
  }

  /**
   * Kafka-2.x validation requires the topics so this class offers this helper method.
   *
   * @return topics name
   */
  public Set<String> topicNames() {
    return new HashSet<>(StringList.ofKafkaList(value(ConnectorDefUtils.TOPIC_NAMES_DEFINITION)));
  }
}
