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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.util.CommonUtils;

/** Request of Kafka Connector APIs matched to request of POST /connectors */
public class Creation implements JsonObject {
  private static final String NAME_KEY = "name";
  private static final String CONFIGS_KEY = "config";
  private final String name;
  private final Map<String, String> configs;

  public static Creation ofJson(String json) {
    return JsonUtils.toObject(json, new TypeReference<Creation>() {});
  }

  public static Creation of(String id, String key, String value) {
    return new Creation(
        id, Map.of(CommonUtils.requireNonEmpty(key), CommonUtils.requireNonEmpty(value)));
  }

  public static Creation of(Map<String, String> configs) {
    return new Creation(CommonUtils.requireNonEmpty(configs.get(NAME_KEY)), configs);
  }

  @JsonCreator
  private Creation(
      @JsonProperty(NAME_KEY) String name, @JsonProperty(CONFIGS_KEY) Map<String, String> configs) {
    this.name = CommonUtils.requireNonEmpty(name);
    CommonUtils.requireNonEmpty(configs)
        .forEach(
            // key can't be empty or null
            (k, v) -> CommonUtils.requireNonEmpty(k));
    this.configs =
        configs.entrySet().stream()
            // the empty or null value should be removed directly...We all hate the unknown
            // value
            // linger in our project.
            .filter(entry -> !CommonUtils.isEmpty(entry.getValue()))
            // NOTED: If settings.name exists, kafka will use it to replace the outside name.
            // for example: {"name":"abc", "settings":{"name":"c"}} is converted to map("name",
            // "c")...
            // Hence, we have to filter out the name here...
            // TODO: this issue is fixed by
            // https://github.com/apache/kafka/commit/5a2960f811c27f59d78dfdb99c7c3c6eeed16c4b
            // TODO: we should remove this workaround after we update kafka to 1.1.x
            .filter(entry -> !entry.getKey().equalsIgnoreCase(NAME_KEY))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @JsonProperty(NAME_KEY)
  public String name() {
    return name;
  }

  @JsonProperty(CONFIGS_KEY)
  public Map<String, String> configs() {
    return Collections.unmodifiableMap(configs);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Creation) return toJsonString().equals(((Creation) obj).toJsonString());
    return false;
  }

  @Override
  public int hashCode() {
    return toJsonString().hashCode();
  }

  @Override
  public String toString() {
    return toJsonString();
  }
}
