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
import java.util.Objects;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.setting.SettingDef;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfo;

/**
 * Response of Kafka Connector APIs matched to response of PUT /connector-plugins/(string:
 * name)/setting/validate
 *
 * <p>this class is related to org.apache.kafka.connect.runtime.rest.entities.ConfigInfo
 */
public final class Setting implements JsonObject {
  public static final String DEFINITION_KEY = "definition";
  public static final String VALUE_KEY = "value";

  public static Setting of(ConfigInfo configInfo) {
    return of(
        ConnectorDefUtils.of(configInfo.configKey()), SettingValue.of(configInfo.configValue()));
  }

  public static Setting ofJson(String json) {
    return JsonUtils.toObject(json, new TypeReference<Setting>() {});
  }

  public static Setting of(SettingDef definition, SettingValue value) {
    return new Setting(definition, value);
  }

  private final SettingDef definition;
  private final SettingValue value;

  @JsonCreator
  private Setting(
      @JsonProperty(DEFINITION_KEY) SettingDef definition,
      @JsonProperty(VALUE_KEY) SettingValue value) {
    this.definition = Objects.requireNonNull(definition);
    this.value = Objects.requireNonNull(value);
  }

  @JsonProperty(DEFINITION_KEY)
  public SettingDef definition() {
    return definition;
  }

  @JsonProperty(VALUE_KEY)
  public SettingValue value() {
    return value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Setting) return toJsonString().equals(((Setting) obj).toJsonString());
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
