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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import oharastream.ohara.common.annotations.Nullable;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.util.CommonUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo;

/**
 * This class stores the value passed by user. this class is related to
 * org.apache.kafka.connect.runtime.rest.entities.ConfigValueInfo
 */
public class SettingValue implements JsonObject {
  public static SettingValue of(ConfigValueInfo configValueInfo) {
    return new SettingValue(
        configValueInfo.name(), configValueInfo.value(), configValueInfo.errors());
  }

  public static SettingValue ofJson(String json) {
    return JsonUtils.toObject(json, new TypeReference<SettingValue>() {});
  }

  public static SettingValue of(String name, @Nullable String value, List<String> errors) {
    return new SettingValue(name, value, errors);
  }

  private static final String KEY_KEY = "key";
  private static final String VALUE_KEY = "value";
  private static final String ERRORS_KEY = "errors";

  private final String key;
  private final @Nullable String value;
  private final List<String> errors;

  @JsonCreator
  private SettingValue(
      @JsonProperty(KEY_KEY) String key,
      @JsonProperty(VALUE_KEY) String value,
      @JsonProperty(ERRORS_KEY) List<String> errors) {
    this.key = CommonUtils.requireNonEmpty(key);
    this.value = value;
    this.errors = new ArrayList<>(Objects.requireNonNull(errors));
  }

  @JsonProperty(KEY_KEY)
  public String key() {
    return key;
  }

  @JsonProperty(VALUE_KEY)
  @Nullable
  public String value() {
    return value;
  }

  @JsonProperty(ERRORS_KEY)
  public List<String> errors() {
    return Collections.unmodifiableList(errors);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SettingValue)
      return toJsonString().equals(((SettingValue) obj).toJsonString());
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
