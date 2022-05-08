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
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.setting.WithDefinitions;
import oharastream.ohara.common.util.CommonUtils;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * contains the report of settings validation. The first field is "errorCount", and the other is
 * list of "definition and value".
 */
public final class SettingInfo implements JsonObject {
  public static final Logger LOG = LoggerFactory.getLogger(SettingInfo.class);
  private static final String ERROR_COUNT_KEY = "errorCount";
  private static final String SETTINGS_KEY = "settings";

  public static SettingInfo of(ConfigInfos configInfos) {
    List<Setting> settings =
        configInfos.values().stream()
            .map(
                configInfo -> {
                  try {
                    return Optional.of(Setting.of(configInfo));
                  } catch (IllegalArgumentException e) {
                    // if configInfo is not serialized by ohara, we will get JsonParseException in
                    // parsing json.
                    if (e.getCause() instanceof JsonParseException) {
                      if (configInfo.configKey().name().startsWith("oharastream/ohara"))
                        LOG.error(
                            "official connector:"
                                + configInfo.configKey().name()
                                + " has illegal display name:"
                                + configInfo.configKey().displayName()
                                + ". This may be a compatible issue!!!",
                            e);
                      else
                        LOG.trace(
                            "The connector:"
                                + configInfo.configKey().name()
                                + " has illegal display name:"
                                + configInfo.configKey().displayName()
                                + ". Please inherit Ohara connector!!!",
                            e);
                      return Optional.empty();
                    }
                    throw e;
                  }
                })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(s -> (Setting) s)
            .collect(Collectors.toUnmodifiableList());
    if (settings.isEmpty())
      throw new IllegalArgumentException(
          "failed to parse ohara data stored in kafka. Are you using stale worker image?");
    return of(settings);
  }

  public static SettingInfo ofJson(String json) {
    return JsonUtils.toObject(json, new TypeReference<SettingInfo>() {});
  }

  public static SettingInfo of(List<Setting> settings) {
    return new SettingInfo(
        (int) settings.stream().map(Setting::value).filter(v -> !v.errors().isEmpty()).count(),
        settings);
  }

  private final int errorCount;
  private final List<Setting> settings;

  @JsonCreator
  private SettingInfo(
      @JsonProperty(ERROR_COUNT_KEY) int errorCount,
      @JsonProperty(SETTINGS_KEY) List<Setting> settings) {
    this.errorCount = errorCount;
    this.settings = new ArrayList<>(CommonUtils.requireNonEmpty(settings));
  }

  // ------------------------[helper method]------------------------//
  public Optional<String> value(String key) {
    List<String> values =
        settings.stream()
            .map(Setting::value)
            .filter(v -> v.value() != null && v.key().equals(key))
            .map(SettingValue::value)
            .collect(Collectors.toUnmodifiableList());
    if (values.isEmpty()) return Optional.empty();
    else return Optional.of(values.get(0));
  }

  // ------------------------[common]------------------------//
  public Optional<String> className() {
    return value(ConnectorDefUtils.CONNECTOR_CLASS_DEFINITION.key());
  }

  /**
   * this value is generated from topicKeys. The form is for kafka which does not support to group
   * topic ... Hence, we generate the topic names via topicKeys
   *
   * @return the topic names in kafka form
   */
  public List<String> topicNamesOnKafka() {
    return value(ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key())
        .map(StringList::ofKafkaList)
        .orElse(List.of());
  }

  /**
   * this is what user input for connector.
   *
   * @return topic keys
   */
  public List<TopicKey> topicKeys() {
    return value(ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key())
        .map(TopicKey::toTopicKeys)
        .orElse(List.of());
  }

  public Optional<Integer> numberOfTasks() {
    return value(ConnectorDefUtils.NUMBER_OF_TASKS_DEFINITION.key()).map(Integer::valueOf);
  }

  public Optional<String> author() {
    return value(WithDefinitions.AUTHOR_KEY);
  }

  public Optional<String> version() {
    return value(WithDefinitions.VERSION_KEY);
  }

  public Optional<String> revision() {
    return value(WithDefinitions.VERSION_KEY);
  }

  public Optional<ObjectKey> workerClusterKey() {
    return value(ConnectorDefUtils.WORKER_CLUSTER_KEY_DEFINITION.key()).map(ObjectKey::toObjectKey);
  }

  public Optional<String> connectorType() {
    return value(WithDefinitions.KIND_KEY);
  }

  // ------------------------[json]------------------------//
  @JsonProperty(ERROR_COUNT_KEY)
  public int errorCount() {
    return errorCount;
  }

  @JsonProperty(SETTINGS_KEY)
  public List<Setting> settings() {
    return Collections.unmodifiableList(settings);
  }

  // ------------------------[object]------------------------//
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SettingInfo)
      return toJsonString().equals(((SettingInfo) obj).toJsonString());
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
