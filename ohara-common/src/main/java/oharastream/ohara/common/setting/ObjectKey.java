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

package oharastream.ohara.common.setting;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import oharastream.ohara.common.json.JsonUtils;

/**
 * This key represents a unique object stored in Ohara Configurator. This class is moved from
 * ohara-client to ohara-kafka on account of ConnectorFormatter. Otherwise, the suitable place is
 * ohara-client ...
 */
public interface ObjectKey extends Comparable<ObjectKey> {

  @Override
  default int compareTo(ObjectKey another) {
    int rval = group().compareTo(another.group());
    if (rval != 0) return rval;
    return name().compareTo(another.name());
  }

  /**
   * @param group group
   * @param name name
   * @return a serializable instance
   */
  static ObjectKey of(String group, String name) {
    return new KeyImpl(group, name);
  }

  /**
   * Try to parse the plain string. If the plain string is not correct, the None is returned.
   *
   * @param string normal string
   * @return a serializable instance
   */
  static Optional<ObjectKey> ofPlain(String string) {
    String[] splits = string.split("-");
    if (splits.length < 2) return Optional.empty();
    // reject "aaa-"
    else if (splits[0].length() == string.length() - 1) return Optional.empty();
    else return Optional.of(new KeyImpl(splits[0], string.substring(splits[0].length() + 1)));
  }

  static ObjectKey requirePlain(String string) {
    return ofPlain(string)
        .orElseGet(
            () -> {
              throw new IllegalArgumentException(string + " is an incorrect plain key");
            });
  }

  /**
   * @return a plain string consisting of group and name. Noted: this is NOT equal to json string
   */
  default String toPlain() {
    return group() + "-" + name();
  }

  static String toJsonString(ObjectKey key) {
    return new KeyImpl(key.group(), key.name()).toJsonString();
  }

  static String toJsonString(Collection<? extends ObjectKey> keys) {
    return JsonUtils.toString(
        keys.stream()
            .map(
                key -> {
                  if (key instanceof KeyImpl) return (KeyImpl) key;
                  else return new KeyImpl(key.group(), key.name());
                })
            .collect(Collectors.toUnmodifiableList()));
  }
  /**
   * parse input json and then generate a ObjectKey instance.
   *
   * <p>Noted: the parser of scala version is powerful to accept multiples format of requests. By
   * contrast, this json parser is too poor to accept following formats: { "name": "n" }
   *
   * <p>and
   *
   * <p>"n"
   *
   * <p>however, it is ok to java version as this parser is used internally. The data transferred
   * internally is normalized to a standard format: { "group": "n", "name": "n" } and hence we don't
   * worry about taking other "supported" formats for java code.
   *
   * @param json json representation
   * @return a serializable instance
   */
  static ObjectKey toObjectKey(String json) {
    return JsonUtils.toObject(json, new TypeReference<KeyImpl>() {});
  }

  /**
   * parse input json and then generate a ObjectKey instance.
   *
   * <p>Noted: the parser of scala version is powerful to accept multiples format of requests. By
   * contrast, this json parser is too poor to accept following formats: { "name": "n" }
   *
   * <p>and
   *
   * <p>"n"
   *
   * <p>however, it is ok to java version as this parser is used internally. The data transferred
   * internally is normalized to a standard format: { "group": "n", "name": "n" } and hence we don't
   * worry about taking other "supported" formats for java code.
   *
   * @param json json representation
   * @return a serializable instance
   */
  static List<ObjectKey> toObjectKeys(String json) {
    return JsonUtils.toObject(json, new TypeReference<List<KeyImpl>>() {}).stream()
        .map(key -> (ObjectKey) key)
        .collect(Collectors.toUnmodifiableList());
  }

  /** @return the group of object */
  String group();

  /** @return the name of object */
  String name();
}
