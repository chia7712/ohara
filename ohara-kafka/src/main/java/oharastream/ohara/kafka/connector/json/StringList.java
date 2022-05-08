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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.util.CommonUtils;

/**
 * Kafka connector APIs requires the specific "string list" in request. This format is illegal in
 * json so we have got to handle it by manually.
 */
public final class StringList {

  public static List<String> ofJson(String json) {
    return CommonUtils.isEmpty(json) || json.equalsIgnoreCase("null")
        ? List.of()
        : JsonUtils.toObject(json, new TypeReference<List<String>>() {});
  }

  public static String toJsonString(List<String> ss) {
    return JsonUtils.toString(ss);
  }

  /**
   * Kafka prefers to use dot format.
   *
   * @param value kafka's string list
   * @return string list
   */
  public static List<String> ofKafkaList(String value) {
    return CommonUtils.isEmpty(value) ? List.of() : Arrays.asList(value.split(","));
  }

  /**
   * Kafka prefers to use dot format.
   *
   * @param stringList string list
   * @return string list
   */
  public static String toKafkaString(Collection<String> stringList) {
    return String.join(",", stringList);
  }

  private StringList() {}
}
