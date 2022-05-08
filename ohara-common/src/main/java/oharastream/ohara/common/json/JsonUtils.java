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

package oharastream.ohara.common.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.util.Objects;
import oharastream.ohara.common.util.CommonUtils;

public final class JsonUtils {

  /** @return the object mapper configured for jdk 8 */
  public static ObjectMapper objectMapper() {
    return new ObjectMapper()
        // the Optional.empty is removed from json string
        .registerModule(new Jdk8Module().configureAbsentsAsNulls(true))
        // the null is removed from json string
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public static <T> T toObject(String string, TypeReference<T> ref) {
    ObjectMapper mapper = JsonUtils.objectMapper();
    try {
      return mapper.readValue(CommonUtils.requireNonEmpty(string), ref);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static String toString(Object obj) {
    try {
      return objectMapper().writeValueAsString(Objects.requireNonNull(obj));
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private JsonUtils() {}
}
