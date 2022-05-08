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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.util.CommonUtils;

/**
 * this is for marshalling object to json representation. { "group": "g", "name": "n" }
 *
 * <p>Noted: this impl extends the {@link TopicKey} than {@link ObjectKey} since both interfaces
 * have identical impl, and we all hate duplicate code. Noted: this class should be internal than
 * public to other packages.
 */
class KeyImpl implements JsonObject, TopicKey, ConnectorKey, Serializable {
  private static final long serialVersionUID = 1L;
  private static final String GROUP_KEY = "group";
  private static final String NAME_KEY = "name";
  private final String group;
  private final String name;

  @JsonCreator
  KeyImpl(@JsonProperty(GROUP_KEY) String group, @JsonProperty(NAME_KEY) String name) {
    this.group = CommonUtils.requireNonEmpty(group);
    this.name = CommonUtils.requireNonEmpty(name);
  }

  @Override
  @JsonProperty(GROUP_KEY)
  public String group() {
    return group;
  }

  @Override
  @JsonProperty(NAME_KEY)
  public String name() {
    return name;
  }

  // ------------------------[object]------------------------//
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ObjectKey)
      return ((ObjectKey) obj).group().equals(group) && ((ObjectKey) obj).name().equals(name);
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
