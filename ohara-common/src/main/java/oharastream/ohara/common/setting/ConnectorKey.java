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

import java.util.Optional;

/**
 * the key of connector object. It is almost same with {@link ObjectKey} excepting for the method
 * "connectorNameOnKafka". the method is a helper method used to generate the connector name on
 * kafka.
 */
public interface ConnectorKey extends ObjectKey {

  /**
   * @param group group
   * @param name name
   * @return a serializable instance
   */
  static ConnectorKey of(String group, String name) {
    return new KeyImpl(group, name);
  }

  /**
   * parse plain string to connector key
   *
   * @param string plain string
   * @return connector key
   */
  static Optional<ConnectorKey> ofPlain(String string) {
    return ObjectKey.ofPlain(string).map(key -> ConnectorKey.of(key.group(), key.name()));
  }

  /**
   * parse plain string to connector key. Otherwise, it throws IllegalArgumentException.
   *
   * @param string plain string
   * @return connector key
   */
  static ConnectorKey requirePlain(String string) {
    ObjectKey key = ObjectKey.requirePlain(string);
    return ConnectorKey.of(key.group(), key.name());
  }

  static String toJsonString(ConnectorKey key) {
    return ObjectKey.toJsonString(key);
  }

  /**
   * parse input json and then generate a ConnectorKey instance.
   *
   * @see ObjectKey#toObjectKey(String)
   * @param json json representation
   * @return a serializable instance
   */
  static ConnectorKey toConnectorKey(String json) {
    ObjectKey key = ObjectKey.toObjectKey(json);
    return ConnectorKey.of(key.group(), key.name());
  }

  /**
   * generate the connector name for kafka. Noted: kafka connector does not support group so we
   * generate the name composed of group and name.
   *
   * @return connector name for kafka
   */
  default String connectorNameOnKafka() {
    return toPlain();
  }
}
