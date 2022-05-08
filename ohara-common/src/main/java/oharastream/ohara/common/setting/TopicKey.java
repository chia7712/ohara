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

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * the key of topic object. It is almost same with {@link ObjectKey} excepting for the method
 * "topicNameOnKafka". the method is a helper method used to generate the topic name on kafka.
 */
public interface TopicKey extends ObjectKey {

  /**
   * @param group group
   * @param name name
   * @return a serializable instance
   */
  static TopicKey of(String group, String name) {
    return new KeyImpl(group, name);
  }

  /**
   * parse plain string to topic key
   *
   * @param string plain string
   * @return topic key
   */
  static Optional<TopicKey> ofPlain(String string) {
    return ObjectKey.ofPlain(string).map(key -> TopicKey.of(key.group(), key.name()));
  }

  /**
   * parse plain string to topic key. Otherwise, it throws IllegalArgumentException.
   *
   * @param string plain string
   * @return topic key
   */
  static TopicKey requirePlain(String string) {
    ObjectKey key = ObjectKey.requirePlain(string);
    return TopicKey.of(key.group(), key.name());
  }

  static String toJsonString(Collection<? extends TopicKey> key) {
    return ObjectKey.toJsonString(key);
  }

  /**
   * parse input json and then generate a TopicKey instances.
   *
   * @see ObjectKey#toObjectKeys(String)
   * @param json json representation
   * @return a serializable instance
   */
  static List<TopicKey> toTopicKeys(String json) {
    return ObjectKey.toObjectKeys(json).stream()
        .map(key -> TopicKey.of(key.group(), key.name()))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * generate the topic name for kafka. Noted: kafka topic does not support group so we generate the
   * name composed of group and name
   *
   * @return topic name for kafka
   */
  default String topicNameOnKafka() {
    return toPlain();
  }
}
