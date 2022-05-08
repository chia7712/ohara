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

import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ConnectorKey;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestConnectorFormatter extends OharaTest {

  @Test
  public void nullColumn() {
    Assertions.assertThrows(NullPointerException.class, () -> ConnectorFormatter.of().column(null));
  }

  @Test
  public void nullColumns() {
    Assertions.assertThrows(
        NullPointerException.class, () -> ConnectorFormatter.of().columns(null));
  }

  @Test
  public void emptyColumns() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> ConnectorFormatter.of().columns(List.of()));
  }

  @Test
  public void stringListShouldInKafkaFormat() {
    Set<TopicKey> topicKeys =
        Sets.newHashSet(TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString()));
    Set<String> topicNames =
        topicKeys.stream().map(TopicKey::topicNameOnKafka).collect(Collectors.toUnmodifiableSet());
    Creation creation =
        ConnectorFormatter.of()
            .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
            .topicKeys(topicKeys)
            .requestOfCreation();
    Assertions.assertNotNull(
        creation.configs().get(ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key()));
    Assertions.assertEquals(
        StringList.toKafkaString(topicNames),
        creation.configs().get(ConnectorDefUtils.TOPIC_NAMES_DEFINITION.key()));
    Assertions.assertNotNull(creation.configs().get(ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key()));
    Assertions.assertEquals(
        JsonUtils.toString(topicKeys),
        creation.configs().get(ConnectorDefUtils.TOPIC_KEYS_DEFINITION.key()));
  }

  @Test
  public void configsNameShouldBeRemoved() {
    Creation creation =
        ConnectorFormatter.of()
            .connectorKey(ConnectorKey.of(CommonUtils.randomString(5), CommonUtils.randomString(5)))
            .topicKey(TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString()))
            .requestOfCreation();
    Assertions.assertNull(
        creation.configs().get(ConnectorDefUtils.CONNECTOR_NAME_DEFINITION.key()));
  }

  @Test
  public void ignoreName() {
    Assertions.assertThrows(
        NullPointerException.class, () -> ConnectorFormatter.of().requestOfCreation());
  }

  @Test
  public void nullConnectorKey() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> ConnectorFormatter.of().connectorKey(null).requestOfCreation());
  }

  @Test
  public void emptyListShouldDisappear() {
    ConnectorFormatter format = ConnectorFormatter.of();
    int initialSize = format.settings.size();
    format.setting("a", "[]");
    Assertions.assertEquals(initialSize, format.settings.size());
  }
}
