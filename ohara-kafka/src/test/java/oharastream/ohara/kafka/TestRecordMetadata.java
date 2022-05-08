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

package oharastream.ohara.kafka;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.TopicKey;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRecordMetadata extends OharaTest {

  @Test
  public void testConversion() {
    var topicKey = TopicKey.of("g", "n");
    var partition = 100;
    var timestamp = 11;
    var offset = 5;
    var serializedKeySize = 111;
    var serializedValueSize = 88;
    var record =
        RecordMetadata.of(
            new org.apache.kafka.clients.producer.RecordMetadata(
                new TopicPartition(topicKey.toPlain(), partition),
                0,
                offset,
                timestamp,
                10L,
                serializedKeySize,
                serializedValueSize));
    Assertions.assertEquals(topicKey, record.topicKey());
    Assertions.assertEquals(partition, record.partition());
    Assertions.assertEquals(timestamp, record.timestamp());
    Assertions.assertEquals(offset, record.offset());
    Assertions.assertEquals(serializedKeySize, record.serializedKeySize());
    Assertions.assertEquals(serializedValueSize, record.serializedValueSize());
  }
}
