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

package oharastream.ohara.stream;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ClassType;
import oharastream.ohara.common.setting.TopicKey;
import oharastream.ohara.common.setting.WithDefinitions;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.stream.config.StreamDefUtils;
import oharastream.ohara.stream.config.StreamSetting;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStream extends OharaTest {

  @Test
  public void testCanFindCustomClassEntryFromInnerClass() {
    CustomStream app = new CustomStream();
    TopicKey fromKey = TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString());
    TopicKey toKey = TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString());

    // initial all required environment
    Stream.execute(
        app.getClass(),
        Map.of(
            StreamDefUtils.GROUP_DEFINITION.key(), CommonUtils.randomString(5),
            StreamDefUtils.NAME_DEFINITION.key(), "TestStream",
            StreamDefUtils.BROKER_DEFINITION.key(), CommonUtils.randomString(),
            StreamDefUtils.FROM_TOPIC_KEYS_DEFINITION.key(),
                TopicKey.toJsonString(List.of(fromKey)),
            StreamDefUtils.TO_TOPIC_KEYS_DEFINITION.key(), TopicKey.toJsonString(List.of(toKey))));
  }

  @Test
  public void testKind() {
    CustomStream app = new CustomStream();
    Assertions.assertEquals(
        ClassType.STREAM.key(),
        app.settingDefinitions().get(WithDefinitions.KIND_KEY).defaultString());
  }

  public static class CustomStream extends Stream {
    final AtomicInteger counter = new AtomicInteger();

    @Override
    public void init() {
      int res = counter.incrementAndGet();
      // Stream should call init() first
      Assertions.assertEquals(1, res);
    }

    @Override
    public void start(OStream<Row> ostream, StreamSetting streamSetting) {
      int res = counter.incrementAndGet();
      // Stream should call start() after init()
      Assertions.assertEquals(2, res);
    }
  }
}
