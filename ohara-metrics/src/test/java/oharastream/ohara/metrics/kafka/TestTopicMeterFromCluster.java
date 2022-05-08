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

package oharastream.ohara.metrics.kafka;

import java.util.stream.Collectors;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.metrics.BeanChannel;
import oharastream.ohara.testing.WithBrokerWorker;
import org.junit.jupiter.api.Test;

/** Worker instance will create three topics on broker so we can use it to test our APIs. */
public class TestTopicMeterFromCluster extends WithBrokerWorker {

  @Test
  public void list() {
    // worker cluster create 3 topics
    CommonUtils.await(
        () ->
            BeanChannel.local().topicMeters().stream()
                    .map(TopicMeter::topicName)
                    .collect(Collectors.toUnmodifiableSet())
                    .size()
                >= 3,
        java.time.Duration.ofSeconds(30));
  }
}
