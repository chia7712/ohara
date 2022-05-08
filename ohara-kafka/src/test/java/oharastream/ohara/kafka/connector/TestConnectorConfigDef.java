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

package oharastream.ohara.kafka.connector;

import java.util.stream.Stream;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.ClassType;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.setting.WithDefinitions;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.kafka.connector.json.ConnectorDefUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestConnectorConfigDef extends OharaTest {

  @Test
  public void testVersion() {
    DumbSink sink = new DumbSink();
    Assertions.assertNotNull(sink.config().configKeys().get(WithDefinitions.VERSION_KEY));
  }

  @Test
  public void testRevision() {
    DumbSink sink = new DumbSink();
    Assertions.assertNotNull(sink.config().configKeys().get(WithDefinitions.VERSION_KEY));
  }

  @Test
  public void testAuthor() {
    DumbSink sink = new DumbSink();
    Assertions.assertNotNull(sink.config().configKeys().get(WithDefinitions.AUTHOR_KEY));
  }

  @Test
  public void testSinkKind() {
    DumbSink sink = new DumbSink();
    Assertions.assertEquals(
        ClassType.SINK.key(),
        sink.config().configKeys().get(WithDefinitions.KIND_KEY).defaultValue);
    Assertions.assertEquals(
        ClassType.SINK.key(),
        sink.settingDefinitions().get(WithDefinitions.KIND_KEY).defaultString());
  }

  @Test
  public void testSourceKind() {
    DumbSource source = new DumbSource();
    Assertions.assertEquals(
        ClassType.SOURCE.key(),
        source.config().configKeys().get(WithDefinitions.KIND_KEY).defaultValue);
    Assertions.assertEquals(
        ClassType.SOURCE.key(),
        source.settingDefinitions().get(WithDefinitions.KIND_KEY).defaultString());
  }

  /** make sure all types from SettingDef are acceptable to kafka type. */
  @Test
  public void testToConfigKey() {
    Stream.of(SettingDef.Type.values())
        .forEach(
            type ->
                ConnectorDefUtils.toConfigKey(
                    SettingDef.builder().key(CommonUtils.randomString()).required(type).build()));
  }
}
