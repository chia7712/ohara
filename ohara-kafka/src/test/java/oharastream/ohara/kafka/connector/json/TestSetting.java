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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.setting.SettingDef;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSetting extends OharaTest {
  @Test
  public void testEqual() throws IOException {
    Setting config =
        Setting.of(
            SettingDef.builder().key(CommonUtils.randomString()).build(),
            SettingValue.of(CommonUtils.randomString(), CommonUtils.randomString(), List.of()));
    ObjectMapper mapper = JsonUtils.objectMapper();
    Assertions.assertEquals(
        config,
        mapper.readValue(mapper.writeValueAsString(config), new TypeReference<Setting>() {}));
  }

  @Test
  public void testGetter() {
    SettingDef def = SettingDef.builder().key(CommonUtils.randomString()).build();
    SettingValue value =
        SettingValue.of(CommonUtils.randomString(), CommonUtils.randomString(), List.of());
    Setting config = Setting.of(def, value);
    Assertions.assertEquals(def, config.definition());
    Assertions.assertEquals(value, config.value());
  }

  @Test
  public void nullDefinition() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            Setting.of(
                null,
                SettingValue.of(
                    CommonUtils.randomString(), CommonUtils.randomString(), List.of())));
  }

  @Test
  public void nullValue() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> Setting.of(SettingDef.builder().key(CommonUtils.randomString()).build(), null));
  }
}
