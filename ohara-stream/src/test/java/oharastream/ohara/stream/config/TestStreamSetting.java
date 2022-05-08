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

package oharastream.ohara.stream.config;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStreamSetting extends OharaTest {
  @Test
  public void testToEnvString() {
    String string = CommonUtils.randomString();
    Assertions.assertEquals(string, StreamSetting.fromEnvString(StreamSetting.toEnvString(string)));
  }

  @Test
  public void failToUseEnvString() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamSetting.toEnvString(
                CommonUtils.randomString() + StreamSetting.INTERNAL_STRING_FOR_ENV));
  }
}
