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

package oharastream.ohara.common.util;

import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestVersionUtils extends OharaTest {

  @Test
  public void allMembersShouldExist() {
    Assertions.assertNotNull(VersionUtils.DATE);
    Assertions.assertFalse(VersionUtils.DATE.isEmpty());
    Assertions.assertNotNull(VersionUtils.REVISION);
    Assertions.assertFalse(VersionUtils.REVISION.isEmpty());
    Assertions.assertNotNull(VersionUtils.USER);
    Assertions.assertFalse(VersionUtils.USER.isEmpty());
    Assertions.assertNotNull(VersionUtils.VERSION);
    Assertions.assertFalse(VersionUtils.VERSION.isEmpty());
    Assertions.assertNotNull(VersionUtils.BRANCH);
    Assertions.assertFalse(VersionUtils.BRANCH.isEmpty());
  }
}
