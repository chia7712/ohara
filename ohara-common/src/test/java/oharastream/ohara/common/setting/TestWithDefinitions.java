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

import java.util.Map;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.common.util.VersionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestWithDefinitions extends OharaTest {

  @Test
  public void authorShouldNotBeReplaced() {
    String author = CommonUtils.randomString();
    Assertions.assertEquals(
        author,
        WithDefinitions.merge(
                this,
                Map.of(WithDefinitions.AUTHOR_KEY, WithDefinitions.authorDefinition(author)),
                Map.of())
            .get(WithDefinitions.AUTHOR_KEY)
            .defaultString());
  }

  @Test
  public void authorShouldHaveDefaultValue() {
    Assertions.assertEquals(
        VersionUtils.USER,
        WithDefinitions.merge(this, Map.of(), Map.of())
            .get(WithDefinitions.AUTHOR_KEY)
            .defaultString());
  }

  @Test
  public void versionShouldNotBeReplaced() {
    String version = CommonUtils.randomString();
    Assertions.assertEquals(
        version,
        WithDefinitions.merge(
                this,
                Map.of(WithDefinitions.VERSION_KEY, WithDefinitions.versionDefinition(version)),
                Map.of())
            .get(WithDefinitions.VERSION_KEY)
            .defaultString());
  }

  @Test
  public void versionShouldHaveDefaultValue() {
    Assertions.assertEquals(
        VersionUtils.VERSION,
        WithDefinitions.merge(this, Map.of(), Map.of())
            .get(WithDefinitions.VERSION_KEY)
            .defaultString());
  }

  @Test
  public void revisionShouldNotBeReplaced() {
    String revision = CommonUtils.randomString();
    Assertions.assertEquals(
        revision,
        WithDefinitions.merge(
                this,
                Map.of(WithDefinitions.REVISION_KEY, WithDefinitions.revisionDefinition(revision)),
                Map.of())
            .get(WithDefinitions.REVISION_KEY)
            .defaultString());
  }

  @Test
  public void revisionShouldHaveDefaultValue() {
    Assertions.assertEquals(
        VersionUtils.REVISION,
        WithDefinitions.merge(this, Map.of(), Map.of())
            .get(WithDefinitions.REVISION_KEY)
            .defaultString());
  }

  @Test
  public void kindShouldBeUnknown() {
    Assertions.assertEquals(
        ClassType.UNKNOWN.key(),
        WithDefinitions.merge(this, Map.of(), Map.of())
            .get(WithDefinitions.KIND_KEY)
            .defaultString());
  }

  @Test
  public void testDefaultGroup() {
    Assertions.assertNotEquals(WithDefinitions.META_GROUP, SettingDef.COMMON_GROUP);
  }
}
