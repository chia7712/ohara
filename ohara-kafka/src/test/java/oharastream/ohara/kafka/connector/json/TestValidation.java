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

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestValidation extends OharaTest {

  @Test
  public void ignoreClassName() {
    Assertions.assertThrows(
        NoSuchElementException.class,
        () ->
            Validation.of(Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
                .className());
  }

  @Test
  public void ignoreTopicNames() {
    Assertions.assertThrows(
        NoSuchElementException.class,
        () ->
            Validation.of(Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
                .topicNames());
  }

  @Test
  public void testRequiredSettings() {
    String className = CommonUtils.randomString();
    Set<String> topicNames = Set.of(CommonUtils.randomString());
    Validation validation = Validation.of(className, topicNames);
    Assertions.assertEquals(className, validation.className());
    Assertions.assertEquals(topicNames, validation.topicNames());
  }

  @Test
  public void testEqual() {
    Validation validation =
        Validation.of(Map.of(CommonUtils.randomString(), CommonUtils.randomString()));
    Assertions.assertEquals(validation, validation);
  }

  @Test
  public void testToString() {
    Validation validation =
        Validation.of(Map.of(CommonUtils.randomString(), CommonUtils.randomString()));
    Assertions.assertEquals(validation.toString(), validation.toString());
  }

  @Test
  public void testToJsonString() {
    Validation validation =
        Validation.of(Map.of(CommonUtils.randomString(), CommonUtils.randomString()));
    Assertions.assertEquals(validation.toJsonString(), validation.toJsonString());
  }

  @Test
  public void testHashCode() {
    Validation validation =
        Validation.of(Map.of(CommonUtils.randomString(), CommonUtils.randomString()));
    Assertions.assertEquals(validation.hashCode(), validation.hashCode());
  }

  @Test
  public void testGetter() {
    String key = CommonUtils.randomString(5);
    String value = CommonUtils.randomString(5);
    Validation validation = Validation.of(Map.of(key, value));
    Assertions.assertEquals(Map.of(key, value), validation.settings());
    Assertions.assertEquals(value, validation.settings().get(key));
  }

  @Test
  public void nullSettings() {
    Assertions.assertThrows(NullPointerException.class, () -> Validation.of(null));
  }

  @Test
  public void emptySettings() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> Validation.of(Map.of()));
  }

  @Test
  public void nullValue() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> Validation.of(Collections.singletonMap(CommonUtils.randomString(), null)));
  }

  @Test
  public void emptyValue() {
    Validation.of(Map.of(CommonUtils.randomString(), ""));
  }

  @Test
  public void failToModify() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            Validation.of(Map.of(CommonUtils.randomString(), CommonUtils.randomString()))
                .settings()
                .remove("a"));
  }
}
