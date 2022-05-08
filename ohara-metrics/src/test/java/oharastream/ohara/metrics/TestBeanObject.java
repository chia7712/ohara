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

package oharastream.ohara.metrics;

import java.util.Map;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBeanObject extends OharaTest {

  @Test
  public void testNullDomain() {
    Assertions.assertThrows(
        NullPointerException.class, () -> BeanObject.builder().domainName(null));
  }

  @Test
  public void testEmptyDomain() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BeanObject.builder().domainName(""));
  }

  @Test
  public void testNullProperties() {
    Assertions.assertThrows(
        NullPointerException.class, () -> BeanObject.builder().properties(null));
  }

  @Test
  public void testEmptyProperties() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BeanObject.builder().properties(Map.of()));
  }

  @Test
  public void testNullValueInProperties() {
    Assertions.assertThrows(
        NullPointerException.class, () -> BeanObject.builder().properties(Map.of("a", null)));
  }

  @Test
  public void testEmptyValueInProperties() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BeanObject.builder().properties(Map.of("a", "")));
  }

  @Test
  public void testNullAttributes() {
    Assertions.assertThrows(
        NullPointerException.class, () -> BeanObject.builder().attributes(null));
  }

  @Test
  public void testEmptyAttributes() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> BeanObject.builder().attributes(Map.of()));
  }

  @Test
  public void testNullValueInAttributes() {
    Assertions.assertThrows(
        NullPointerException.class, () -> BeanObject.builder().attributes(Map.of("a", null)));
  }

  @Test
  public void testImmutableProperties() {
    BeanObject obj =
        BeanObject.builder()
            .domainName(CommonUtils.randomString())
            .properties(Map.of("a", "b"))
            .attributes(Map.of("a", "b"))
            .queryTime(CommonUtils.current())
            .build();
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> obj.properties().remove(("a")));
  }

  @Test
  public void testImmutableAttributes() {
    BeanObject obj =
        BeanObject.builder()
            .domainName(CommonUtils.randomString())
            .properties(Map.of("a", "b"))
            .attributes(Map.of("a", "b"))
            .queryTime(CommonUtils.current())
            .build();
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> obj.attributes().remove(("a")));
  }

  @Test
  public void testZeroQueryNumber() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BeanObject.builder()
                .domainName(CommonUtils.randomString())
                .properties(Map.of("a", "b"))
                .attributes(Map.of("a", "b"))
                .queryTime(0)
                .build());
  }

  @Test
  public void testNegativeQueryNumber() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            BeanObject.builder()
                .domainName(CommonUtils.randomString())
                .properties(Map.of("a", "b"))
                .attributes(Map.of("a", "b"))
                .queryTime(-999)
                .build());
  }
}
