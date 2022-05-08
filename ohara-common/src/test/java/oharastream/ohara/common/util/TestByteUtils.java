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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestByteUtils extends OharaTest {

  @Test
  public void testBoolean() {
    Assertions.assertTrue(ByteUtils.toBoolean(ByteUtils.toBytes(true)));
    Assertions.assertFalse(ByteUtils.toBoolean(ByteUtils.toBytes(false)));
  }

  @Test
  public void testShort() {
    List<Short> data =
        Arrays.asList(Short.MIN_VALUE, (short) -10, (short) 0, (short) 10, Short.MAX_VALUE);
    data.forEach(v -> Assertions.assertEquals((short) v, ByteUtils.toShort(ByteUtils.toBytes(v))));
  }

  @Test
  public void testInt() {
    List<Integer> data = Arrays.asList(Integer.MIN_VALUE, -10, 0, 10, Integer.MAX_VALUE);
    data.forEach(v -> Assertions.assertEquals((int) v, ByteUtils.toInt(ByteUtils.toBytes(v))));
  }

  @Test
  public void testLong() {
    List<Long> data =
        Arrays.asList(Long.MIN_VALUE, (long) -10, (long) 0, (long) 10, Long.MAX_VALUE);
    data.forEach(v -> Assertions.assertEquals((long) v, ByteUtils.toLong(ByteUtils.toBytes(v))));
  }

  @Test
  public void testFloat() {
    List<Float> data =
        Arrays.asList(Float.MIN_VALUE, (float) -10, (float) 0, (float) 10, Float.MAX_VALUE);
    data.forEach(v -> Assertions.assertEquals(v, ByteUtils.toFloat(ByteUtils.toBytes(v)), 0.0));
  }

  @Test
  public void testDouble() {
    List<Double> data =
        Arrays.asList(Double.MIN_VALUE, (double) -10, (double) 0, (double) 10, Double.MAX_VALUE);
    data.forEach(v -> Assertions.assertEquals(v, ByteUtils.toDouble(ByteUtils.toBytes(v)), 0.0));
  }

  @Test
  public void testString() {
    List<String> data =
        Arrays.asList(
            String.valueOf(Double.MIN_VALUE),
            "abc",
            "aaaaa",
            "Ccccc",
            String.valueOf(Double.MAX_VALUE));
    data.forEach(v -> Assertions.assertEquals(v, ByteUtils.toString(ByteUtils.toBytes(v))));
  }

  @Test
  public void testBooleanComparator() {
    List<byte[]> data = Arrays.asList(ByteUtils.toBytes(true), ByteUtils.toBytes(false));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.BYTES_COMPARATOR.compare(v, v)));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.compare(v, v)));
  }

  @Test
  public void testShortComparator() {
    List<byte[]> data =
        Stream.of(Short.MIN_VALUE, (short) -10, (short) 0, (short) 10, Short.MAX_VALUE)
            .map(ByteUtils::toBytes)
            .collect(Collectors.toUnmodifiableList());
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.BYTES_COMPARATOR.compare(v, v)));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.compare(v, v)));

    short lhs = ByteUtils.toShort(ByteUtils.toBytes((short) -10));
    short rhs = ByteUtils.toShort(ByteUtils.toBytes((short) 20));
    Assertions.assertTrue(lhs < rhs);
  }

  @Test
  public void testIntComparator() {
    List<byte[]> data =
        Stream.of(Integer.MIN_VALUE, -10, 0, 10, Integer.MAX_VALUE)
            .map(ByteUtils::toBytes)
            .collect(Collectors.toUnmodifiableList());
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.BYTES_COMPARATOR.compare(v, v)));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.compare(v, v)));

    int lhs = ByteUtils.toInt(ByteUtils.toBytes(-10));
    int rhs = ByteUtils.toInt(ByteUtils.toBytes(20));
    Assertions.assertTrue(lhs < rhs);
  }

  @Test
  public void testLongComparator() {
    List<byte[]> data =
        Stream.of(Long.MIN_VALUE, (long) -10, (long) 0, (long) 10, Long.MAX_VALUE)
            .map(ByteUtils::toBytes)
            .collect(Collectors.toUnmodifiableList());
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.BYTES_COMPARATOR.compare(v, v)));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.compare(v, v)));

    long lhs = ByteUtils.toLong(ByteUtils.toBytes((long) -10));
    long rhs = ByteUtils.toLong(ByteUtils.toBytes((long) 20));
    Assertions.assertTrue(lhs < rhs);
  }

  @Test
  public void testFloatComparator() {
    List<byte[]> data =
        Stream.of(Float.MIN_VALUE, (float) -10, (float) 0, (float) 10, Float.MAX_VALUE)
            .map(ByteUtils::toBytes)
            .collect(Collectors.toUnmodifiableList());
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.BYTES_COMPARATOR.compare(v, v)));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.compare(v, v)));

    float lhs = ByteUtils.toFloat(ByteUtils.toBytes((float) -10));
    float rhs = ByteUtils.toFloat(ByteUtils.toBytes((float) 20));
    Assertions.assertTrue(lhs < rhs);
  }

  @Test
  public void testDoubleComparator() {
    List<byte[]> data =
        Stream.of(Double.MIN_VALUE, (double) -10, (double) 0, (double) 10, Double.MAX_VALUE)
            .map(ByteUtils::toBytes)
            .collect(Collectors.toUnmodifiableList());
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.BYTES_COMPARATOR.compare(v, v)));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.compare(v, v)));

    double lhs = ByteUtils.toDouble(ByteUtils.toBytes((double) -10));
    double rhs = ByteUtils.toDouble(ByteUtils.toBytes((double) 20));
    Assertions.assertTrue(lhs < rhs);
  }

  @Test
  public void testStringComparator() {
    List<byte[]> data =
        Stream.of(
                String.valueOf(Double.MIN_VALUE),
                "abc",
                "aaaaa",
                "Ccccc",
                String.valueOf(Double.MAX_VALUE))
            .map(ByteUtils::toBytes)
            .collect(Collectors.toUnmodifiableList());
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.BYTES_COMPARATOR.compare(v, v)));
    data.forEach(v -> Assertions.assertEquals(0, ByteUtils.compare(v, v)));

    byte[] lhs = ByteUtils.toBytes("abc");
    byte[] rhs = ByteUtils.toBytes("bc");
    Assertions.assertTrue(ByteUtils.compare(lhs, rhs) < 0);
    Assertions.assertTrue(ByteUtils.BYTES_COMPARATOR.compare(lhs, rhs) < 0);
  }
}
