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

import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public final class ByteUtils {
  public static final Comparator<byte[]> BYTES_COMPARATOR = ByteUtils::compare;
  public static final int SIZE_OF_BYTE = 1;
  public static final int SIZE_OF_BOOLEAN = 1;
  public static final int SIZE_OF_SHORT = java.lang.Short.SIZE / java.lang.Byte.SIZE;
  public static final int SIZE_OF_INT = java.lang.Integer.SIZE / java.lang.Byte.SIZE;
  public static final int SIZE_OF_LONG = java.lang.Long.SIZE / java.lang.Byte.SIZE;
  public static final int SIZE_OF_FLOAT = java.lang.Float.SIZE / java.lang.Byte.SIZE;
  public static final int SIZE_OF_DOUBLE = java.lang.Double.SIZE / java.lang.Byte.SIZE;

  // -------------[boolean]------------- //
  public static byte[] toBytes(boolean value) {
    if (value) return new byte[] {(byte) -1};
    else return new byte[] {(byte) 0};
  }

  public static boolean toBoolean(byte[] bytes) {
    checkSize(bytes, SIZE_OF_BOOLEAN);
    return bytes[0] != (byte) 0;
  }

  // -------------[short]------------- //
  public static byte[] toBytes(short value) {
    return new byte[] {(byte) (value >>> 8), (byte) (value)};
  }

  public static short toShort(byte[] bytes) {
    return toShort(bytes, 0);
  }

  public static short toShort(byte[] bytes, int offset) {
    checkSize(bytes, offset, SIZE_OF_SHORT);
    short value = 0;
    value <<= 8;
    value |= bytes[offset] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 1] & 0xFF;
    return value;
  }

  // -------------[int]------------- //
  public static byte[] toBytes(int value) {
    return new byte[] {
      (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) (value)
    };
  }

  public static int toInt(byte[] bytes) {
    return toInt(bytes, 0);
  }

  public static int toInt(byte[] bytes, int offset) {
    checkSize(bytes, offset, SIZE_OF_INT);
    int value = 0;
    value <<= 8;
    value |= bytes[offset] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 1] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 2] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 3] & 0xFF;
    return value;
  }

  // -------------[long]------------- //
  public static byte[] toBytes(long value) {
    return new byte[] {
      (byte) (value >>> 56),
      (byte) (value >>> 48),
      (byte) (value >>> 40),
      (byte) (value >>> 32),
      (byte) (value >>> 24),
      (byte) (value >>> 16),
      (byte) (value >>> 8),
      (byte) (value)
    };
  }

  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0);
  }

  public static long toLong(byte[] bytes, int offset) {
    checkSize(bytes, offset, SIZE_OF_LONG);
    long value = 0;
    value <<= 8;
    value |= bytes[offset] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 1] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 2] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 3] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 4] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 5] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 6] & 0xFF;
    value <<= 8;
    value |= bytes[offset + 7] & 0xFF;
    return value;
  }
  // -------------[float]------------- //
  public static byte[] toBytes(float value) {
    return toBytes(Float.floatToIntBits(value));
  }

  public static float toFloat(byte[] bytes) {
    return toFloat(bytes, 0);
  }

  public static float toFloat(byte[] bytes, int offset) {
    checkSize(bytes, offset, SIZE_OF_FLOAT);
    return Float.intBitsToFloat(toInt(bytes, offset));
  }

  // -------------[double]------------- //
  public static byte[] toBytes(double value) {
    return toBytes(Double.doubleToLongBits(value));
  }

  public static double toDouble(byte[] bytes) {
    return toDouble(bytes, 0);
  }

  public static double toDouble(byte[] bytes, int offset) {
    checkSize(bytes, offset, SIZE_OF_DOUBLE);
    return Double.longBitsToDouble(toLong(bytes, offset));
  }
  // -------------[string]------------- //
  public static byte[] toBytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  public static String toString(byte[] bytes) {
    return toString(bytes, 0, bytes.length);
  }

  public static String toString(byte[] bytes, int offset, int length) {
    return new String(bytes, offset, length, StandardCharsets.UTF_8);
  }

  public static boolean equals(byte[] buf1, byte[] buf2) {
    return compare(buf1, buf2) == 0;
  }

  public static int compare(byte[] buf1, byte[] buf2) {
    return compare(buf1, 0, buf1.length, buf2, 0, buf2.length);
  }

  public static int compare(
      byte[] buf1, int offset1, int len1, byte[] buf2, int offset2, int len2) {
    if (buf1 == buf2 && offset1 == offset2 && len1 == len2) return 0;
    else {
      int end1 = offset1 + len1;
      int end2 = offset2 + len2;
      for (int index1 = offset1, index2 = offset2;
          index1 < end1 && index2 < end2;
          ++index1, ++index2) {
        int a = buf1[index1] & 0xff;
        int b = buf2[index2] & 0xff;
        if (a != b) return a - b;
      }
      return len1 - len2;
    }
  }

  private static void checkSize(byte[] bytes, int expectedSize) {
    checkSize(bytes, 0, expectedSize);
  }

  private static void checkSize(byte[] bytes, int offset, int expectedSize) {
    if (bytes == null) throw new IllegalArgumentException("bytes can't be null");
    if (bytes.length < offset + expectedSize)
      throw new IllegalArgumentException(
          "bytes's length is "
              + bytes.length
              + " offset:"
              + offset
              + " expected size:"
              + expectedSize);
  }

  private ByteUtils() {}
}
