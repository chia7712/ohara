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

package oharastream.ohara.common.data;

import java.io.*;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import oharastream.ohara.common.util.ByteUtils;

/**
 * Used to convert a T object to V NOTED: the impl should not be an inner/anonymous class since
 * Store will use the reflection to create the object. The dynamical call to inner/anonymous class
 * is fraught with risks.
 *
 * @param <T> data type
 */
public interface Serializer<T> {

  /**
   * Convert the object to a serializable type
   *
   * @param obj object
   * @return a serializable type
   */
  byte[] to(T obj);

  /**
   * Convert the serialized data to object
   *
   * @param bytes serialized data
   * @return object
   */
  T from(byte[] bytes);

  Serializer<byte[]> BYTES =
      new Serializer<byte[]>() {
        @Override
        public byte[] to(byte[] obj) {
          return obj;
        }

        @Override
        public byte[] from(byte[] bytes) {
          return bytes;
        }
      };

  Serializer<Boolean> BOOLEAN =
      new Serializer<Boolean>() {
        @Override
        public byte[] to(Boolean obj) {
          return ByteUtils.toBytes(obj);
        }

        @Override
        public Boolean from(byte[] bytes) {
          return ByteUtils.toBoolean(bytes);
        }
      };

  Serializer<Byte> BYTE =
      new Serializer<Byte>() {
        @Override
        public byte[] to(Byte obj) {
          return new byte[] {obj};
        }

        @Override
        public Byte from(byte[] bytes) {
          return bytes[0];
        }
      };

  Serializer<Short> SHORT =
      new Serializer<Short>() {
        @Override
        public byte[] to(Short obj) {
          return ByteUtils.toBytes(obj);
        }

        @Override
        public Short from(byte[] bytes) {
          return ByteUtils.toShort(bytes);
        }
      };

  Serializer<Integer> INT =
      new Serializer<Integer>() {
        @Override
        public byte[] to(Integer obj) {
          return ByteUtils.toBytes(obj);
        }

        @Override
        public Integer from(byte[] bytes) {
          return ByteUtils.toInt(bytes);
        }
      };

  Serializer<Long> LONG =
      new Serializer<Long>() {
        @Override
        public byte[] to(Long obj) {
          return ByteUtils.toBytes(obj);
        }

        @Override
        public Long from(byte[] bytes) {
          return ByteUtils.toLong(bytes);
        }
      };

  Serializer<Float> FLOAT =
      new Serializer<Float>() {
        @Override
        public byte[] to(Float obj) {
          return ByteUtils.toBytes(obj);
        }

        @Override
        public Float from(byte[] bytes) {
          return ByteUtils.toFloat(bytes);
        }
      };

  Serializer<Double> DOUBLE =
      new Serializer<Double>() {
        @Override
        public byte[] to(Double obj) {
          return ByteUtils.toBytes(obj);
        }

        @Override
        public Double from(byte[] bytes) {
          return ByteUtils.toDouble(bytes);
        }
      };

  Serializer<String> STRING =
      new Serializer<String>() {
        @Override
        public byte[] to(String obj) {
          return ByteUtils.toBytes(obj);
        }

        @Override
        public String from(byte[] bytes) {
          return ByteUtils.toString(bytes);
        }
      };

  /**
   * | version (short 1 byte) | name length (short 2 bytes) | name (string in bytes) | type (short 2
   * bytes) | value length (short 2 bytes) | value (bytes) |
   */
  Serializer<Cell<?>> CELL =
      new Serializer<Cell<?>>() {
        @Override
        public byte[] to(Cell<?> cell) {
          byte[] nameBytes = STRING.to(cell.name());
          final byte[] valueBytes;
          DataType type = DataType.from(cell.value());
          switch (type) {
            case BYTES:
              valueBytes = BYTES.to((byte[]) cell.value());
              break;
            case BOOLEAN:
              valueBytes = BOOLEAN.to((Boolean) cell.value());
              break;
            case BYTE:
              valueBytes = BYTE.to((Byte) cell.value());
              break;
            case SHORT:
              valueBytes = SHORT.to((Short) cell.value());
              break;
            case INT:
              valueBytes = INT.to((Integer) cell.value());
              break;
            case LONG:
              valueBytes = LONG.to((Long) cell.value());
              break;
            case FLOAT:
              valueBytes = FLOAT.to((Float) cell.value());
              break;
            case DOUBLE:
              valueBytes = DOUBLE.to((Double) cell.value());
              break;
            case STRING:
              valueBytes = STRING.to((String) cell.value());
              break;
            case CELL:
              valueBytes = CELL.to((Cell) cell.value());
              break;
            case ROW:
              valueBytes = ROW.to((Row) cell.value());
              break;
            case OBJECT:
              valueBytes = OBJECT.to(cell.value());
              break;
            default:
              throw new UnsupportedClassVersionError(type.getClass().getName());
          }
          int initialSize =
              // version
              ByteUtils.SIZE_OF_BYTE
                  // name length
                  + ByteUtils.SIZE_OF_SHORT
                  // name
                  + nameBytes.length
                  // type order
                  + ByteUtils.SIZE_OF_SHORT
                  // value length
                  + ByteUtils.SIZE_OF_SHORT
                  // value
                  + valueBytes.length;
          try (ByteArrayOutputStream output = new ByteArrayOutputStream(initialSize)) {
            // version
            output.write(BYTE.to((byte) 0));
            // we have got to cast Cell<?> to Cell<object>. Otherwise, we can't obey CAP#1
            try {
              if (nameBytes.length > Short.MAX_VALUE)
                throw new IllegalArgumentException(
                    "the max size from name is "
                        + Short.MAX_VALUE
                        + " current:"
                        + nameBytes.length);
              if (valueBytes.length > Short.MAX_VALUE)
                throw new IllegalArgumentException(
                    "the max size from value is "
                        + Short.MAX_VALUE
                        + " current:"
                        + valueBytes.length);
              // noted: the (int) length is converted to short type.
              output.write(SHORT.to((short) nameBytes.length));
              output.write(nameBytes);
              output.write(SHORT.to(type.order));
              // noted: the (int) length is converted to short type.
              output.write(SHORT.to((short) valueBytes.length));
              output.write(valueBytes);
            } catch (IOException e) {
              throw new IllegalArgumentException(e);
            }
            output.flush();
            return output.toByteArray();
          } catch (Exception e) {
            throw new IllegalArgumentException(e);
          }
        }

        @Override
        public Cell<?> from(byte[] bytes) {
          try (InputStream input = new ByteArrayInputStream(bytes)) {
            int version = input.read();
            switch (version) {
              case 0:
                String name =
                    STRING.from(
                        forceRead(input, SHORT.from(forceRead(input, ByteUtils.SIZE_OF_SHORT))));
                DataType type = DataType.of(SHORT.from(forceRead(input, ByteUtils.SIZE_OF_SHORT)));
                byte[] valueBytes =
                    forceRead(input, SHORT.from(forceRead(input, ByteUtils.SIZE_OF_SHORT)));
                switch (type) {
                  case BYTES:
                    return Cell.of(name, BYTES.from(valueBytes));
                  case BOOLEAN:
                    return Cell.of(name, BOOLEAN.from(valueBytes));
                  case BYTE:
                    return Cell.of(name, BYTE.from(valueBytes));
                  case SHORT:
                    return Cell.of(name, SHORT.from(valueBytes));
                  case INT:
                    return Cell.of(name, INT.from(valueBytes));
                  case LONG:
                    return Cell.of(name, LONG.from(valueBytes));
                  case FLOAT:
                    return Cell.of(name, FLOAT.from(valueBytes));
                  case DOUBLE:
                    return Cell.of(name, DOUBLE.from(valueBytes));
                  case STRING:
                    return Cell.of(name, STRING.from(valueBytes));
                  case CELL:
                    return Cell.of(name, CELL.from(valueBytes));
                  case ROW:
                    return Cell.of(name, ROW.from(valueBytes));
                  case OBJECT:
                    return Cell.of(name, OBJECT.from(valueBytes));
                  default:
                    throw new UnsupportedClassVersionError(type.getClass().getName());
                }
              default:
                throw new UnsupportedOperationException("Unsupported version:" + version);
            }
          } catch (Exception e) {
            throw new IllegalArgumentException(e);
          }
        }
      };

  /**
   * | version (1 byte) | cell count (int 4 bytes) | first cell length (int 4 bytes) | first cell
   * (bytes) | second cell length (int 4 bytes) | second cell (bytes) | | tag count (short bytes) |
   * first tag length (short 2 bytes) | first tag (bytes) |
   */
  Serializer<Row> ROW =
      new Serializer<Row>() {
        @Override
        public byte[] to(Row row) {
          List<byte[]> cellsInBytes =
              row.cells().stream().map(CELL::to).collect(Collectors.toUnmodifiableList());
          List<byte[]> tagsInBytes =
              row.tags().stream().map(STRING::to).collect(Collectors.toUnmodifiableList());
          int initialSize =
              // version
              ByteUtils.SIZE_OF_BYTE
                  // cells count
                  + ByteUtils.SIZE_OF_INT
                  // all cells' size
                  + cellsInBytes.stream().mapToInt(bs -> bs.length + ByteUtils.SIZE_OF_INT).sum()
                  // tags count
                  + ByteUtils.SIZE_OF_SHORT
                  // all tags' size
                  + tagsInBytes.stream().mapToInt(bs -> bs.length + ByteUtils.SIZE_OF_SHORT).sum();
          try (ByteArrayOutputStream output = new ByteArrayOutputStream(initialSize)) {
            // version
            output.write(BYTE.to((byte) 0));
            // cells count
            output.write(INT.to(row.cells().size()));
            cellsInBytes.forEach(
                bytes -> {
                  int cellSize = bytes.length;
                  try {
                    // cell size
                    output.write(INT.to(cellSize));
                    // cell
                    output.write(bytes);
                  } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                  }
                });

            // process tag
            // noted: the (int) length is converted to short type.
            output.write(SHORT.to((short) row.tags().size()));
            tagsInBytes.forEach(
                tagBytes -> {
                  try {
                    if (tagBytes.length > Short.MAX_VALUE)
                      throw new IllegalArgumentException(
                          "the max size from tag is "
                              + Short.MAX_VALUE
                              + " current:"
                              + tagBytes.length);
                    // noted: the (int) length is converted to short type.
                    output.write(SHORT.to((short) tagBytes.length));
                    output.write(tagBytes);
                  } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                  }
                });
            output.flush();
            return output.toByteArray();
          } catch (Exception e) {
            throw new IllegalArgumentException(e);
          }
        }

        @Override
        public Row from(byte[] bytes) {
          try (InputStream input = new ByteArrayInputStream(bytes)) {
            int version = input.read();
            switch (version) {
              case 0:
                int cellCount = INT.from(forceRead(input, ByteUtils.SIZE_OF_INT));
                if (cellCount < 0)
                  throw new IllegalStateException(
                      "the number from cell should be bigger than zero");
                Cell<?>[] cells =
                    IntStream.range(0, cellCount)
                        .mapToObj(
                            i ->
                                CELL.from(
                                    forceRead(
                                        input, INT.from(forceRead(input, ByteUtils.SIZE_OF_INT)))))
                        .toArray(Cell[]::new);
                int tagCount = SHORT.from(forceRead(input, ByteUtils.SIZE_OF_SHORT));
                if (tagCount < 0)
                  throw new IllegalStateException("the number from tag should be bigger than zero");
                List<String> tag =
                    IntStream.range(0, tagCount)
                        .mapToObj(
                            i ->
                                STRING.from(
                                    forceRead(
                                        input,
                                        SHORT.from(forceRead(input, ByteUtils.SIZE_OF_SHORT)))))
                        .collect(Collectors.toUnmodifiableList());
                return Row.of(tag, cells);
              default:
                throw new UnsupportedOperationException("Unsupported version:" + version);
            }
          } catch (Exception e) {
            throw new IllegalArgumentException(e);
          }
        }
      };

  Serializer<Object> OBJECT =
      new Serializer<Object>() {
        @Override
        public byte[] to(Object obj) {
          try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(100);
              ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(obj);
            out.flush();
            return bytes.toByteArray();
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
        }

        @Override
        public Object from(byte[] bytes) {
          try (ByteArrayInputStream bs = new ByteArrayInputStream(bytes);
              ObjectInputStream input = new ObjectInputStream(bs)) {
            return input.readObject();
          } catch (IOException | java.lang.ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
          }
        }
      };

  static byte[] forceRead(InputStream input, int len) {
    if (len == 0) return new byte[0];
    else if (len < 0) throw new IllegalStateException(len + " should be bigger than zero");
    else {
      int remaining = len;
      byte[] buf = new byte[len];
      while (remaining != 0) {
        try {
          int rval = input.read(buf, buf.length - remaining, remaining);
          if (rval < 0)
            throw new IllegalStateException(
                "required " + len + " but actual " + (len - remaining) + " bytes");
          if (rval > remaining)
            throw new IllegalStateException(
                "ask " + remaining + " bytes but actual " + rval + " bytes");
          remaining -= rval;
        } catch (Throwable e) {
          throw new IllegalStateException(e);
        }
      }
      return buf;
    }
  }
}
