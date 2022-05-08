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

import java.util.List;
import oharastream.ohara.common.data.Cell;
import oharastream.ohara.common.data.Column;
import oharastream.ohara.common.data.DataType;
import oharastream.ohara.common.data.Row;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestConnectorUtils extends OharaTest {
  @Test
  public void testMatch() {
    Column column =
        Column.builder()
            .name(CommonUtils.randomString(10))
            .newName(CommonUtils.randomString(10))
            .dataType(DataType.STRING)
            .build();

    // test illegal type
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ConnectorUtils.match(Row.of(Cell.of(column.name(), 123)), List.of(column), true));

    // test illegal name
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ConnectorUtils.match(
                Row.of(Cell.of(CommonUtils.randomString(), 123)), List.of(column), true));

    // pass
    ConnectorUtils.match(
        Row.of(Cell.of(column.name(), CommonUtils.randomString())), List.of(column), true);

    // test illegal type
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ConnectorUtils.match(Row.of(Cell.of(column.newName(), 123)), List.of(column), false));

    // test illegal name
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ConnectorUtils.match(
                Row.of(Cell.of(CommonUtils.randomString(), 123)), List.of(column), false));

    // pass
    ConnectorUtils.match(
        Row.of(Cell.of(column.newName(), CommonUtils.randomString())), List.of(column), false);
  }
}
