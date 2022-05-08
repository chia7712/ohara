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

package oharastream.ohara.testing.service;

import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDatabase extends OharaTest {

  @Test
  public void nullUser() {
    Assertions.assertThrows(
        NullPointerException.class, () -> Database.builder().user(null).build());
  }

  @Test
  public void emptyUser() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Database.builder().user("").build());
  }

  @Test
  public void nullPassword() {
    Assertions.assertThrows(
        NullPointerException.class, () -> Database.builder().password(null).build());
  }

  @Test
  public void emptyPassword() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Database.builder().password("").build());
  }

  @Test
  public void nullDatabaseName() {
    Assertions.assertThrows(
        NullPointerException.class, () -> Database.builder().databaseName(null).build());
  }

  @Test
  public void emptyDatabase() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Database.builder().databaseName("").build());
  }

  @Test
  public void negativeControlPort() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> Database.builder().port(-1).build());
  }

  @Test
  public void testSpecificPort() {
    int port = CommonUtils.availablePort();
    try (Database db = Database.builder().port(port).build()) {
      Assertions.assertEquals(port, db.port());
    }
  }

  @Test
  public void testRandomPort() {
    try (Database db = Database.builder().build()) {
      Assertions.assertNotEquals(0, db.port());
    }
  }

  @Test
  public void testPortOfLocal() {
    try (Database db = Database.local()) {
      Assertions.assertNotEquals(0, db.port());
    }
  }
}
