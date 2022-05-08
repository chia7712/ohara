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

public class TestSleeper extends OharaTest {

  @Test
  public void testTimeToSleep() {
    int count = 0;
    Sleeper sleeper = new Sleeper();
    do {
      count++;
    } while (sleeper.tryToSleep());
    Assertions.assertEquals(count, 4);
    do {
      count++;
    } while (sleeper.tryToSleep());
    Assertions.assertEquals(count, 5);
    do {
      count++;
    } while (sleeper.tryToSleep());
    Assertions.assertEquals(count, 6);
  }
}
