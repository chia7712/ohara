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

public class TestReleaseOnce extends OharaTest {

  @Test
  public void testIsClosed() {
    SimpleReleaseOnce c = new SimpleReleaseOnce();
    c.close();
    Assertions.assertTrue(c.isClosed());
  }

  @Test
  public void testCloseOnce() {
    SimpleReleaseOnce c = new SimpleReleaseOnce();
    c.close();
    Assertions.assertEquals(1, c.closeCount);
    c.close();
    Assertions.assertEquals(1, c.closeCount);
  }

  @Test
  public void testSwallowException() {
    Releasable.close(new TerribleReleaseOnce());
  }

  /** NOTED: all exception is converted to RuntimeException */
  @Test
  public void testThrowException() {
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            Releasable.close(
                new TerribleReleaseOnce(),
                t -> {
                  throw new RuntimeException(t);
                }));
  }

  private static class SimpleReleaseOnce extends ReleaseOnce {
    private int closeCount = 0;

    @Override
    protected void doClose() {
      ++closeCount;
    }
  }

  private static class TerribleReleaseOnce extends ReleaseOnce {

    @Override
    protected void doClose() {
      throw new RuntimeException("awwwwwwwww");
    }
  }
}
