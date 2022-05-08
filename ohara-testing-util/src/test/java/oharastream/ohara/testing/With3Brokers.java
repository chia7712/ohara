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

package oharastream.ohara.testing;

import java.util.NoSuchElementException;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.Releasable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/**
 * This class create a mini broker cluster with 3 nodes. And the cluster will be closed after all
 * test cases have been done.
 *
 * <p>NOTED: All subclass has "same" reference to util. This is ok in junit test since it default
 * run each test on "different" jvm. The "same" static member won't cause trouble in testing.
 * However, you should move the static "util" into your test if you don't depend on junit...by chia
 */
public abstract class With3Brokers extends OharaTest {
  protected static OharaTestUtils util;

  @BeforeAll
  public static void beforeAll() {
    if (util != null)
      throw new NoSuchElementException(
          "The test util had been initialized!!! This happens on your tests don't run on different jvm");
    util = OharaTestUtils.brokers(3);
  }

  protected OharaTestUtils testUtil() {
    return util;
  }

  @AfterAll
  public static void afterAll() {
    Releasable.close(util);
    // we have to assign null to util since we allow junit to reuse jvm
    util = null;
  }
}
