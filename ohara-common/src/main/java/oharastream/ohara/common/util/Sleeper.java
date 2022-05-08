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

import java.util.concurrent.TimeUnit;

/**
 * implement the sleep time based on exponential growth. The min value is 100ms and max value is
 * 1000ms.
 */
public class Sleeper {
  private static final long INIT_SLEEP_TIME = 100;
  private static final long MAX_SLEEP_TIME = 1000; // The 1000 is 1 seconds
  private long nextSleepTime = INIT_SLEEP_TIME;

  /**
   * It should be updated to an new value (multiplied by 2) after sleep.
   *
   * @return True if it does not reach the max sleep time. Otherwise, false.
   */
  public boolean tryToSleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(nextSleepTime);
      nextSleepTime = Math.min(nextSleepTime * 2, MAX_SLEEP_TIME);
      return nextSleepTime < MAX_SLEEP_TIME;
    } catch (InterruptedException e) {
      // restore the interrupt
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
