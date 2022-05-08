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

package oharastream.ohara.shabondi.sink

import java.time.{Duration => JDuration}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

import oharastream.ohara.common.data.Row

private[sink] class RowQueue extends ConcurrentLinkedQueue[Row] {
  private[sink] val lastTime = new AtomicLong(System.currentTimeMillis())

  override def poll(): Row =
    try {
      super.poll()
    } finally {
      lastTime.set(System.currentTimeMillis())
    }

  def isIdle(idleTime: JDuration): Boolean =
    System.currentTimeMillis() > (idleTime.toMillis + lastTime.get())
}
