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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class implements the {@link AutoCloseable} and it make sure {@link #close()} is executed
 * only once. Since java disallow interface to have member, this class has got to be a abstract
 * class. Hence, you SHOULD NOT apply this class to the "interface" layer. This class is more
 * suitable to the implementation.
 */
public abstract class ReleaseOnce implements Releasable {
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** @return true if this object have been closed */
  public boolean isClosed() {
    return closed.get();
  }
  /** Do what you want to do when calling closing. */
  protected abstract void doClose();

  @Override
  public final void close() {
    if (closed.compareAndSet(false, true)) doClose();
  }
}
