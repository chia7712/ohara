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

package oharastream.ohara.common.exception;

public class InterruptedException extends oharastream.ohara.common.exception.Exception {
  private static final long serialVersionUID = 1L;

  public InterruptedException() {}

  public InterruptedException(Throwable e) {
    super(e);
  }

  public InterruptedException(String message, Throwable e) {
    super(message, e);
  }
}
