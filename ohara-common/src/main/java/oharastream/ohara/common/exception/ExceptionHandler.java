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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * this class catches the checked exception and then rethrow a unchecked exception instead. You can
 * proxy your function, which produces checked exception, via this handler if you do hate the
 * checked exception obstructing you from writing graceful lambda expression.
 *
 * <p>Noted: the exception does not disappear and you ought to catch the converted exceptions and
 * then give them love so as to produce beautiful result :)
 */
@FunctionalInterface
public interface ExceptionHandler {

  /**
   * convert the execked stuff to unchecked. Use this default handler if all you want is to
   * eliminate the checked exceptions.
   */
  ExceptionHandler DEFAULT = builder().build();

  <T> T handle(Worker<T> worker);

  @FunctionalInterface
  interface Worker<T> {
    T process() throws Throwable;
  }

  static Builder builder() {
    return new Builder();
  }

  class Builder implements oharastream.ohara.common.pattern.Builder<ExceptionHandler> {
    private static final Function<Throwable, RuntimeException> BASIC_FUNCTION =
        exception -> {
          if (exception instanceof RuntimeException) {
            return (RuntimeException) exception;
          } else {
            return new oharastream.ohara.common.exception.Exception(exception);
          }
        };

    private final Map<Class<? extends Throwable>, Function<Throwable, RuntimeException>>
        exceptionFunctions = new HashMap<>();

    private Builder() {}

    public Builder with(
        Class<? extends Throwable> clz, Function<Throwable, RuntimeException> function) {
      Objects.requireNonNull(clz);
      Objects.requireNonNull(function);
      if (exceptionFunctions.containsKey(clz))
        throw new IllegalArgumentException(clz + " already has handler");
      exceptionFunctions.put(clz, function);
      return this;
    }

    @Override
    public ExceptionHandler build() {
      return new ExceptionHandler() {
        @Override
        public <T> T handle(Worker<T> worker) {
          try {
            return worker.process();
          } catch (Throwable e) {
            throw exceptionFunctions.getOrDefault(e.getClass(), BASIC_FUNCTION).apply(e);
          }
        }
      };
    }
  }
}
