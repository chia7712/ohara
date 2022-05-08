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

import java.io.IOException;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestExceptionHandler extends OharaTest {

  @Test
  public void addDuplicateFunction() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ExceptionHandler.builder()
                .with(IOException.class, Exception::new)
                .with(IOException.class, Exception::new));
  }

  @Test
  public void nullClass() {
    Assertions.assertThrows(
        NullPointerException.class, () -> ExceptionHandler.builder().with(null, Exception::new));
  }

  @Test
  public void nullFunction() {
    Assertions.assertThrows(
        NullPointerException.class, () -> ExceptionHandler.builder().with(IOException.class, null));
  }

  @Test
  public void testHandle() {
    Assertions.assertThrows(
        TimeoutException.class,
        () ->
            ExceptionHandler.builder()
                .with(IOException.class, TimeoutException::new)
                .build()
                .handle(
                    () -> {
                      throw new IOException("HELLO WORLD");
                    }));
  }

  @Test
  public void testDefaultHandle() {
    Assertions.assertThrows(
        Exception.class,
        () ->
            ExceptionHandler.builder()
                .with(IOException.class, TimeoutException::new)
                .build()
                .handle(
                    () -> {
                      throw new java.lang.InterruptedException("HELLO WORLD");
                    }));
  }

  @Test
  public void testManyHandlers() {
    Assertions.assertThrows(
        TimeoutException.class,
        () ->
            ExceptionHandler.builder()
                .with(java.lang.InterruptedException.class, TimeoutException::new)
                .with(IOException.class, ExecutionException::new)
                .build()
                .handle(
                    () -> {
                      throw new java.lang.InterruptedException("HELLO WORLD");
                    }));
  }
}
