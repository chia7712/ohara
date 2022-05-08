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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import oharastream.ohara.common.rule.OharaTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStreamUtils extends OharaTest {

  private final List<String> names = Arrays.asList("a", "b", "c");

  @Test
  public void testZipWithIndex() {
    Map<Integer, String> namesWithIndex =
        StreamUtils.zipWithIndex(names.stream())
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
    Assertions.assertEquals(3, namesWithIndex.size());
    Assertions.assertEquals("a", namesWithIndex.get(0));
    Assertions.assertEquals("b", namesWithIndex.get(1));
    Assertions.assertEquals("c", namesWithIndex.get(2));
  }
}
