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

package oharastream.ohara.metrics;

import java.util.Map;
import java.util.Objects;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBeanChannel extends OharaTest {

  @Test
  public void testLocalFlag() {
    BeanChannel.Builder builder = BeanChannel.builder().hostname("Asdad");
    Assertions.assertFalse(builder.local);
  }

  @Test
  public void listLocal() {
    BeanChannel channel = BeanChannel.local();
    Assertions.assertTrue(channel.iterator().hasNext());
    channel.forEach(
        beanObject -> {
          CommonUtils.requireNonEmpty(beanObject.domainName());
          Objects.requireNonNull(beanObject.properties());
          Objects.requireNonNull(beanObject.attributes());
        });
  }

  @Test
  public void loadSpecificMean() {
    String domain = CommonUtils.randomString();
    Map<String, String> props = Map.of("a", "b");
    double value0 = 1.2;
    double value1 = 2.2;

    BeanChannel.register()
        .domain(domain)
        .properties(props)
        .beanObject(new SimpleInfo(value0, value1))
        .run();

    Assertions.assertEquals(1, BeanChannel.builder().local().domainName(domain).build().size());

    BeanChannel.builder().local().domainName(domain).build().stream()
        .filter(beanObject -> beanObject.domainName().equals(domain))
        .forEach(
            beanObject -> {
              Assertions.assertEquals(value0, (double) beanObject.attributes().get("Value0"), 0);
              Assertions.assertEquals(value1, (double) beanObject.attributes().get("Value1"), 0);
            });
  }

  public interface SimpleInfoMBean {
    double getValue0();

    double getValue1();
  }

  static class SimpleInfo implements SimpleInfoMBean {
    private final double value0;
    private final double value1;

    SimpleInfo(double value0, double value1) {
      this.value0 = value0;
      this.value1 = value1;
    }

    @Override
    public double getValue0() {
      return value0;
    }

    @Override
    public double getValue1() {
      return value1;
    }
  }
}
