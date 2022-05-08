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

package oharastream.ohara.metrics.kafka;

import java.util.concurrent.TimeUnit;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTopicMeter extends OharaTest {

  @Test
  public void nullTopicName() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new TopicMeter(
                null,
                TopicMeter.Catalog.BytesInPerSec,
                CommonUtils.current(),
                CommonUtils.randomString(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                TimeUnit.DAYS,
                CommonUtils.current()));
  }

  @Test
  public void nullTopicNameInBuilder() {
    Assertions.assertThrows(
        NullPointerException.class, () -> TopicMeter.builder().topicName(null).build());
  }

  @Test
  public void emptyTopicName() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new TopicMeter(
                "",
                TopicMeter.Catalog.BytesInPerSec,
                CommonUtils.current(),
                CommonUtils.randomString(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                TimeUnit.DAYS,
                CommonUtils.current()));
  }

  @Test
  public void emptyTopicNameInBuilder() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TopicMeter.builder().topicName("").build());
  }

  @Test
  public void nullCatalog() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new TopicMeter(
                CommonUtils.randomString(),
                null,
                CommonUtils.current(),
                CommonUtils.randomString(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                TimeUnit.DAYS,
                CommonUtils.current()));
  }

  @Test
  public void nullCatalogInBuilder() {
    Assertions.assertThrows(
        NullPointerException.class, () -> TopicMeter.builder().catalog(null).build());
  }

  @Test
  public void nullEventType() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new TopicMeter(
                CommonUtils.randomString(),
                TopicMeter.Catalog.BytesInPerSec,
                CommonUtils.current(),
                null,
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                TimeUnit.DAYS,
                CommonUtils.current()));
  }

  @Test
  public void nullEventTypeInBuilder() {
    Assertions.assertThrows(
        NullPointerException.class, () -> TopicMeter.builder().eventType(null).build());
  }

  @Test
  public void emptyEventType() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new TopicMeter(
                CommonUtils.randomString(),
                TopicMeter.Catalog.BytesInPerSec,
                CommonUtils.current(),
                "",
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                TimeUnit.DAYS,
                CommonUtils.current()));
  }

  @Test
  public void emptyEventTypeInBuilder() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TopicMeter.builder().eventType("").build());
  }

  @Test
  public void nullTimeUnit() {
    Assertions.assertThrows(
        NullPointerException.class,
        () ->
            new TopicMeter(
                CommonUtils.randomString(),
                TopicMeter.Catalog.BytesInPerSec,
                CommonUtils.current(),
                CommonUtils.randomString(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                (double) CommonUtils.current(),
                null,
                CommonUtils.current()));
  }

  @Test
  public void nullTimeUnitInBuilder() {
    Assertions.assertThrows(
        NullPointerException.class, () -> TopicMeter.builder().rateUnit(null).build());
  }

  @Test
  public void testGetter() {
    String topicName = CommonUtils.randomString();
    TopicMeter.Catalog catalog = TopicMeter.Catalog.BytesInPerSec;
    long count = CommonUtils.current();
    String eventType = CommonUtils.randomString();
    double fifteenMinuteRate = (double) CommonUtils.current();
    double fiveMinuteRate = (double) CommonUtils.current();
    double meanRate = (double) CommonUtils.current();
    double oneMinuteRate = (double) CommonUtils.current();
    TimeUnit rateUnit = TimeUnit.HOURS;
    TopicMeter meter =
        new TopicMeter(
            topicName,
            catalog,
            count,
            eventType,
            fifteenMinuteRate,
            fiveMinuteRate,
            meanRate,
            oneMinuteRate,
            rateUnit,
            CommonUtils.current());

    Assertions.assertEquals(topicName, meter.topicName());
    Assertions.assertEquals(catalog, meter.catalog());
    Assertions.assertEquals(count, meter.count());
    Assertions.assertEquals(eventType, meter.eventType());
    Assertions.assertEquals(fifteenMinuteRate, meter.fifteenMinuteRate(), 0);
    Assertions.assertEquals(fiveMinuteRate, meter.fiveMinuteRate(), 0);
    Assertions.assertEquals(meanRate, meter.meanRate(), 0);
    Assertions.assertEquals(oneMinuteRate, meter.oneMinuteRate(), 0);
    Assertions.assertEquals(rateUnit, meter.rateUnit());
  }
}
