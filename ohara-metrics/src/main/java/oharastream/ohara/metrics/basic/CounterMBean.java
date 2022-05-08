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

package oharastream.ohara.metrics.basic;

import oharastream.ohara.common.setting.ObjectKey;
import oharastream.ohara.metrics.BeanObject;

public interface CounterMBean {
  String DOMAIN = "oharastream.ohara";
  String TYPE_KEY = "type";
  String TYPE_VALUE = "counter";
  /**
   * we have to put the key in properties in order to distinguish the metrics in GUI tool (for
   * example, jmc)
   */
  String KEY_KEY = "key";

  /**
   * we have to put the item in properties in order to distinguish the metrics in GUI tool (for
   * example, jmc)
   */
  String ITEM_KEY = "item";

  /** This is a internal property used to distinguish the counter. */
  String ID_KEY = "id";

  String START_TIME_KEY = "StartTime";
  String VALUE_KEY = "Value";
  String LAST_MODIFIED_KEY = "LastModified";
  String DOCUMENT_KEY = "Document";
  String UNIT_KEY = "Unit";

  static boolean is(BeanObject obj) {
    return obj.domainName().equals(DOMAIN)
        && TYPE_VALUE.equals(obj.properties().get(TYPE_KEY))
        && obj.properties().containsKey(ITEM_KEY)
        && obj.properties().containsKey(KEY_KEY)
        && ObjectKey.ofPlain(obj.properties().get(KEY_KEY)).isPresent()
        && obj.attributes().containsKey(START_TIME_KEY)
        && obj.attributes().containsKey(LAST_MODIFIED_KEY)
        && obj.attributes().containsKey(VALUE_KEY)
        && obj.attributes().containsKey(DOCUMENT_KEY)
        && obj.attributes().containsKey(UNIT_KEY);
  }

  static CounterMBean of(BeanObject obj) {
    return Counter.builder()
        // NOTED: key is NOT a part of attribute!!!!
        .key(ObjectKey.requirePlain(obj.properties().get(KEY_KEY)))
        // NOTED: item is NOT a part of attribute!!!!
        .item(obj.properties().get(ITEM_KEY))
        .startTime((long) obj.attributes().get(START_TIME_KEY))
        .lastModified((long) obj.attributes().get(LAST_MODIFIED_KEY))
        .queryTime(obj.queryTime())
        .value((long) obj.attributes().get(VALUE_KEY))
        .document((String) obj.attributes().get(DOCUMENT_KEY))
        .unit((String) obj.attributes().get(UNIT_KEY))
        .build();
  }

  /**
   * NOTED: this is NOT a part of java beans!!!
   *
   * @return name of this counter
   */
  ObjectKey key();

  /**
   * NOTED: this is NOT a part of java beans!!!
   *
   * @return name of this counter
   */
  String item();

  /**
   * NOTED: if you are going to change the method name, you have to rewrite the {@link
   * CounterMBean#START_TIME_KEY} also
   *
   * @return the start time of this counter
   */
  long getStartTime();

  /**
   * Get query time
   *
   * @return the time of querying metrics object
   */
  long getQueryTime();

  /**
   * Get last modified time
   *
   * @return the time of modifying metrics object
   */
  long getLastModified();

  /**
   * NOTED: if you are going to change the method name, you have to rewrite the {@link
   * CounterMBean#VALUE_KEY} also
   *
   * @return current value of counter
   */
  long getValue();

  /**
   * NOTED: if you are going to change the method name, you have to rewrite the {@link
   * CounterMBean#UNIT_KEY} also
   *
   * @return the unit of value
   */
  String getUnit();

  /**
   * NOTED: if you are going to change the method name, you have to rewrite the {@link
   * CounterMBean#DOCUMENT_KEY} also
   *
   * @return description of counter
   */
  String getDocument();

  /**
   * A helper method to calculate the average in per second
   *
   * @return value in per sec
   */
  default double valueInPerSec() {
    long duration = getLastModified() - getStartTime();
    if (duration <= 0) return 0;
    else return ((double) getValue() / (double) duration) * (double) 1000;
  }
}
