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

package oharastream.ohara.kafka;

import java.util.Objects;

/**
 * get TopicOption from kafka client
 *
 * @see TopicAdmin ;
 */
public class TopicOption {
  private final String key;
  private final String value;
  private final boolean isDefault;
  private final boolean sensitive;
  private final boolean readOnly;

  public TopicOption(
      String key, String value, boolean isDefault, boolean sensitive, boolean readOnly) {
    this.key = key;
    this.value = value;
    this.isDefault = isDefault;
    this.sensitive = sensitive;
    this.readOnly = readOnly;
  }

  public String key() {
    return key;
  }

  public String value() {
    return value;
  }

  public boolean isDefault() {
    return isDefault;
  }

  public boolean sensitive() {
    return sensitive;
  }

  public boolean readOnly() {
    return readOnly;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicOption that = (TopicOption) o;
    return Objects.equals(key, that.key)
        && Objects.equals(value, that.value)
        && Objects.equals(isDefault, that.isDefault)
        && Objects.equals(sensitive, that.sensitive)
        && Objects.equals(readOnly, that.readOnly);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value, isDefault, sensitive, readOnly);
  }

  @Override
  public String toString() {
    return "key="
        + key
        + " value="
        + value
        + " sensitive="
        + sensitive
        + " default="
        + isDefault
        + " readOnly="
        + readOnly;
  }
}
