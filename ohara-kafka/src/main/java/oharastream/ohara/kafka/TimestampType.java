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

/**
 * The timestamp type of the records. NOTED: those names MUST be same with
 * org.apache.kafka.common.record.TimestampType
 */
public enum TimestampType {
  NO_TIMESTAMP_TYPE,
  CREATE_TIME,
  LOG_APPEND_TIME;

  public static TimestampType of(org.apache.kafka.common.record.TimestampType type) {
    if (type == org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE)
      return NO_TIMESTAMP_TYPE;
    if (type == org.apache.kafka.common.record.TimestampType.CREATE_TIME) return CREATE_TIME;
    if (type == org.apache.kafka.common.record.TimestampType.LOG_APPEND_TIME)
      return LOG_APPEND_TIME;
    throw new IllegalArgumentException("unknown " + type);
  }
}
