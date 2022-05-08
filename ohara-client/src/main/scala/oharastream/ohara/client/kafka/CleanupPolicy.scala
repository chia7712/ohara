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

package oharastream.ohara.client.kafka
import oharastream.ohara.client.Enum

/**
  * Mapping to kafka's topic cleanup policy.
  * NOTED: the name is part of "kafka topic config", so "DON'T" change it arbitrarily
  *
  * @see org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG
  * @see org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_COMPACT
  * @see org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE
  */
sealed abstract class CleanupPolicy(val name: String)

object CleanupPolicy extends Enum[CleanupPolicy] {
  case object DELETE    extends CleanupPolicy("delete")
  case object COMPACTED extends CleanupPolicy("compact")
}
