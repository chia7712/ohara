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

package oharastream.ohara.client.configurator

import oharastream.ohara.common.setting.SettingDef
import oharastream.ohara.common.setting.SettingDef.Type
import oharastream.ohara.common.util.CommonUtils

import scala.collection.mutable

/**
  * used to collect definitions. It offers common definition for most components.
  */
trait DefinitionCollector {
  /**
    * set the current group. The following config (for example: name(), group() and so on) will be added
    * to this group.
    * @param currentGroup current group
    * @return this collector
    */
  def addFollowupTo(currentGroup: String): DefinitionCollector

  final def group(): DefinitionCollector =
    definition(
      _.key(GROUP_KEY)
        .documentation("group of this worker cluster")
        .optional(GROUP_DEFAULT)
        .permission(SettingDef.Permission.CREATE_ONLY)
    )

  final def name(): DefinitionCollector =
    definition(
      _.key(NAME_KEY)
        .documentation("name of this worker cluster")
        .stringWithRandomDefault()
        .permission(SettingDef.Permission.CREATE_ONLY)
    )

  final def imageName(imageName: String): DefinitionCollector =
    definition(
      _.key(IMAGE_NAME_KEY)
        .optional(imageName)
        .documentation("the docker image of this service")
        .permission(SettingDef.Permission.CREATE_ONLY)
    )

  final def jmxPort(): DefinitionCollector =
    definition(
      _.key(JMX_PORT_KEY)
        .documentation("the port used to expose the metrics of this cluster")
        .bindingPortWithRandomDefault()
    )

  final def nodeNames(): DefinitionCollector =
    definition(
      _.key(NODE_NAMES_KEY)
        .documentation("the nodes hosting this cluster")
        .denyList(java.util.Set.of(START_COMMAND, STOP_COMMAND, PAUSE_COMMAND, RESUME_COMMAND))
        .reference(SettingDef.Reference.NODE)
    )

  final def routes(): DefinitionCollector =
    definition(
      _.key(ROUTES_KEY)
        .documentation("the extra routes to this service")
        .optional(Type.TAGS)
    )

  final def tags(): DefinitionCollector =
    definition(
      _.key(TAGS_KEY)
        .documentation("the tags to this cluster")
        .optional(Type.TAGS)
    )

  final def maxHeap(): DefinitionCollector =
    definition(
      _.key(MAX_HEAP_KEY)
        .documentation("maximum memory allocation (in MB)")
        .positiveNumber(1024L)
    )

  final def initHeap(): DefinitionCollector =
    definition(
      _.key(INIT_HEAP_KEY)
        .documentation("initial heap size (in MB)")
        .positiveNumber(1024L)
    )

  final def clientPort(): DefinitionCollector =
    definition(
      _.key(CLIENT_PORT_KEY)
        .documentation("the port used to expose the service")
        .bindingPortWithRandomDefault()
    )

  def definition(f: SettingDef.Builder => SettingDef.Builder): DefinitionCollector

  def result: Seq[SettingDef]
}

object DefinitionCollector {
  def apply(): DefinitionCollector = new DefinitionCollector {
    private[this] var currentGroup = "core"
    private[this] val definitions  = mutable.ArrayBuffer[SettingDef]()
    override def addFollowupTo(currentGroup: String): DefinitionCollector = {
      this.currentGroup = CommonUtils.requireNonEmpty(currentGroup)
      this
    }

    override def definition(f: SettingDef.Builder => SettingDef.Builder): DefinitionCollector = {
      val definition = f(SettingDef.builder())
        .group(currentGroup)
        .orderInGroup(definitions.size)
        .build()
      definitions += definition
      this
    }

    override def result: Seq[SettingDef] = definitions.toSeq
  }
}
