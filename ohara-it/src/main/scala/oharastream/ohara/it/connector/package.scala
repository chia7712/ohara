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

package oharastream.ohara.it
import java.util

import oharastream.ohara.common.setting.SettingDef.{Reference, Type}
import oharastream.ohara.common.setting.{SettingDef, TableColumn}

package object connector {
  /**
    * add some definitions for testing.
    */
  val ALL_SETTING_DEFINITIONS: Map[String, SettingDef] = Seq(
    SettingDef
      .builder()
      .displayName("dumb boolean")
      .key("dumb.boolean")
      .documentation("boolean for testing")
      .optional(false)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb string")
      .key("dumb.string")
      .documentation("string for testing")
      .optional("random")
      .build(),
    SettingDef
      .builder()
      .displayName("dumb short")
      .key("dumb.short")
      .documentation("short for testing")
      .optional(10.asInstanceOf[Short])
      .build(),
    SettingDef
      .builder()
      .displayName("dumb positive short")
      .key("dumb.positive.short")
      .documentation("positive short for testing")
      .positiveNumber(10.asInstanceOf[Short])
      .build(),
    SettingDef
      .builder()
      .displayName("dumb integer")
      .key("dumb.integer")
      .documentation("integer for testing")
      .optional(10)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb positive integer")
      .key("dumb.positive.integer")
      .documentation("positive integer for testing")
      .positiveNumber(10)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb long")
      .key("dumb.long")
      .documentation("long for testing")
      .optional(10L)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb positive long")
      .key("dumb.positive.long")
      .documentation("positive long for testing")
      .positiveNumber(10L)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb double")
      .key("dumb.double")
      .documentation("double for testing")
      .optional(10d)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb positive double")
      .key("dumb.positive.double")
      .documentation("positive double for testing")
      .positiveNumber(10d)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb array")
      .key("dumb.array")
      .optional(Type.ARRAY)
      .documentation("array for testing")
      .build(),
    SettingDef
      .builder()
      .displayName("dumb class")
      .key("dumb.class")
      .optional(Type.CLASS)
      .documentation("class for testing")
      .build(),
    SettingDef
      .builder()
      .displayName("dumb password")
      .key("dumb.password")
      .optional(Type.PASSWORD)
      .documentation("password for testing")
      .build(),
    SettingDef
      .builder()
      .displayName("dumb jdbc table")
      .key("dumb.jdbc.table")
      .optional(Type.JDBC_TABLE)
      .documentation("jdbc table for testing")
      .build(),
    SettingDef
      .builder()
      .displayName("dumb table")
      .key("dumb.table")
      .optional(
        util.Arrays.asList(
          TableColumn
            .builder()
            .`type`(TableColumn.Type.NUMBER)
            .name("number item")
            .build(),
          TableColumn
            .builder()
            .`type`(TableColumn.Type.STRING)
            .name("string item")
            .build(),
          TableColumn
            .builder()
            .`type`(TableColumn.Type.BOOLEAN)
            .name("boolean item")
            .build()
        )
      )
      .documentation("jdbc table for testing")
      .build(),
    SettingDef
      .builder()
      .displayName("dumb duration")
      .key("dumb.duration")
      .documentation("duration for testing")
      .optional(java.time.Duration.ofSeconds(30))
      .build(),
    SettingDef
      .builder()
      .displayName("dumb port")
      .key("dumb.port")
      .documentation("port for testing")
      .optionalPort(22)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb binding port")
      .key("dumb.binding.port")
      .documentation("binding port for testing")
      .optionalBindingPort(12345)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb object keys")
      .key("dumb.object.keys")
      .documentation("object keys for testing")
      .optional(Type.OBJECT_KEYS)
      .reference(Reference.FILE)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb object keys2")
      .key("dumb.object.keys2")
      .documentation("object keys2 for testing")
      .optional(Type.OBJECT_KEYS)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb object key")
      .key("dumb.object.key")
      .documentation("object key for testing")
      .optional(Type.OBJECT_KEY)
      .reference(Reference.TOPIC)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb object key2")
      .key("dumb.object.key2")
      .documentation("object key2 for testing")
      .optional(Type.OBJECT_KEY)
      .build(),
    SettingDef
      .builder()
      .displayName("dumb tags")
      .key("dumb.tags")
      .optional(Type.TAGS)
      .documentation("Tags")
      .build()
  ).map(s => s.key() -> s).toMap
}
