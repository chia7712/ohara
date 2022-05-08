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

import oharastream.ohara.common.setting.SettingDef;

/**
 * This class represent that the input configs are rejected by SettingDef.
 *
 * <p>There are many resources having a bunch of available configs, and most of them are exposed to
 * restful APIs. In order to avoid wrong configs in using resources, SettingDef offers many check
 * rules to validate input configs. This exception is used to say what happens on your configs.
 *
 * <p>Note: We wrap the {@link SettingDef#checker()} exception to this class since all definitions
 * of Ohara object should use <b>SettingDef</b> to define. Some implementation may want to handle
 * their own exception (like kafka config exception) should be responsible for their exception
 * casting.
 */
public class ConfigException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public ConfigException(String message) {
    super(message);
  }

  public ConfigException(String name, Object value) {
    this(name, value, null);
  }

  public ConfigException(String name, Object value, String message) {
    super(
        String.format(
            "Invalid value \"%s\" of type \"%s\" for configuration \"%s\" :\n%s",
            value, value.getClass().getName(), name, message == null ? "" : message));
  }
}
