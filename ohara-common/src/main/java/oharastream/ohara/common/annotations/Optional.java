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

package oharastream.ohara.common.annotations;

/**
 * There are many builder in ohara. Normally some methods, which used to assign value to member, are
 * not required. For example, the value can be generated automatically. This annotation is used to
 * "hint" developer that the methods which can be "ignored" in production purpose.
 */
public @interface Optional {
  String value() default "This method or member is not required to call or use";
}
