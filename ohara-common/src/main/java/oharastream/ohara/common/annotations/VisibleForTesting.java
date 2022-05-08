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
 * add this annotation to the method/member if you "open" the access modifiers in order to test the
 * related code. For example, an method which should have "private" modifier is marked as "package
 * private" since a test case has got to access the method to verify somethings to complete test.
 */
public @interface VisibleForTesting {}
