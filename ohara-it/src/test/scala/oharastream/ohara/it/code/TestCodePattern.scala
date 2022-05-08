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

package oharastream.ohara.it.code

import oharastream.ohara.common.pattern.{Builder, Creator}
import oharastream.ohara.common.rule.OharaTest
import org.junit.jupiter.api.Test
import org.scalatest.matchers.should.Matchers._
import ClassUtils._

/**
  * this test class is used to find out the invalid format of builder/creator.
  * Builder/Creator pattern is across whole ohara project, and we try to make all impls
  * have similar form of method signature.
  */
class TestCodePattern extends OharaTest {
  @Test
  def testBuilder(): Unit =
    checkBaseClasses(
      baseClass = classOf[Builder[_]],
      postfix = "builder",
      excludedClasses = Seq(
        classOf[Builder[_]]
      )
    )

  @Test
  def testMethodNameForBuilder(): Unit =
    checkMethodNames(
      postfix = "builder",
      illegalPrefixName = Set(
        "set",
        "get",
        "remove"
      ),
      excludedMethods = Set(
        "getClass"
      )
    )

  @Test
  def testCreator(): Unit =
    checkBaseClasses(
      baseClass = classOf[Creator[_]],
      postfix = "creator",
      excludedClasses = Seq(
        classOf[Creator[_]]
      )
    )

  @Test
  def testMethodNameForCreator(): Unit =
    checkMethodNames(
      postfix = "creator",
      illegalPrefixName = Set(
        "set",
        "get",
        "remove"
      ),
      excludedMethods = Set(
        "getClass",
        "setting",
        "setting$",
        "settings",
        "settings$",
        "removeContainerOnExit",
        "removeContainerOnExit$"
      )
    )

  private[this] def checkMethodNames(
    postfix: String,
    illegalPrefixName: Set[String],
    excludedMethods: Set[String]
  ): Unit = {
    val invalidClassesAndMethods = classesInProductionScope()
      .filter(_.getName.toLowerCase.endsWith(postfix))
      .map(
        clz =>
          clz -> clz.getMethods
            .filter(m => illegalPrefixName.exists(n => m.getName.startsWith(n)))
            .filterNot(m => excludedMethods.contains(m.getName))
      )
      .filter(_._2.nonEmpty)
    if (invalidClassesAndMethods.nonEmpty)
      throw new IllegalArgumentException(
        invalidClassesAndMethods
          .map(
            clzAndMethods =>
              s"${clzAndMethods._1} has following illegal methods:${clzAndMethods._2.map(_.getName).mkString(",")}"
          )
          .mkString("|")
      )
  }

  private[this] def checkBaseClasses(baseClass: Class[_], postfix: String, excludedClasses: Seq[Class[_]]): Unit = {
    val classes = classesInProductionScope()
    classes.size should not be 0
    val invalidClasses = classes
      .filterNot(clz => superClasses(clz).contains(baseClass))
      .filter(_.getName.toLowerCase.endsWith(postfix))
      .filter(clz => !excludedClasses.contains(clz))
    if (invalidClasses.nonEmpty)
      throw new IllegalArgumentException(
        s"those classes:${invalidClasses.map(_.getName).mkString(",")} do not extend $baseClass"
      )
  }
}
