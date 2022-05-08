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

import java.io.FileInputStream
import java.lang.reflect.Modifier
import java.util.jar.JarInputStream
import java.util.regex.Pattern

import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest

import scala.jdk.CollectionConverters._

private[code] object ClassUtils {
  /**
    * return the super class and interfaces of input class.
    * @param rootClass input class
    * @return super class and interfaces
    */
  def superClasses(rootClass: Class[_]): Seq[Class[_]] =
    org.apache.commons.lang3.ClassUtils.getAllInterfaces(rootClass).asScala.toSeq ++ org.apache.commons.lang3.ClassUtils
      .getAllSuperclasses(rootClass)
      .asScala
      .toSeq

  /**
    * seek the methods having annotation "Test" from a class
    * @param clz class
    * @return methods having annotation "Test"
    */
  def testCases(clz: Class[_]): Set[String] =
    clz.getMethods
      .filter { m =>
        val annotations = m.getAnnotations
        if (annotations == null || annotations.isEmpty) false
        else
          annotations.exists(
            annotation =>
              annotation.annotationType() == classOf[Test]
                || annotation.annotationType() == classOf[ParameterizedTest]
          )
      }
      .map(_.getName)
      .toSet

  /**
    * Java generate $number class for the class which has private constructor and we don't care for them in some tests.
    * This helper method is used to filter them
    * from our tests
    * @param clz class
    * @return true if the input class is anonymous.
    */
  def isAnonymous(clz: Class[_]): Boolean = clz.getName.contains("$")

  def isAbstract(clz: Class[_]): Boolean = Modifier.isAbstract(clz.getModifiers)

  /**
    * Find the test classes which have following attributes.
    * 1) in tests.jar
    * 2) the name starts with "Test"
    * 3) non anonymous class (It is impossible to use anonymous class to write ohara test)
    * @return test classes
    */
  def testClasses(): Seq[Class[_]] =
    allClasses(_.contains("tests.jar"))
    // the test class should not be anonymous class
      .filterNot(isAnonymous)
      // the previous filter had chosen the normal class so it is safe to produce the simple name for classes
      .filter(_.getSimpleName.startsWith("Test"))

  /**
    * @return all classes in testing scope
    */
  def classesInTestScope(): Seq[Class[_]] = allClasses(_.contains("tests.jar"))

  /**
    * @return all classes in production scope
    */
  def classesInProductionScope(): Seq[Class[_]] = allClasses(n => !n.contains("tests.jar"))

  def allClasses(fileNameFilter: String => Boolean): Seq[Class[_]] = {
    val classLoader = ClassLoader.getSystemClassLoader
    val path        = "oharastream/ohara"
    val pattern     = Pattern.compile("^file:(.+\\.jar)!/" + path + "$")
    val urls        = classLoader.getResources(path)
    urls.asScala
      .map(url => pattern.matcher(url.getFile))
      .filter(_.find())
      .map(_.group(1))
      .filter(fileNameFilter)
      .flatMap { f =>
        val jarInput = new JarInputStream(new FileInputStream(f))
        try Iterator
          .continually(jarInput.getNextJarEntry)
          .takeWhile(_ != null)
          .map(_.getName)
          .toArray
          .filter(_.endsWith(".class"))
          .map(_.replace('/', '.'))
          .map(className => className.substring(0, className.length - ".class".length))
          .map(Class.forName)
        finally jarInput.close()
      }
      .toSeq
  }
}
