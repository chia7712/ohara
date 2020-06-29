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

import java.util.Objects
import java.util.concurrent.TimeUnit

import oharastream.ohara.common.setting.SettingDef.{Necessary, Permission, Type}
import oharastream.ohara.common.setting.{ObjectKey, PropGroup, SettingDef}
import oharastream.ohara.common.util.CommonUtils
import spray.json.DefaultJsonProtocol._
import spray.json.{
  DeserializationException,
  JsArray,
  JsBoolean,
  JsNull,
  JsNumber,
  JsObject,
  JsString,
  JsValue,
  RootJsonFormat
}

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.reflect.{ClassTag, classTag}

/**
  * this is a akka-json representation++ which offers many useful sugar conversion of input json.
  * The order of applying rules is shown below.
  * 1) convert the value to another type
  * 2) convert the null to the value of another key
  * 3) convert the null to default value
  * 4) value check
  *
  * Noted: the keys having checker MUST exist. otherwise, DeserializationException will be thrown to interrupt the serialization.
  * @tparam T scala object type
  */
trait JsonRefinerBuilder[T] extends oharastream.ohara.common.pattern.Builder[JsonRefiner[T]] {
  /**
    * set the core format used to convert the refined json to scala object
    * @param format akka json format
    * @return this refiner builder
    */
  def format(format: RootJsonFormat[T]): JsonRefinerBuilder[T]

  /**
    * config this refiner according to setting definitions
    *
    * @param definitions setting definitions
    * @return this refiner
    */
  def definitions(definitions: Seq[SettingDef]): JsonRefinerBuilder[T] = {
    definitions.foreach(definition)
    this
  }

  /**
    * config this refiner according to setting definition
    *
    * @param definition setting definition
    * @return this refiner
    */
  def definition(definition: SettingDef): JsonRefinerBuilder[T] = {
    // the internal is not exposed to user so we skip it.
    // remove the value if the field is readonly
    if (definition.permission() == Permission.READ_ONLY) ignoreKeys(Set(definition.key()))
    definition.regex().ifPresent(regex => stringRestriction(definition.key(), regex))
    if (!definition.internal()) {
      if (definition.necessary() == Necessary.REQUIRED) requireKeys(Set(definition.key()))
    }

    definition.valueType() match {
      case Type.BOOLEAN =>
        if (definition.hasDefault)
          nullToBoolean(definition.key(), definition.defaultBoolean)
        if (!definition.internal()) requireJsonType[JsBoolean](definition.key(), _ => ())
      case Type.POSITIVE_SHORT =>
        if (definition.hasDefault)
          nullToShort(definition.key(), definition.defaultShort)
        if (!definition.internal()) requireNumberType(key = definition.key(), min = 1, max = Short.MaxValue)
      case Type.SHORT =>
        if (definition.hasDefault)
          nullToShort(definition.key(), definition.defaultShort)
        if (!definition.internal())
          requireNumberType(key = definition.key(), min = Short.MinValue, max = Short.MaxValue)
      case Type.POSITIVE_INT =>
        if (definition.hasDefault) nullToInt(definition.key(), definition.defaultInt)
        if (!definition.internal()) requireNumberType(key = definition.key(), min = 1, max = Int.MaxValue)
      case Type.INT =>
        if (definition.hasDefault) nullToInt(definition.key(), definition.defaultInt)
        if (!definition.internal()) requireNumberType(key = definition.key(), min = Int.MinValue, max = Int.MaxValue)
      case Type.POSITIVE_LONG =>
        if (definition.hasDefault) nullToLong(definition.key(), definition.defaultLong)
        if (!definition.internal()) requireNumberType(key = definition.key(), min = 1, max = Long.MaxValue)
      case Type.LONG =>
        if (definition.hasDefault) nullToLong(definition.key(), definition.defaultLong)
        if (!definition.internal()) requireNumberType(key = definition.key(), min = Long.MinValue, max = Long.MaxValue)
      case Type.POSITIVE_DOUBLE =>
        if (definition.hasDefault)
          nullToDouble(definition.key(), definition.defaultDouble)
        if (!definition.internal()) requireNumberType(key = definition.key(), min = 1, max = Double.MaxValue)
      case Type.DOUBLE =>
        if (definition.hasDefault)
          nullToDouble(definition.key(), definition.defaultDouble)
        if (!definition.internal())
          requireNumberType(key = definition.key(), min = Double.MinValue, max = Double.MaxValue)
      case Type.ARRAY =>
        // we don't allow to set default value to array type.
        // see SettingDef
        if (definition.hasDefault)
          throw new IllegalArgumentException(
            "the default value to array type is not allowed. default:" + definition.defaultValue
          )
        if (!definition.internal()) {
          nullToJsValue(definition.key(), () => JsArray.empty)
          definition.necessary() match {
            case SettingDef.Necessary.REQUIRED =>
              requireJsonType[JsArray](
                definition.key(),
                (array: JsArray) =>
                  if (array.elements.isEmpty)
                    throw DeserializationException(
                      s"empty array is illegal to ${definition.key()}",
                      fieldNames = List(definition.key())
                    )
              )
            case _ =>
              requireJsonType[JsArray](definition.key(), _ => ())
          }
          // check deny list
          requireJsonType[JsArray](
            definition.key(),
            (array: JsArray) => {
              array.elements.foreach {
                case JsString(s) =>
                  if (definition.denyList().asScala.contains(s))
                    throw DeserializationException(s"the $s is protected word", fieldNames = List(definition.key()))
                case _ => // we only check the string value
              }
            }
          )
        }
      case Type.DURATION =>
        if (definition.hasDefault)
          // we convert the time based on milliseconds
          nullToString(
            definition.key(),
            Duration(definition.defaultDuration.toMillis, TimeUnit.MILLISECONDS).toString()
          )
        if (!definition.internal()) requireType[Duration](definition.key(), _ => ())
      case Type.REMOTE_PORT =>
        if (definition.hasDefault)
          nullToInt(definition.key(), definition.defaultInt())
        if (!definition.internal()) requireConnectionPort(definition.key())
      case Type.BINDING_PORT =>
        if (definition.hasDefault)
          nullToInt(definition.key(), definition.defaultInt)
        else if (definition.necessary() == Necessary.RANDOM_DEFAULT)
          nullToJsValue(definition.key(), () => JsNumber(CommonUtils.availablePort()))
        if (!definition.internal()) requireConnectionPort(definition.key())
      case Type.OBJECT_KEY =>
        // we don't allow to set default value to array type.
        // see SettingDef
        if (definition.hasDefault)
          throw new IllegalArgumentException(
            "the default value to object key is not allowed. default:" + definition.defaultValue()
          )
        if (!definition.internal()) {
          // convert the value to complete Object key
          valueConverter(definition.key(), v => OBJECT_KEY_FORMAT.write(OBJECT_KEY_FORMAT.read(v)))
          requireType[ObjectKey](definition.key(), _ => ())
        }
      case Type.OBJECT_KEYS =>
        // we don't allow to set default value to array type.
        // see SettingDef
        if (definition.hasDefault)
          throw new IllegalArgumentException(
            "the default value to array type is not allowed. default:" + definition.defaultValue()
          )
        if (!definition.internal()) {
          nullToJsValue(definition.key(), () => JsArray.empty)
          // convert the value to complete Object key
          valueConverter(
            definition.key(), {
              case JsArray(es) => JsArray(es.map(v => OBJECT_KEY_FORMAT.write(OBJECT_KEY_FORMAT.read(v))))
              case v: JsValue  => OBJECT_KEY_FORMAT.write(OBJECT_KEY_FORMAT.read(v))
            }
          )
          definition.necessary() match {
            case SettingDef.Necessary.REQUIRED =>
              requireType[Seq[ObjectKey]](
                definition.key(),
                (keys: Seq[ObjectKey]) =>
                  if (keys.isEmpty)
                    throw DeserializationException(s"empty keys is illegal", fieldNames = List(definition.key()))
              )
            case _ =>
              requireType[Seq[ObjectKey]](definition.key(), _ => ())
          }
        }
      case Type.TAGS =>
        // we don't allow to set default value to array type.
        // see SettingDef
        if (definition.hasDefault)
          throw new IllegalArgumentException(
            "the default value to array type is not allowed. default:" + definition.defaultValue()
          )
        if (!definition.internal()) {
          nullToJsValue(definition.key(), () => JsObject.empty)
          definition.necessary() match {
            case SettingDef.Necessary.REQUIRED =>
              requireJsonType[JsObject](
                definition.key(),
                (tags: JsObject) =>
                  if (tags.fields.isEmpty)
                    throw DeserializationException(s"empty tags is illegal", fieldNames = List(definition.key()))
              )
            case _ =>
              requireJsonType[JsObject](definition.key(), _ => ())
          }
        }
      case Type.TABLE =>
        // we don't allow to set default value to array type.
        // see SettingDef
        if (definition.hasDefault)
          throw new IllegalArgumentException(
            "the default value to array type is not allowed. default:" + definition.defaultValue()
          )
        if (!definition.internal()) {
          nullToJsValue(definition.key(), () => JsArray.empty)
          definition.necessary() match {
            case SettingDef.Necessary.REQUIRED =>
              requireType[PropGroup](
                definition.key(),
                (table: PropGroup) =>
                  if (table.raw().isEmpty)
                    throw DeserializationException(s"empty table is illegal", fieldNames = List(definition.key()))
              )
            case _ =>
              requireType[PropGroup](definition.key(), _ => ())
          }
        }
      case Type.STRING =>
        if (definition.hasDefault) nullToString(definition.key(), definition.defaultString)
        else if (definition.necessary() == Necessary.RANDOM_DEFAULT)
          nullToJsValue(
            definition.key(),
            () =>
              JsString(
                definition.prefix().orElse("") + CommonUtils.randomString(SettingDef.STRING_LENGTH_LIMIT)
              )
          )
        if (!definition.internal()) requireJsonType[JsString](definition.key(), _ => ())
      case _ @(Type.CLASS | Type.PASSWORD | Type.JDBC_TABLE) =>
        if (definition.hasDefault) nullToString(definition.key(), definition.defaultString)
        if (!definition.internal()) requireJsonType[JsString](definition.key(), _ => ())
    }
    this
  }

  //-------------------------[null to default]-------------------------//

  def nullToBoolean(key: String, value: Boolean): JsonRefinerBuilder[T] = nullToJsValue(key, () => JsBoolean(value))

  def nullToString(key: String, defaultValue: String): JsonRefinerBuilder[T] =
    nullToJsValue(key, () => JsString(defaultValue))

  def nullToString(key: String, defaultValue: () => String): JsonRefinerBuilder[T] =
    nullToJsValue(key, () => JsString(defaultValue()))

  def nullToEmptyArray(key: String): JsonRefinerBuilder[T] = nullToJsValue(key, () => JsArray.empty)

  def nullToEmptyObject(key: String): JsonRefinerBuilder[T] = nullToJsValue(key, () => JsObject.empty)

  def nullToShort(key: String, value: Short): JsonRefinerBuilder[T] = nullToJsValue(key, () => JsNumber(value))

  def nullToInt(key: String, value: Int): JsonRefinerBuilder[T] = nullToJsValue(key, () => JsNumber(value))

  def nullToLong(key: String, value: Long): JsonRefinerBuilder[T] = nullToJsValue(key, () => JsNumber(value))

  def nullToDouble(key: String, value: Double): JsonRefinerBuilder[T] = nullToJsValue(key, () => JsNumber(value))

  /**
    * auto-fill the value of key by another key's value. For example
    * defaultToAnother(Map("k0", "k1"))
    * input:
    * {
    * "k1": "abc"
    * }
    * output:
    * {
    * "k0": "abc",
    * "k1": "abc"
    * }
    *
    * This method is useful in working with deprecated key. For example, you plant to change the key from "k0" to "k1".
    * To avoid breaking the APIs, you can refine the parser with this method - nullToAnotherValueOfKey("k1", "k0") -
    * the json parser will seek the "k1" first and then find the "k0" if "k1" does not exist.
    *
    * Noted: it does nothing if both key and another key are nonexistent
    *
    * @param key        the fist key mapped to input json that it can be ignored or null.
    * @param anotherKey the second key mapped to input json that it must exists!!!
    * @return this refiner
    */
  def nullToAnotherValueOfKey(key: String, anotherKey: String): JsonRefinerBuilder[T]

  //-------------------------[more checks]-------------------------//

  /**
    * ignore and remove the keys from following verification.
    * This is useful when you want to remove user-defined fields from input.
    * @param keys to remove
    * @return this builder
    */
  def ignoreKeys(keys: Set[String]): JsonRefinerBuilder[T] = {
    keys.foreach(key => valueConverter(key, _ => JsNull))
    this
  }

  /**
    * require the key for input json. It produces exception if input json is lack of key.
    *
    * @param keys required key
    * @return this refiner
    */
  def requireKeys(keys: Set[String]): JsonRefinerBuilder[T] = keysChecker { inputKeys =>
    val diff = keys.diff(inputKeys)
    if (diff.nonEmpty)
      throw DeserializationException(
        s"$diff are required!!!",
        fieldNames = diff.toList
      )
  }

  /**
    * check whether target port is legal to connect. The legal range is (0, 65535].
    *
    * @param key key
    * @return this refiner
    */
  def requireConnectionPort(key: String): JsonRefinerBuilder[T] = requireNumberType(key = key, min = 1, max = 65535)

  /**
    * require the number type and check the legal number.
    *
    * @param key key
    * @param min the min value (included). if input value is bigger than min, DeserializationException is thrown
    * @param max the max value (included). if input value is smaller than max, DeserializationException is thrown
    * @return this refiner
    */
  def requireNumberType(key: String, min: Long, max: Long): JsonRefinerBuilder[T] = requireJsonType[JsNumber](
    key,
    (jsNumber: JsNumber) => {
      val number = jsNumber.value.toLong
      if (number < min || number > max)
        throw DeserializationException(s"$key: the number must be [$min, $max], actual:$number")
    }
  )

  /**
    * require the number type and check the legal number.
    *
    * @param key key
    * @param min the min value (included). if input value is bigger than min, DeserializationException is thrown
    * @param max the max value (included). if input value is smaller than max, DeserializationException is thrown
    * @return this refiner
    */
  private[this] def requireNumberType(key: String, min: Double, max: Double): JsonRefinerBuilder[T] =
    requireJsonType[JsNumber](
      key,
      (jsNumber: JsNumber) => {
        val number = jsNumber.value.toDouble
        if (number < min || number > max)
          throw DeserializationException(s"$key: the number must be [$min, $max], actual:$number")
      }
    )

  /**
    * check the value of existent key. the value type MUST match the expected type. otherwise, the DeserializationException is thrown.
    *
    * @param key     key
    * @param checker checker
    * @tparam Json expected value type
    * @return this refiner
    */
  private[this] def requireJsonType[Json <: JsValue: ClassTag](
    key: String,
    checker: Json => Unit
  ): JsonRefinerBuilder[T] =
    valuesChecker(
      Set(CommonUtils.requireNonEmpty(key)),
      vs => {
        val json = vs.getOrElse(key, JsNull)
        if (!classTag[Json].runtimeClass.isInstance(json))
          throw DeserializationException(
            s"""the $key must be ${classTag[Json].runtimeClass.getSimpleName} type, but actual type is \"${json.getClass.getSimpleName}\"""",
            fieldNames = List(key)
          )
        checker(json.asInstanceOf[Json])
      }
    )

  /**
    * check the value of existent key. the value type MUST match the expected type. otherwise, the DeserializationException is thrown.
    *
    * @param key     key
    * @param checker checker
    * @return this refiner
    */
  private[this] def requireType[C](key: String, checker: C => Unit)(
    implicit format: RootJsonFormat[C]
  ): JsonRefinerBuilder[T] =
    valuesChecker(
      Set(key),
      vs =>
        vs.get(key) match {
          case Some(v) => checker(format.read(v))
          case None    => // nothing
        }
    )

  /**
    * throw exception if the specific key of input json is associated to empty array.
    *
    * @param key key
    * @return this refiner
    */
  def rejectEmptyArray(key: String): JsonRefinerBuilder[T] =
    requireJsonType[JsArray](
      key,
      (array: JsArray) =>
        if (array.elements.isEmpty)
          throw DeserializationException(s"""$key cannot be an empty array!!!""")
    )

  /**
    * This method includes following rules.
    * 1) the value type must be array
    * 2) the elements in value must NOT exist in denyList.
    * @param key field key
    * @param denyList illegal word to this field
    * @return this refiner
    */
  def rejectKeywordsFromArray(key: String, denyList: Set[String]): JsonRefinerBuilder[T] =
    requireJsonType[JsArray](
      key,
      jsArray => {
        val input   = jsArray.elements.filter(_.isInstanceOf[JsString]).map(_.convertTo[String])
        val illegal = denyList.filter(input.contains)
        if (illegal.nonEmpty)
          throw DeserializationException(s"$illegal are illegal for key: $key", fieldNames = List(key))
      }
    )

  def stringRestriction(key: String, regex: String): JsonRefinerBuilder[T] = valuesChecker(
    Set(key),
    vs =>
      vs.get(key) match {
        case Some(v) =>
          val s = v match {
            case JsString(s) => s
            case _           => v.toString
          }
          if (!s.matches(regex))
            throw DeserializationException(s"""the value \"$s\" is not matched to $regex""", fieldNames = List(key))
        case None => // nothing
      }
  )

  //-------------------------[more conversion]-------------------------//

  /**
    * Set the auto-conversion to key that the string value wiil be converted to number. Hence, the key is able to accept
    * both string type and number type. By contrast, akka json will produce DeserializationException in parsing string to number.
    *
    * @param key key
    * @return this refiner
    */
  def acceptStringToNumber(key: String): JsonRefinerBuilder[T] = valueConverter(
    key, {
      case s: JsString =>
        try JsNumber(s.value)
        catch {
          case e: NumberFormatException =>
            throw DeserializationException(
              s"""the \"${s.value}\" can't be converted to number""",
              e,
              fieldNames = List(key)
            )
        }
      case s: JsValue => s
    }
  )

  /**
    * add your custom check for a set of keys' values.
    * Noted: We don't guarantee the order of each (key, value) pair since this method is intend
    * to do an "aggregation" check, like sum or length check.
    *
    * @param keys    keys
    * @param checker checker
    * @return this refiner
    */
  def valuesChecker(keys: Set[String], checker: Map[String, JsValue] => Unit): JsonRefinerBuilder[T]

  //-------------------------[protected methods]-------------------------//

  protected def keysChecker(checker: Set[String] => Unit): JsonRefinerBuilder[T]

  /**
    * set the default number for input keys. The default value will be added into json request if the associated key is nonexistent.
    *
    * @param key          key
    * @param defaultValue default value
    * @return this refiner
    */
  protected def nullToJsValue(key: String, defaultValue: () => JsValue): JsonRefinerBuilder[T]

  protected def valueConverter(key: String, converter: JsValue => JsValue): JsonRefinerBuilder[T]
}

object JsonRefinerBuilder {
  def apply[T]: JsonRefinerBuilder[T] = new JsonRefinerBuilder[T] {
    private[this] var format: RootJsonFormat[T]                                      = _
    private[this] var valueConverters: Map[String, JsValue => JsValue]               = Map.empty
    private[this] var keyCheckers: Seq[Set[String] => Unit]                          = Seq.empty
    private[this] var valuesCheckers: Map[Set[String], Map[String, JsValue] => Unit] = Map.empty
    private[this] var nullToJsValue: Map[String, () => JsValue]                      = Map.empty
    private[this] var nullToAnotherValueOfKey: Map[String, String]                   = Map.empty

    override def format(format: RootJsonFormat[T]): JsonRefinerBuilder[T] = {
      this.format = Objects.requireNonNull(format)
      this
    }
    override protected def keysChecker(checker: Set[String] => Unit): JsonRefinerBuilder[T] = {
      this.keyCheckers = this.keyCheckers ++ Seq(checker)
      this
    }

    override def valuesChecker(
      keys: Set[String],
      checkers: Map[String, JsValue] => Unit
    ): JsonRefinerBuilder[T] = {
      /**
        * compose the new checker with older one.
        */
      val composedChecker = valuesCheckers
        .get(keys)
        .map(
          origin =>
            (fields: Map[String, JsValue]) => {
              origin(fields)
              checkers(fields)
            }
        )
        .getOrElse(checkers)
      this.valuesCheckers = this.valuesCheckers + (keys -> Objects.requireNonNull(composedChecker))
      this
    }

    override protected def valueConverter(key: String, converter: JsValue => JsValue): JsonRefinerBuilder[T] = {
      /**
        * compose the new checker with older one.
        */
      val composedChecker = valueConverters
        .get(key)
        .map(
          origin =>
            (value: JsValue) => {
              converter(origin(value))
            }
        )
        .getOrElse(converter)
      this.valueConverters = this.valueConverters + (key -> Objects.requireNonNull(composedChecker))
      this
    }

    override def nullToAnotherValueOfKey(key: String, anotherKey: String): JsonRefinerBuilder[T] = {
      if (nullToAnotherValueOfKey.contains(CommonUtils.requireNonEmpty(key)))
        throw new IllegalArgumentException(s"""the \"$key\" has been associated to another key:\"$anotherKey\"""")
      this.nullToAnotherValueOfKey = this.nullToAnotherValueOfKey + (key -> CommonUtils.requireNonEmpty(anotherKey))
      this
    }

    override protected def nullToJsValue(key: String, defaultValue: () => JsValue): JsonRefinerBuilder[T] = {
      if (nullToJsValue.contains(CommonUtils.requireNonEmpty(key)))
        throw new IllegalArgumentException(
          s"""the \"$key\" have been associated to default value:${nullToJsValue(key)}"""
        )
      this.nullToJsValue = this.nullToJsValue + (key -> Objects.requireNonNull(defaultValue))
      this
    }

    override def build: JsonRefiner[T] = {
      Objects.requireNonNull(format)
      nullToJsValue.keys.foreach(CommonUtils.requireNonEmpty)
      valueConverters.keys.foreach(CommonUtils.requireNonEmpty)
      nullToAnotherValueOfKey.keys.foreach(CommonUtils.requireNonEmpty)
      nullToAnotherValueOfKey.values.foreach(CommonUtils.requireNonEmpty)
      new JsonRefiner[T] {
        override def toBuilder: JsonRefinerBuilder[T] =
          JsonRefinerBuilder[T]
            .format(this)

        private[this] def checkGlobalCondition(key: String, value: JsValue): Unit = {
          def checkEmptyString(k: String, s: JsString): Unit =
            if (s.value.isEmpty)
              throw DeserializationException(s"""the value of \"$k\" can't be empty string!!!""", fieldNames = List(k))

          def checkJsValueForEmptyString(k: String, v: JsValue): Unit = v match {
            case s: JsString => checkEmptyString(k, s)
            case s: JsArray  => s.elements.foreach(v => checkJsValueForEmptyString(k, v))
            case s: JsObject => s.fields.foreach(pair => checkJsValueForEmptyString(pair._1, pair._2))
            case _           => // nothing
          }

          // 1) check empty string
          checkJsValueForEmptyString(key, value)
        }

        /**
          * remove the null field since we all hate null :)
          * @param json json object
          * @return fields without null
          */
        private[this] def noNull(json: JsValue): Map[String, JsValue] = json.asJsObject.fields.filter {
          case (_, value) =>
            value match {
              case JsNull => false
              case _      => true
            }
        }

        override def read(json: JsValue): T = json match {
          // we refine only the complicated json object
          case _: JsObject =>
            var fields = noNull(json)

            // 1) convert the value to another type
            fields = (fields ++ valueConverters
              .filter {
                case (key, _) => fields.contains(key)
              }
              .map {
                case (key, converter) => key -> converter(fields(key))
              }).filter {
              case (_, jsValue) =>
                jsValue match {
                  case JsNull => false
                  case _      => true
                }
            }

            // 2) convert the null to the value of another key
            fields = fields ++ nullToAnotherValueOfKey
              .filterNot(pair => fields.contains(pair._1))
              .filter(pair => fields.contains(pair._2))
              .map {
                case (key, anotherKye) => key -> fields(anotherKye)
              }

            // 3) convert the null to default value
            fields = fields ++ nullToJsValue.map {
              case (key, defaultValue) =>
                key -> fields.getOrElse(key, defaultValue())
            }

            // 4) check the keys existence
            // this check is excluded from step.4 since the check is exposed publicly, And it does care for key-value only.
            keyCheckers.foreach(_(fields.keySet))

            // 5) check the fields
            fields = check(fields)

            format.read(JsObject(fields))
          case _ => format.read(json)
        }
        override def write(obj: T): JsValue = JsObject(noNull(format.write(obj)))

        override def check(fields: Map[String, JsValue]): Map[String, JsValue] = {
          // 1) check global condition
          fields.foreach {
            case (k, v) => checkGlobalCondition(k, v)
          }

          // 2) custom checks
          valuesCheckers.foreach {
            case (keys, checker) =>
              // we don't run the check if the keys are not matched
              val matchedFields = fields.filter(e => keys.contains(e._1))
              if (matchedFields.size == keys.size) checker(matchedFields)
          }

          fields
        }
      }
    }
  }
}
