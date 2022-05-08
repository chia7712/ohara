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

package oharastream.ohara.common.setting;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.*;
import oharastream.ohara.common.data.Serializer;
import oharastream.ohara.common.exception.ConfigException;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.rule.OharaTest;
import oharastream.ohara.common.util.CommonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSettingDef extends OharaTest {

  @Test
  public void nullKey() {
    Assertions.assertThrows(NullPointerException.class, () -> SettingDef.builder().key(null));
  }

  @Test
  public void emptyKey() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SettingDef.builder().key(""));
  }

  @Test
  public void nullDefaultWithString() {
    Assertions.assertThrows(
        NullPointerException.class, () -> SettingDef.builder().optional((String) null));
  }

  @Test
  public void nullDefaultWithDuration() {
    Assertions.assertThrows(
        NullPointerException.class, () -> SettingDef.builder().optional((Duration) null));
  }

  @Test
  public void nullDocumentation() {
    Assertions.assertThrows(
        NullPointerException.class, () -> SettingDef.builder().documentation(null));
  }

  @Test
  public void emptyDocumentation() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SettingDef.builder().documentation(""));
  }

  @Test
  public void nullReference() {
    Assertions.assertThrows(NullPointerException.class, () -> SettingDef.builder().reference(null));
  }

  @Test
  public void nullGroup() {
    Assertions.assertThrows(NullPointerException.class, () -> SettingDef.builder().group(null));
  }

  @Test
  public void emptyGroup() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SettingDef.builder().group(""));
  }

  @Test
  public void nullDisplay() {
    Assertions.assertThrows(
        NullPointerException.class, () -> SettingDef.builder().displayName(null));
  }

  @Test
  public void emptyDisplay() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SettingDef.builder().displayName(""));
  }

  @Test
  public void testOnlyKey() {
    String key = CommonUtils.randomString(5);
    SettingDef def = SettingDef.builder().key(key).build();
    Assertions.assertEquals(key, def.key());
    Assertions.assertNotNull(def.displayName());
    Assertions.assertNotNull(def.documentation());
    Assertions.assertNotNull(def.valueType());
    Assertions.assertNotNull(def.group());
    Assertions.assertNotNull(def.reference());
    // yep. the default value should be null
    Assertions.assertFalse(def.hasDefault());
  }

  @Test
  public void testGetterWithEditableAndDefaultValue() {
    String key = CommonUtils.randomString(5);
    String displayName = CommonUtils.randomString(5);
    String group = CommonUtils.randomString(5);
    SettingDef.Reference reference = SettingDef.Reference.WORKER;
    int orderInGroup = 100;
    String valueDefault = CommonUtils.randomString(5);
    String documentation = CommonUtils.randomString(5);
    SettingDef def =
        SettingDef.builder()
            .key(key)
            .displayName(displayName)
            .group(group)
            .reference(reference)
            .orderInGroup(orderInGroup)
            .optional(valueDefault)
            .documentation(documentation)
            .build();

    Assertions.assertEquals(key, def.key());
    Assertions.assertEquals(SettingDef.Type.STRING, def.valueType());
    Assertions.assertEquals(displayName, def.displayName());
    Assertions.assertEquals(group, def.group());
    Assertions.assertEquals(reference, def.reference());
    Assertions.assertEquals(orderInGroup, def.orderInGroup());
    Assertions.assertEquals(valueDefault, def.defaultString());
    Assertions.assertEquals(documentation, def.documentation());
    Assertions.assertEquals(def.necessary(), SettingDef.Necessary.OPTIONAL);
    Assertions.assertFalse(def.internal());
  }

  @Test
  public void testGetterWithoutEditableAndDefaultValue() {
    String key = CommonUtils.randomString(5);
    SettingDef.Type type = SettingDef.Type.TABLE;
    String displayName = CommonUtils.randomString(5);
    String group = CommonUtils.randomString(5);
    SettingDef.Reference reference = SettingDef.Reference.WORKER;
    int orderInGroup = 100;
    String documentation = CommonUtils.randomString(5);
    SettingDef def =
        SettingDef.builder()
            .key(key)
            .required(type)
            .displayName(displayName)
            .group(group)
            .reference(reference)
            .orderInGroup(orderInGroup)
            .documentation(documentation)
            .permission(SettingDef.Permission.READ_ONLY)
            .internal()
            .build();

    Assertions.assertEquals(key, def.key());
    Assertions.assertEquals(type, def.valueType());
    Assertions.assertEquals(displayName, def.displayName());
    Assertions.assertEquals(group, def.group());
    Assertions.assertEquals(reference, def.reference());
    Assertions.assertEquals(orderInGroup, def.orderInGroup());
    Assertions.assertFalse(def.hasDefault());
    Assertions.assertEquals(documentation, def.documentation());
    Assertions.assertEquals(def.necessary(), SettingDef.Necessary.REQUIRED);
    Assertions.assertTrue(def.internal());
  }

  @Test
  public void testTableChecker() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(
                Arrays.asList(
                    TableColumn.builder()
                        .name("a")
                        .recommendedValues(new HashSet<>(Arrays.asList("a0", "a1")))
                        .build(),
                    TableColumn.builder().name("b").build()))
            .build();
    // there is default value so null is ok
    settingDef.checker().accept(null);
    // illegal format
    Assertions.assertThrows(ConfigException.class, () -> settingDef.checker().accept(123));
    // illegal format
    Assertions.assertThrows(ConfigException.class, () -> settingDef.checker().accept(List.of()));
    // neglect column "b"
    Assertions.assertThrows(
        ConfigException.class, () -> settingDef.checker().accept(List.of(Map.of("a", "c"))));

    // too many items
    Map<String, String> goodMap = Map.of("a", "a0", "b", "c");
    settingDef.checker().accept(PropGroup.of(List.of(goodMap)).toJsonString());

    Map<String, String> illegalColumnMap = new HashMap<>(goodMap);
    illegalColumnMap.put("dddd", "fff");
    Assertions.assertThrows(
        ConfigException.class,
        () -> settingDef.checker().accept(PropGroup.of(List.of(illegalColumnMap)).toJsonString()));
  }

  @Test
  public void testDurationChecker() {
    SettingDef settingDef =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.DURATION)
            .build();
    Assertions.assertThrows(ConfigException.class, () -> settingDef.checker().accept(null));
    Assertions.assertThrows(ConfigException.class, () -> settingDef.checker().accept(123));
    Assertions.assertThrows(ConfigException.class, () -> settingDef.checker().accept(List.of()));
    settingDef.checker().accept(Duration.ofHours(3).toString());
    settingDef.checker().accept("10 MILLISECONDS");
    settingDef.checker().accept("10 SECONDS");
  }

  @Test
  public void testSetDisplayName() {
    String displayName = CommonUtils.randomString();
    SettingDef settingDef =
        SettingDef.builder()
            .displayName(displayName)
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.STRING)
            .build();
    Assertions.assertEquals(displayName, settingDef.displayName());
  }

  @Test
  public void testPortType() {
    SettingDef s =
        SettingDef.builder().required(SettingDef.Type.REMOTE_PORT).key("port.key").build();
    // pass
    s.checker().accept(100);
    Assertions.assertThrows(ConfigException.class, () -> s.checker().accept(-1));
    Assertions.assertThrows(ConfigException.class, () -> s.checker().accept(0));
    Assertions.assertThrows(ConfigException.class, () -> s.checker().accept(100000000));
  }

  @Test
  public void testTagsType() {
    SettingDef s = SettingDef.builder().required(SettingDef.Type.TAGS).key("tags.key").build();
    // pass
    s.checker().accept("{\"a\": \"b\"}");
    s.checker().accept("{\"123\":456}");
    s.checker().accept(List.of());
    // not a jsonObject
    Assertions.assertThrows(
        ConfigException.class, () -> s.checker().accept(CommonUtils.randomString()));
    Assertions.assertThrows(ConfigException.class, () -> s.checker().accept("{abc}"));
    Assertions.assertThrows(ConfigException.class, () -> s.checker().accept("{\"123\"}"));
  }

  @Test
  public void testSerialization() {
    SettingDef setting =
        SettingDef.builder().required(SettingDef.Type.TAGS).key("tags.key").build();
    SettingDef copy = (SettingDef) Serializer.OBJECT.from(Serializer.OBJECT.to(setting));
    Assertions.assertEquals(setting, copy);
  }

  @Test
  public void testTopicKeysType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.OBJECT_KEYS)
            .build();
    // pass
    def.checker()
        .accept(
            JsonUtils.toString(
                Set.of(TopicKey.of(CommonUtils.randomString(), CommonUtils.randomString()))));
    // empty array is illegal
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept("[]"));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept("{}"));
    Assertions.assertThrows(
        ConfigException.class, () -> def.checker().accept(CommonUtils.randomString()));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(100000000));
  }

  @Test
  public void testDuration() {
    Duration duration = Duration.ofHours(10);
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).optional(duration).build();
    Assertions.assertEquals(def.defaultDuration(), duration);
    Assertions.assertTrue(
        def.toJsonString()
            .contains("\"defaultValue\":" + "\"" + duration.toMillis() + " milliseconds\""));
  }

  @Test
  public void testRejectNullValue() {
    Assertions.assertThrows(
        ConfigException.class,
        () -> SettingDef.builder().key(CommonUtils.randomString()).build().checker().accept(null));
  }

  @Test
  public void testOptionNullValue() {
    // pass
    SettingDef.builder()
        .key(CommonUtils.randomString())
        .optional(SettingDef.Type.STRING)
        .build()
        .checker()
        .accept(null);
  }

  @Test
  public void testOptionNullValueWithDefault() {
    // pass
    SettingDef.builder()
        .key(CommonUtils.randomString())
        .optional("abc")
        .build()
        .checker()
        .accept(null);
  }

  @Test
  public void testBooleanType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.BOOLEAN)
            .build();
    // only accept "true" or "false"
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept("aaa"));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(123));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(null));
    def.checker().accept(false);
    def.checker().accept("true");
    // case in-sensitive
    def.checker().accept("FaLse");

    // optional definition
    SettingDef defOption =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(SettingDef.Type.BOOLEAN)
            .build();
    // only accept "true" or "false"
    Assertions.assertThrows(ConfigException.class, () -> defOption.checker().accept("aaa"));
    Assertions.assertThrows(ConfigException.class, () -> defOption.checker().accept(123));
    // since we don't have any default value, the "null" will be passed since it is optional
    defOption.checker().accept(null);
    defOption.checker().accept(false);
    defOption.checker().accept("true");
    // case in-sensitive
    defOption.checker().accept("FaLse");
  }

  @Test
  public void testStringType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.STRING)
            .build();

    def.checker().accept("aaa");
    def.checker().accept(111);
  }

  @Test
  public void testStringTypeWithRecommendedValues() {
    Set<String> recommendedValues =
        new HashSet<>(
            Arrays.asList(
                CommonUtils.randomString(),
                CommonUtils.randomString(),
                CommonUtils.randomString()));

    // required with recommended values(default is null)
    SettingDef settingDef1 =
        SettingDef.builder().key(CommonUtils.randomString()).required(recommendedValues).build();
    Assertions.assertEquals(settingDef1.recommendedValues(), recommendedValues);
  }

  @Test
  public void testShortType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.SHORT)
            .build();

    def.checker().accept(111);

    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(""));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(11111111111L));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(2.2));
  }

  @Test
  public void testIntType() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).required(SettingDef.Type.INT).build();

    def.checker().accept(111);

    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(""));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(11111111111L));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(2.2));
  }

  @Test
  public void testLongType() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).required(SettingDef.Type.LONG).build();

    def.checker().accept(111);
    def.checker().accept(11111111111L);

    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(""));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(2.2));
  }

  @Test
  public void testDoubleType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.DOUBLE)
            .build();

    def.checker().accept(111);
    def.checker().accept(11111111111L);
    def.checker().accept(2.2);

    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept("abc"));
    Assertions.assertThrows(ConfigException.class, () -> def.checker().accept(""));
  }

  @Test
  public void testArrayType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.ARRAY)
            .build();
    // pass
    def.checker().accept("[gg]");
    def.checker().accept("[\"aa\", \"bb\"]");
    def.checker().accept("[123]");
    def.checker().accept(Arrays.asList("ab", "cd"));

    // empty array is ok
    def.checker().accept(List.of());
    def.checker().accept("[]");
  }

  @Test
  public void testKafkaArrayType() {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.ARRAY)
            .build();
    // since connector use "xxx,yyy" to use in kafka format
    // we should pass this (these cases are not json array)
    def.checker().accept("abc");
    def.checker().accept(111);
    def.checker().accept("null");
    def.checker().accept("abc,def");
    def.checker().accept("123 , 456");

    // empty string means empty list, it is ok
    def.checker().accept("");
  }

  @Test
  public void testBindingPort() throws IOException {
    SettingDef def =
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .required(SettingDef.Type.BINDING_PORT)
            .build();
    def.checker().accept(CommonUtils.availablePort());

    int port = CommonUtils.availablePort();
    try (ServerSocket server = new ServerSocket(port)) {
      Assertions.assertThrows(
          ConfigException.class, () -> def.checker().accept(server.getLocalPort()));
    }
    def.checker().accept(port);
  }

  @Test
  public void doubleUnderScoreIsIllegal() {
    SettingDef.builder().key("aaa").build();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> SettingDef.builder().key("aaa__").build());
  }

  @Test
  public void nullRecommendedValues() {
    Assertions.assertThrows(
        NullPointerException.class, () -> SettingDef.builder().optional((Set<String>) null));
  }

  @Test
  public void nullDenyList() {
    Assertions.assertThrows(NullPointerException.class, () -> SettingDef.builder().denyList(null));
  }

  @Test
  public void defaultBuild() {
    // all fields should have default value except for key
    SettingDef.builder().key(CommonUtils.randomString()).build();
  }

  @Test
  public void testShortDefault() {
    short defaultValue = 123;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    // jackson convert the number to int or long only
    Assertions.assertEquals(copy.defaultShort(), defaultValue);
  }

  @Test
  public void testIntDefault() {
    int defaultValue = 123;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assertions.assertEquals(copy.defaultInt(), defaultValue);
  }

  @Test
  public void testLongDefault() {
    long defaultValue = Long.MAX_VALUE;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assertions.assertEquals(copy.defaultLong(), defaultValue);
  }

  @Test
  public void testDoubleDefault() {
    double defaultValue = 123;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assertions.assertEquals(copy.defaultDouble(), defaultValue, 0);
  }

  @Test
  public void testStringDefault() {
    String defaultValue = "asd";
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assertions.assertEquals(copy.defaultString(), defaultValue);
  }

  @Test
  public void testDurationDefault() {
    Duration defaultValue = Duration.ofMillis(12345);
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assertions.assertEquals(copy.defaultDuration(), defaultValue);
  }

  @Test
  public void testBooleanDefault() {
    boolean defaultValue = true;
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(defaultValue).build();
    SettingDef copy = SettingDef.ofJson(settingDef.toString());
    Assertions.assertEquals(copy.defaultBoolean(), defaultValue);
  }

  @Test
  public void testEmptyRecommendedValuesTest() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SettingDef.builder().key(CommonUtils.randomString()).optional(Set.of()).build());
  }

  @Test
  public void testRecommendedValues() {
    Set<String> recommendedValues =
        new HashSet<>(
            Arrays.asList(
                CommonUtils.randomString(),
                CommonUtils.randomString(),
                CommonUtils.randomString()));
    SettingDef settingDef =
        SettingDef.builder().key(CommonUtils.randomString()).optional(recommendedValues).build();
    Assertions.assertEquals(settingDef.recommendedValues(), recommendedValues);
    Assertions.assertEquals(settingDef.defaultString(), recommendedValues.iterator().next());
  }

  @Test
  public void testSpaceInKey() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SettingDef.builder().key(" "));
  }

  @Test
  public void testEqualInKey() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SettingDef.builder().key("="));
  }

  @Test
  public void testQuoteInKey() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> SettingDef.builder().key("\""));
  }

  @Test
  public void testDotInKey() {
    SettingDef.builder().key(".");
  }

  @Test
  public void testSlashInKey() {
    SettingDef.builder().key("-");
  }

  @Test
  public void testUnderLineInKey() {
    SettingDef.builder().key("_");
  }

  @Test
  public void testDefaultDuration() {
    Assertions.assertEquals(
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(Duration.ofMillis(10))
            .build()
            .defaultValue()
            .get(),
        "10 milliseconds");

    Assertions.assertEquals(
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optional(Duration.ofMillis(1))
            .build()
            .defaultValue()
            .get(),
        "1 millisecond");
  }

  @Test
  public void testOptionalPort() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).optionalPort(12345).build();
    Assertions.assertEquals(def.valueType(), SettingDef.Type.REMOTE_PORT);
    Assertions.assertEquals(def.defaultPort(), 12345);
  }

  @Test
  public void testOptionalBindingPort() {
    SettingDef def =
        SettingDef.builder().key(CommonUtils.randomString()).optionalBindingPort(12345).build();
    Assertions.assertEquals(def.valueType(), SettingDef.Type.BINDING_PORT);
    Assertions.assertEquals(def.defaultPort(), 12345);
  }

  @Test
  public void testNameStringRegex() {
    Assertions.assertFalse("_".matches(SettingDef.NAME_STRING_REGEX));
    Assertions.assertFalse("-".matches(SettingDef.NAME_STRING_REGEX));
    // upper case is illegal
    Assertions.assertFalse("A".matches(SettingDef.NAME_STRING_REGEX));
    // dot is illegal
    Assertions.assertTrue("a.".matches(SettingDef.NAME_STRING_REGEX));
    // the length limit is 25
    Assertions.assertFalse(CommonUtils.randomString(100).matches(SettingDef.NAME_STRING_REGEX));
  }

  @Test
  public void testGroupStringRegex() {
    Assertions.assertFalse("_".matches(SettingDef.GROUP_STRING_REGEX));
    Assertions.assertFalse("-".matches(SettingDef.GROUP_STRING_REGEX));
    // upper case is illegal
    Assertions.assertFalse("A".matches(SettingDef.GROUP_STRING_REGEX));
    // dot is illegal
    Assertions.assertTrue("a.".matches(SettingDef.GROUP_STRING_REGEX));
    // the length limit is 25
    Assertions.assertFalse(CommonUtils.randomString(100).matches(SettingDef.GROUP_STRING_REGEX));
  }

  @Test
  public void testHostnameRegex() {
    Assertions.assertTrue("aAbB-".matches(SettingDef.HOSTNAME_REGEX));
    // dash is illegal
    Assertions.assertFalse("a_".matches(SettingDef.HOSTNAME_REGEX));
    // dot is legal
    Assertions.assertTrue("a.".matches(SettingDef.HOSTNAME_REGEX));
    // the length limit is 25
    Assertions.assertFalse(CommonUtils.randomString(100).matches(SettingDef.HOSTNAME_REGEX));
  }

  @Test
  public void nullFieldShouldBeRemovedFromJsonString() {
    checkNullField(SettingDef.builder().key(CommonUtils.randomString()).build());
  }

  @Test
  public void nullFieldInConvertingJsonString() {
    checkNullField(
        SettingDef.of(SettingDef.builder().key(CommonUtils.randomString()).build().toJsonString()));
  }

  private static void checkNullField(SettingDef def) {
    Assertions.assertFalse(def.toJsonString().contains(SettingDef.REGEX_KEY));
    Assertions.assertFalse(def.toJsonString().contains(SettingDef.DEFAULT_VALUE_KEY));
    Assertions.assertFalse(def.toJsonString().contains(SettingDef.PREFIX_KEY));
  }

  @Test
  public void testDefaultShortOnBindingPort() {
    Assertions.assertEquals(
        12345,
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optionalBindingPort(12345)
            .build()
            .defaultInt());
  }

  @Test
  public void testDefaultShortOnRemotePort() {
    Assertions.assertEquals(
        12345,
        SettingDef.builder()
            .key(CommonUtils.randomString())
            .optionalRemotePort(12345)
            .build()
            .defaultInt());
  }
}
