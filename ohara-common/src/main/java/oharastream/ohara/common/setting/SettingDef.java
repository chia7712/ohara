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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.Serializable;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import oharastream.ohara.common.annotations.Nullable;
import oharastream.ohara.common.annotations.Optional;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.exception.ConfigException;
import oharastream.ohara.common.json.JsonObject;
import oharastream.ohara.common.json.JsonUtils;
import oharastream.ohara.common.util.CommonUtils;

/**
 * This class is the base class to define configuration for ohara object.
 *
 * <p>SettingDef is stored by Configurator Store now, and the serialization is based on java
 * serializable. Hence, we add this Serializable interface here.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SettingDef implements JsonObject, Serializable {

  public static SettingDef of(String json) {
    return JsonUtils.toObject(json, new TypeReference<SettingDef>() {});
  }

  public static final int STRING_LENGTH_LIMIT = 25;
  public static final String GROUP_STRING_REGEX = "[a-z0-9\\.]{1," + STRING_LENGTH_LIMIT + "}$";
  public static final String NAME_STRING_REGEX = "[a-z0-9\\.]{1," + STRING_LENGTH_LIMIT + "}$";
  public static final String HOSTNAME_REGEX = "[a-zA-Z0-9.\\-]{1," + STRING_LENGTH_LIMIT + "}$";

  private static final long serialVersionUID = 1L;
  // -------------------------------[groups]-------------------------------//
  public static final String COMMON_GROUP = "common";
  // -------------------------------[table]-------------------------------//
  public static final String COLUMN_ORDER_KEY = "order";
  public static final String COLUMN_NAME_KEY = "name";
  public static final String COLUMN_NEW_NAME_KEY = "newName";
  public static final String COLUMN_DATA_TYPE_KEY = "dataType";
  // -------------------------------[reference]-------------------------------//
  public enum Reference {
    BROKER,
    CONNECTOR,
    FILE,
    NONE,
    NODE,
    OBJECT,
    PIPELINE,
    SHABONDI,
    STREAM,
    TOPIC,
    VOLUME,
    WORKER,
    ZOOKEEPER,
  }

  // -------------------------------[value permission]-------------------------------//
  public enum Permission {
    READ_ONLY,
    CREATE_ONLY,
    EDITABLE
  }

  // -------------------------------[value required]-------------------------------//
  public enum Necessary {
    REQUIRED,
    OPTIONAL,
    RANDOM_DEFAULT
  }

  // -------------------------------[Check rule]-------------------------------//
  public enum CheckRule {
    NONE,
    PERMISSIVE,
    ENFORCING
  }

  // -------------------------------[type]-------------------------------//
  public enum Type {
    BOOLEAN,
    STRING,
    POSITIVE_SHORT,
    SHORT,
    POSITIVE_INT,
    INT,
    POSITIVE_LONG,
    LONG,
    POSITIVE_DOUBLE,
    DOUBLE,
    /**
     * ARRAY is a better naming than LIST as LIST has another meaning to ohara manager.
     *
     * <p>[ "a1", "a2", "a3" ]
     */
    ARRAY,
    CLASS,
    PASSWORD,
    /**
     * JDBC_TABLE is a specific string type used to reminder Ohara Manager that this field requires
     * a **magic** button to show available tables of remote database via Query APIs. Except for the
     * **magic** in UI, there is no other stuff for this JDBC_TYPE since kafka can't verify the
     * input arguments according to other arguments. It means we can't connect to remote database to
     * check the existence of input table.
     */
    JDBC_TABLE,
    TABLE,
    /**
     * The formats accepted are based on the ISO-8601 duration format PnDTnHnMn.nS with days
     * considered to be exactly 24 hours. Please reference to
     * https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-
     */
    DURATION,
    /**
     * The legal range for port is [1, 65535]. Usually you will need this type if you want to
     * connect to a port in remote machine
     */
    REMOTE_PORT,
    /**
     * The legal range for port is [1, 65535]. The difference between REMOTE_PORT and BINDING_PORT
     * is that we will check the availability for the BINDING_PORT.
     */
    BINDING_PORT,
    /** { "group": "g", "name":" n" } */
    OBJECT_KEY,
    /** [ { "group": "g", "name":" n" } ] */
    OBJECT_KEYS,
    /**
     * TAGS is a flexible type accepting a json representation. For example:
     *
     * <p>{ "k0": "v0", "k1": "v1", "k2": ["a0", "b0" ] }
     */
    TAGS,
  }

  // -------------------------------[key]-------------------------------//
  private static final String REFERENCE_KEY = "reference";
  @VisibleForTesting static final String REGEX_KEY = "regex";
  private static final String GROUP_KEY = "group";
  private static final String ORDER_IN_GROUP_KEY = "orderInGroup";
  private static final String DISPLAY_NAME_KEY = "displayName";
  private static final String KEY_KEY = "key";
  private static final String VALUE_TYPE_KEY = "valueType";
  private static final String NECESSARY_KEY = "necessary";
  @VisibleForTesting static final String DEFAULT_VALUE_KEY = "defaultValue";
  private static final String DOCUMENTATION_KEY = "documentation";
  private static final String INTERNAL_KEY = "internal";
  private static final String TABLE_KEYS_KEY = "tableKeys";
  private static final String PERMISSION_KEY = "permission";
  // exposed to TableColumn
  static final String RECOMMENDED_VALUES_KEY = "recommendedValues";
  private static final String DENY_LIST_KEY = "denyList";
  @VisibleForTesting static final String PREFIX_KEY = "prefix";

  public static SettingDef ofJson(String json) {
    return JsonUtils.toObject(json, new TypeReference<>() {});
  }

  private final String displayName;
  private final String group;
  private final int orderInGroup;
  private final String key;
  private final Type valueType;
  @Nullable private final Object defaultValue;
  @Nullable private final String regex;
  private final Necessary necessary;
  private final String documentation;
  private final Reference reference;
  private final boolean internal;
  private final Permission permission;
  private final List<TableColumn> tableKeys;
  private final Set<String> recommendedValues;
  private final Set<String> denyList;
  /**
   * this is used by RANDOM_DEFAULT and type.String that the prefix will be added to the random
   * string.
   */
  @Nullable private final String prefix;

  @JsonCreator
  private SettingDef(
      @JsonProperty(DISPLAY_NAME_KEY) String displayName,
      @JsonProperty(GROUP_KEY) String group,
      @JsonProperty(ORDER_IN_GROUP_KEY) int orderInGroup,
      @JsonProperty(KEY_KEY) String key,
      @JsonProperty(VALUE_TYPE_KEY) Type valueType,
      @JsonProperty(NECESSARY_KEY) Necessary necessary,
      @Nullable @JsonProperty(DEFAULT_VALUE_KEY) Object defaultValue,
      @JsonProperty(DOCUMENTATION_KEY) String documentation,
      @Nullable @JsonProperty(REFERENCE_KEY) Reference reference,
      @Nullable @JsonProperty(REGEX_KEY) String regex,
      @JsonProperty(INTERNAL_KEY) boolean internal,
      @JsonProperty(PERMISSION_KEY) Permission permission,
      @JsonProperty(TABLE_KEYS_KEY) List<TableColumn> tableKeys,
      @JsonProperty(RECOMMENDED_VALUES_KEY) Set<String> recommendedValues,
      @JsonProperty(DENY_LIST_KEY) Set<String> denyList,
      @Nullable @JsonProperty(PREFIX_KEY) String prefix) {
    this.group = CommonUtils.requireNonEmpty(group);
    this.orderInGroup = orderInGroup;
    this.key = CommonUtils.requireNonEmpty(key);
    if (this.key.contains("__"))
      throw new IllegalArgumentException(
          "the __ is keyword so it is illegal word to definition key");
    this.valueType = Objects.requireNonNull(valueType);
    this.necessary = necessary;
    if ((valueType == Type.SHORT || valueType == Type.POSITIVE_SHORT)
        && defaultValue instanceof Integer) {
      // jackson convert the number to int by default so we have to cast it again.
      this.defaultValue = (short) ((int) defaultValue);
    } else if ((valueType == Type.LONG || valueType == Type.POSITIVE_LONG)
        && defaultValue instanceof Integer) {
      // jackson convert the number to int by default so we have to cast it again.
      this.defaultValue = (long) ((int) defaultValue);
    } else this.defaultValue = defaultValue;
    this.documentation = CommonUtils.requireNonEmpty(documentation);
    this.reference = Objects.requireNonNull(reference);
    this.internal = internal;
    this.permission = permission;
    this.tableKeys = Objects.requireNonNull(tableKeys);
    // It is legal to ignore the display name.
    // However, we all hate null so we set the default value equal to key.
    this.displayName = CommonUtils.isEmpty(displayName) ? this.key : displayName;
    this.recommendedValues = Objects.requireNonNull(recommendedValues);
    this.denyList = Objects.requireNonNull(denyList);
    this.regex = regex;
    this.prefix = prefix;
  }

  /**
   * Generate official checker according to input type.
   *
   * @return checker
   */
  public Consumer<Object> checker() {
    return (Object value) -> {
      // we don't check the optional key with null value
      if (this.necessary != Necessary.REQUIRED && this.defaultValue == null && value == null)
        return;

      // besides the first rule, any other combination of settings should check the value is not
      // null (default or from api)
      final Object trueValue = value == null ? this.defaultValue : value;
      if (trueValue == null) throw new ConfigException("the key [" + this.key + "] is required");

      // each type checking
      switch (valueType) {
        case BOOLEAN:
          if (trueValue instanceof Boolean) return;
          // we implement our logic here since
          // boolean type should only accept two possible values: "true" or "false"
          if (!String.valueOf(trueValue).equalsIgnoreCase("true")
              && !String.valueOf(trueValue).equalsIgnoreCase("false"))
            throw new ConfigException(
                this.key,
                trueValue,
                "The value should equals to case-insensitive string value of 'true' or 'false'");
          break;
        case STRING:
        case PASSWORD:
          try {
            String.valueOf(trueValue);
          } catch (Exception e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case POSITIVE_SHORT:
        case SHORT:
          try {
            int v;
            if (trueValue instanceof Short) v = (short) trueValue;
            else v = Short.parseShort(String.valueOf(trueValue));
            if (valueType == SettingDef.Type.POSITIVE_SHORT && v <= 0)
              throw new ConfigException(
                  this.key, trueValue, "the value must be bigger than zero but actual:" + v);
          } catch (NumberFormatException e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case POSITIVE_INT:
        case INT:
          try {
            int v;
            if (trueValue instanceof Integer) v = (int) trueValue;
            else v = Integer.parseInt(String.valueOf(trueValue));
            if (valueType == SettingDef.Type.POSITIVE_INT && v <= 0)
              throw new ConfigException(
                  this.key, trueValue, "the value must be bigger than zero but actual:" + v);
          } catch (NumberFormatException e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case POSITIVE_LONG:
        case LONG:
          try {
            long v;
            if (trueValue instanceof Long) v = (long) trueValue;
            else v = Long.parseLong(String.valueOf(trueValue));
            if (valueType == SettingDef.Type.POSITIVE_LONG && v <= 0)
              throw new ConfigException(
                  this.key, trueValue, "the value must be bigger than zero but actual:" + v);
          } catch (NumberFormatException e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case POSITIVE_DOUBLE:
        case DOUBLE:
          try {
            double v;
            if (trueValue instanceof Double) v = (double) trueValue;
            else v = Double.parseDouble(String.valueOf(trueValue));
            if (valueType == SettingDef.Type.POSITIVE_DOUBLE && v <= 0)
              throw new ConfigException(
                  this.key, trueValue, "the value must be bigger than zero but actual:" + v);
          } catch (NumberFormatException e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case ARRAY:
          try {
            // Determine whether the value is JSON array or not.
            // Note: we hard-code the "kafka list format" to json array here for checking,
            // but it should be implemented a better way (add a converter interface for example)
            JsonUtils.toObject(String.valueOf(trueValue), new TypeReference<List<String>>() {});
          } catch (Exception e) {
            // TODO refactor this in #2400
            try {
              String.valueOf(value).split(",");
            } catch (Exception ex) {
              throw new ConfigException("value not match the array string, actual: " + value);
            }
          }
          break;
        case CLASS:
          // TODO: implement the class checking
          break;
        case JDBC_TABLE:
          // TODO: implement the jdbc table checking
          break;
        case TABLE:
          try {
            PropGroup propGroup = PropGroup.ofJson(String.valueOf(trueValue));
            if (tableKeys.isEmpty()) return;
            if (propGroup.isEmpty()) throw new IllegalArgumentException("row is empty");
            propGroup
                .raw()
                .forEach(
                    row -> {
                      if (!tableKeys.isEmpty()) {
                        Set<String> expectedColumnNames =
                            tableKeys.stream()
                                .map(TableColumn::name)
                                .collect(Collectors.toUnmodifiableSet());
                        Set<String> actualColumnName = row.keySet();
                        if (!actualColumnName.equals(expectedColumnNames)) {
                          throw new IllegalArgumentException(
                              "expected column names:"
                                  + String.join(",", expectedColumnNames)
                                  + ", actual:"
                                  + String.join(",", actualColumnName));
                        }
                      }
                    });

          } catch (Exception e) {
            throw new ConfigException(this.key, trueValue, "can't be converted to PropGroup type");
          }
          break;
        case DURATION:
          try {
            CommonUtils.toDuration(String.valueOf(trueValue));
          } catch (Exception e) {
            throw new ConfigException(this.key, trueValue, "can't be converted to Duration type");
          }
          break;
        case BINDING_PORT:
        case REMOTE_PORT:
          try {
            int port = Integer.parseInt(String.valueOf(trueValue));
            if (!CommonUtils.isConnectionPort(port))
              throw new ConfigException(
                  "the legal range for port is [1, 65535], but actual port is " + port);
            if (valueType == Type.BINDING_PORT) {
              // tries to bind the port :)
              try (ServerSocket socket = new ServerSocket(port)) {
                if (port != socket.getLocalPort())
                  throw new ConfigException(
                      "the port:" + port + " is not available in host:" + CommonUtils.hostname());
              }
            }
          } catch (Exception e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case OBJECT_KEYS:
          try {
            if (ObjectKey.toObjectKeys(String.valueOf(trueValue)).isEmpty())
              throw new ConfigException("OBJECT_KEYS can't be empty!!!");
          } catch (Exception e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case OBJECT_KEY:
          try {
            // try parse the json string to Connector Key
            ObjectKey.toObjectKey(String.valueOf(trueValue));
          } catch (Exception e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        case TAGS:
          try {
            // Determine whether the value is JSON object or not (We assume the "tags" field is an
            // json object)
            JsonUtils.toObject(String.valueOf(trueValue), new TypeReference<Object>() {});
          } catch (Exception e) {
            throw new ConfigException(this.key, trueValue, e.getMessage());
          }
          break;
        default:
          // do nothing
          break;
      }
    };
  }

  @JsonProperty(PREFIX_KEY)
  public java.util.Optional<String> prefix() {
    return java.util.Optional.ofNullable(prefix);
  }

  @JsonProperty(INTERNAL_KEY)
  public boolean internal() {
    return internal;
  }

  @JsonProperty(PERMISSION_KEY)
  public Permission permission() {
    return permission;
  }

  @JsonProperty(DISPLAY_NAME_KEY)
  public String displayName() {
    return displayName;
  }

  @JsonProperty(GROUP_KEY)
  public String group() {
    return group;
  }

  @JsonProperty(ORDER_IN_GROUP_KEY)
  public int orderInGroup() {
    return orderInGroup;
  }

  @JsonProperty(KEY_KEY)
  public String key() {
    return key;
  }

  @JsonProperty(VALUE_TYPE_KEY)
  public Type valueType() {
    return valueType;
  }

  @JsonProperty(NECESSARY_KEY)
  public Necessary necessary() {
    return necessary;
  }

  /**
   * get and convert the default value to boolean type
   *
   * @return default value in boolean type
   */
  public boolean defaultBoolean() {
    if (valueType == Type.BOOLEAN) return (boolean) Objects.requireNonNull(defaultValue);
    throw new IllegalStateException("expected type: boolean, but actual:" + valueType);
  }

  /**
   * get and convert the default value to short type
   *
   * @return default value in short type
   */
  public short defaultShort() {
    if (valueType == Type.SHORT || valueType == Type.POSITIVE_SHORT)
      return (short) Objects.requireNonNull(defaultValue);
    throw new IllegalStateException("expected type: short, but actual:" + valueType);
  }

  /**
   * get and convert the default value to Port type
   *
   * @return default value in short type
   */
  public int defaultPort() {
    if (valueType == Type.REMOTE_PORT || valueType == Type.BINDING_PORT)
      return (int) Objects.requireNonNull(defaultValue);
    throw new IllegalStateException(
        "expected type: REMOTE_PORT/BINDING_PORT, but actual:" + valueType);
  }

  /**
   * get and convert the default value to int type
   *
   * @return default value in int type
   */
  public int defaultInt() {
    if (valueType == Type.INT
        || valueType == Type.POSITIVE_INT
        || valueType == Type.BINDING_PORT
        || valueType == Type.REMOTE_PORT) return (int) Objects.requireNonNull(defaultValue);
    throw new IllegalStateException("expected type: int, but actual:" + valueType);
  }

  /**
   * get and convert the default value to long type
   *
   * @return default value in long type
   */
  public long defaultLong() {
    if (valueType == Type.LONG || valueType == Type.POSITIVE_LONG)
      return (long) Objects.requireNonNull(defaultValue);
    throw new IllegalStateException("expected type: long, but actual:" + valueType);
  }

  /**
   * get and convert the default value to double type
   *
   * @return default value in double type
   */
  public double defaultDouble() {
    if (valueType == Type.DOUBLE || valueType == Type.POSITIVE_DOUBLE)
      return (double) Objects.requireNonNull(defaultValue);
    throw new IllegalStateException("expected type: double, but actual:" + valueType);
  }

  /**
   * get and convert the default value to String type
   *
   * @return default value in String type
   */
  public String defaultString() {
    switch (valueType) {
      case STRING:
      case CLASS:
      case PASSWORD:
      case JDBC_TABLE:
        return (String) Objects.requireNonNull(defaultValue);
      default:
        throw new IllegalStateException("expected type: String, but actual:" + valueType);
    }
  }

  /**
   * get and convert the default value to Duration type
   *
   * @return default value in Duration type
   */
  public Duration defaultDuration() {
    if (valueType == Type.DURATION)
      return CommonUtils.toDuration((String) Objects.requireNonNull(defaultValue));
    throw new IllegalStateException("expected type: Duration, but actual:" + valueType);
  }

  /** @return true if there is a default value. otherwise, false */
  public boolean hasDefault() {
    return defaultValue != null;
  }

  /**
   * Normally, you should not use this method since it does not convert the value according to type.
   * the main purpose of this method is exposed to jackson to render the json string.
   *
   * @return origin type (object type)
   */
  @JsonProperty(DEFAULT_VALUE_KEY)
  public java.util.Optional<Object> defaultValue() {
    return java.util.Optional.ofNullable(defaultValue);
  }

  @JsonProperty(REGEX_KEY)
  public java.util.Optional<String> regex() {
    return java.util.Optional.ofNullable(regex);
  }

  @JsonProperty(DOCUMENTATION_KEY)
  public String documentation() {
    return documentation;
  }

  @JsonProperty(REFERENCE_KEY)
  public Reference reference() {
    return reference;
  }

  @JsonProperty(TABLE_KEYS_KEY)
  public List<TableColumn> tableKeys() {
    return Collections.unmodifiableList(tableKeys);
  }

  @JsonProperty(RECOMMENDED_VALUES_KEY)
  public Set<String> recommendedValues() {
    return Collections.unmodifiableSet(recommendedValues);
  }

  @JsonProperty(DENY_LIST_KEY)
  public Set<String> denyList() {
    return Collections.unmodifiableSet(denyList);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SettingDef) {
      SettingDef another = (SettingDef) obj;
      return Objects.equals(displayName, another.displayName)
          && Objects.equals(group, another.group)
          && Objects.equals(orderInGroup, another.orderInGroup)
          && Objects.equals(key, another.key)
          && Objects.equals(valueType, another.valueType)
          && Objects.equals(necessary, another.necessary)
          && Objects.equals(defaultValue, another.defaultValue)
          && Objects.equals(documentation, another.documentation)
          && Objects.equals(reference, another.reference)
          && Objects.equals(internal, another.internal)
          && Objects.equals(permission, another.permission)
          && Objects.equals(tableKeys, another.tableKeys)
          && Objects.equals(recommendedValues, another.recommendedValues)
          && Objects.equals(denyList, another.denyList)
          && Objects.equals(prefix, another.prefix);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return toJsonString().hashCode();
  }

  @Override
  public String toString() {
    return toJsonString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements oharastream.ohara.common.pattern.Builder<SettingDef> {
    private String displayName;
    private String group = COMMON_GROUP;
    private int orderInGroup = -1;
    private String key;
    private Type valueType = null;
    private Necessary necessary = null;
    @Nullable private Object defaultValue = null;
    private String documentation = "this is no documentation for this setting";
    private Reference reference = Reference.NONE;
    private boolean internal = false;
    private Permission permission = Permission.EDITABLE;
    private List<TableColumn> tableKeys = List.of();
    private Set<String> recommendedValues = Set.of();
    private Set<String> denyList = Set.of();
    @Nullable private String regex = null;
    @Nullable private String prefix = null;

    private Builder() {}

    @Optional("default value is false")
    public Builder internal() {
      this.internal = true;
      return this;
    }

    @Optional("default value is editable")
    public Builder permission(Permission permission) {
      this.permission = Objects.requireNonNull(permission);
      return this;
    }

    /**
     * noted that the legal chars are digits (0-9), letters (a-z|A-Z), -, and .
     *
     * @param key key
     * @return this builder
     */
    public Builder key(String key) {
      this.key = CommonUtils.requireNonEmpty(key);
      if (!key.matches("[a-zA-Z0-9._\\-]+"))
        throw new IllegalArgumentException("the legal char is [a-zA-Z0-9\\._\\-]+");
      return this;
    }

    /** check and set all related fields at once. */
    private Builder checkAndSet(Type valueType, Necessary necessary, Object defaultValue) {
      if (defaultValue != null) {
        switch (valueType) {
          case ARRAY:
          case TABLE:
          case OBJECT_KEY:
          case OBJECT_KEYS:
          case TAGS:
            throw new IllegalArgumentException("type:" + valueType + " can't have default value");
          default:
            break;
        }
      }
      if (this.valueType != null && this.valueType != valueType)
        throw new IllegalArgumentException(
            "type is defined to " + this.valueType + ", new one:" + valueType);
      this.valueType = Objects.requireNonNull(valueType);
      this.necessary = Objects.requireNonNull(necessary);
      this.defaultValue = defaultValue;
      return this;
    }

    /**
     * set the type. Noted: the following types are filled with default empty. 1) {@link
     * SettingDef.Type#ARRAY} 2) {@link SettingDef.Type#TAGS} 3) {@link SettingDef.Type#TABLE} 4)
     * {@link SettingDef.Type#OBJECT_KEYS}
     *
     * @param valueType value type
     * @return this builder
     */
    public Builder required(Type valueType) {
      return checkAndSet(valueType, Necessary.REQUIRED, null);
    }

    public Builder required(Set<String> recommendedValues) {
      this.recommendedValues = Objects.requireNonNull(recommendedValues);
      return checkAndSet(Type.STRING, Necessary.REQUIRED, null);
    }

    /**
     * set the type and announce the empty/null value is legal
     *
     * @param valueType value type
     * @return this builder
     */
    public Builder optional(Type valueType) {
      return checkAndSet(valueType, Necessary.OPTIONAL, null);
    }

    /**
     * set the value type to TABLE and give a rule to table schema.
     *
     * @param tableKeys key name of table
     * @return this builder
     */
    public Builder optional(List<TableColumn> tableKeys) {
      this.tableKeys = new ArrayList<>(CommonUtils.requireNonEmpty(tableKeys));
      return checkAndSet(Type.TABLE, Necessary.OPTIONAL, null);
    }

    /**
     * set the string type and this definition generate random default value if the value is not
     * defined.
     *
     * @return this builder
     */
    public Builder stringWithRandomDefault() {
      return checkAndSet(Type.STRING, Necessary.RANDOM_DEFAULT, null);
    }

    /**
     * set the string type and this definition generate random default value if the value is not
     * defined.
     *
     * @param prefix the prefix added to random string
     * @return this builder
     */
    public Builder stringWithRandomDefault(String prefix) {
      checkAndSet(Type.STRING, Necessary.RANDOM_DEFAULT, null);
      this.prefix = prefix;
      return this;
    }

    /**
     * set the binding port type and this definition generate random default value if the value is
     * not defined.
     *
     * @return this builder
     */
    public Builder bindingPortWithRandomDefault() {
      return checkAndSet(Type.BINDING_PORT, Necessary.RANDOM_DEFAULT, null);
    }

    /**
     * set the type to boolean and add the default value.
     *
     * @param defaultValue the default boolean value
     * @return builder
     */
    public Builder optional(boolean defaultValue) {
      return checkAndSet(Type.BOOLEAN, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to short and add the default value.
     *
     * @param defaultValue the default short value
     * @return builder
     */
    public Builder optional(short defaultValue) {
      return checkAndSet(Type.SHORT, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to positive short and add the default value.
     *
     * @param defaultValue the default short value
     * @return builder
     */
    public Builder positiveNumber(short defaultValue) {
      return checkAndSet(Type.POSITIVE_SHORT, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to int and add the default value.
     *
     * @param defaultValue the default int value
     * @return builder
     */
    public Builder optional(int defaultValue) {
      return checkAndSet(Type.INT, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to positive int and add the default value.
     *
     * @param defaultValue the default int value
     * @return builder
     */
    public Builder positiveNumber(int defaultValue) {
      return checkAndSet(Type.POSITIVE_INT, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to long and add the default value.
     *
     * @param defaultValue the default long value
     * @return builder
     */
    public Builder optional(long defaultValue) {
      return checkAndSet(Type.LONG, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to positive long and add the default value.
     *
     * @param defaultValue the default long value
     * @return builder
     */
    public Builder positiveNumber(long defaultValue) {
      return checkAndSet(Type.POSITIVE_LONG, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to double and add the default value.
     *
     * @param defaultValue the default double value
     * @return builder
     */
    public Builder optional(double defaultValue) {
      return checkAndSet(Type.DOUBLE, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to positive double and add the default value.
     *
     * @param defaultValue the default double value
     * @return builder
     */
    public Builder positiveNumber(double defaultValue) {
      return checkAndSet(Type.POSITIVE_DOUBLE, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to Duration and add the default value.
     *
     * @param defaultValue the default Duration value
     * @return builder
     */
    public Builder optional(Duration defaultValue) {
      long value = defaultValue.toMillis();
      return checkAndSet(
          Type.DURATION,
          Necessary.OPTIONAL,
          // this format is more readable and it is compatible to scala.Duration
          value <= 1 ? value + " millisecond" : value + " milliseconds");
    }

    /**
     * set the type to string and add the default value.
     *
     * @param defaultValue the default string value
     * @return builder
     */
    public Builder optional(String defaultValue) {
      return checkAndSet(
          Type.STRING, Necessary.OPTIONAL, CommonUtils.requireNonEmpty(defaultValue));
    }

    /**
     * this is a composed operation. 1) set the type to string 2) set the default value to first
     * element 3) set the recommended values
     *
     * @param recommendedValues recommended string value
     * @return builder
     */
    public Builder optional(Set<String> recommendedValues) {
      this.recommendedValues = CommonUtils.requireNonEmpty(recommendedValues);
      return checkAndSet(
          Type.STRING,
          Necessary.OPTIONAL,
          CommonUtils.requireNonEmpty(recommendedValues.iterator().next()));
    }

    /**
     * set the type to CLASS
     *
     * @param defaultValue the default CLASS value
     * @return builder
     */
    public Builder optional(Class<?> defaultValue) {
      return checkAndSet(Type.CLASS, Necessary.OPTIONAL, defaultValue.getName());
    }

    /**
     * set the type to REMOTE_PORT
     *
     * @param defaultValue the default CLASS value
     * @return builder
     */
    public Builder optionalPort(int defaultValue) {
      return checkAndSet(Type.REMOTE_PORT, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to binding port and add default value.
     *
     * @param defaultValue the default CLASS value
     * @return builder
     */
    public Builder optionalBindingPort(int defaultValue) {
      return checkAndSet(Type.BINDING_PORT, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * set the type to remote port and add default value.
     *
     * @param defaultValue the default CLASS value
     * @return builder
     */
    public Builder optionalRemotePort(int defaultValue) {
      return checkAndSet(Type.REMOTE_PORT, Necessary.OPTIONAL, defaultValue);
    }

    /**
     * this method includes following settings. 1. set the type to ARRAY 2. disable specific values
     * 3. set the value to be required
     *
     * @param denyList the values in this list is illegal
     * @return builder
     */
    public Builder denyList(Set<String> denyList) {
      this.denyList = Objects.requireNonNull(denyList);
      return checkAndSet(
          Type.ARRAY, this.necessary == null ? Necessary.REQUIRED : this.necessary, null);
    }

    @Optional("this is no documentation for this setting by default")
    public Builder documentation(String documentation) {
      this.documentation = CommonUtils.requireNonEmpty(documentation);
      return this;
    }

    /**
     * This property is required by ohara manager. There are some official setting having particular
     * control on UI.
     *
     * @param reference the reference type
     * @return this builder
     */
    @Optional("Using in Ohara Manager. Default is None")
    public Builder reference(Reference reference) {
      this.reference = Objects.requireNonNull(reference);
      return this;
    }

    @Optional("default is common")
    public Builder group(String group) {
      this.group = CommonUtils.requireNonEmpty(group);
      return this;
    }

    @Optional("default is -1")
    public Builder orderInGroup(int orderInGroup) {
      this.orderInGroup = orderInGroup;
      return this;
    }

    @Optional("default value is equal to key")
    public Builder displayName(String displayName) {
      this.displayName = CommonUtils.requireNonEmpty(displayName);
      return this;
    }

    /**
     * set the regex to limit the value. Noted, it call toString from input value and apply this
     * regex.
     *
     * @param regex regex
     * @return this builder
     */
    @Optional("default value is null")
    public Builder regex(String regex) {
      this.regex = CommonUtils.requireNonEmpty(regex);
      return this;
    }

    @Override
    public SettingDef build() {
      return new SettingDef(
          displayName,
          group,
          orderInGroup,
          key,
          valueType == null ? Type.STRING : valueType,
          necessary == null ? Necessary.REQUIRED : necessary,
          defaultValue,
          documentation,
          reference,
          regex,
          internal,
          permission,
          tableKeys,
          recommendedValues,
          denyList,
          prefix);
    }
  }
}
