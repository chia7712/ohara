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

package oharastream.ohara.metrics;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import oharastream.ohara.common.annotations.VisibleForTesting;
import oharastream.ohara.common.util.CommonUtils;
import oharastream.ohara.metrics.basic.CounterMBean;
import oharastream.ohara.metrics.kafka.TopicMeter;

/**
 * This channel is a SNAPSHOT of all bean objects from local/remote bean server. Since Java APIs
 * don't allow us to get all objects in single connection, the implementation has to send multi
 * requests to fetch all bean objects. If you really case the performance, you should set both
 * {@link BeanChannel.Builder#domainName} and {@link BeanChannel.Builder#properties(Map)} to filter
 * unwanted objects. If you don't set both arguments, the filter will happen after the connection to
 * local/remote bean server. It won't save your life.
 */
@FunctionalInterface
public interface BeanChannel extends Iterable<BeanObject> {

  /**
   * remove unused beans from local jvm. This is not a public method since it should be called by
   * ohara's beans only.
   *
   * @param domain domain
   * @param properties properties
   */
  static void unregister(String domain, Map<String, String> properties) {
    try {
      ManagementFactory.getPlatformMBeanServer()
          .unregisterMBean(ObjectName.getInstance(domain, new Hashtable<>(properties)));
    } catch (InstanceNotFoundException
        | MBeanRegistrationException
        | MalformedObjectNameException e) {
      throw new IllegalArgumentException(e);
    }
  }
  /**
   * a helper method creating a channel to access local mbean server
   *
   * @return a bean channel connecting to local bean server
   */
  static BeanChannel local() {
    return builder().local().build();
  }

  /** @return a immutable list of bean objects */
  List<BeanObject> beanObjects();

  /** @return get only counter type from bean objects */
  default List<CounterMBean> counterMBeans() {
    return stream()
        .filter(CounterMBean::is)
        .map(CounterMBean::of)
        .collect(Collectors.toUnmodifiableList());
  }

  /** @return get only TopicMeter type from bean objects */
  default List<TopicMeter> topicMeters() {
    return stream()
        .filter(TopicMeter::is)
        .map(TopicMeter::of)
        .collect(Collectors.toUnmodifiableList());
  }

  default Stream<BeanObject> stream() {
    return beanObjects().stream();
  }

  /** @return the number of beans in this channel */
  default int size() {
    return beanObjects().size();
  }

  /** @return true if there is no beans in this channel. */
  default boolean empty() {
    return beanObjects().isEmpty();
  }

  /** @return false if there is no beans in this channel. */
  default boolean nonEmpty() {
    return !empty();
  }

  @Override
  default Iterator<BeanObject> iterator() {
    return beanObjects().iterator();
  }

  static Builder builder() {
    return new Builder();
  }

  static <T> Register<T> register() {
    return new Register<T>();
  }

  class Builder implements oharastream.ohara.common.pattern.Builder<BeanChannel> {
    private String domainName;
    private Map<String, String> properties = Map.of();
    private String hostname = null;
    private int port = -1;
    @VisibleForTesting boolean local = true;

    private Builder() {}

    public Builder hostname(String hostname) {
      this.hostname = CommonUtils.requireNonEmpty(hostname);
      this.local = false;
      return this;
    }

    public Builder port(int port) {
      this.port = CommonUtils.requireConnectionPort(port);
      this.local = false;
      return this;
    }

    @oharastream.ohara.common.annotations.Optional("default value is true")
    public Builder local() {
      this.local = true;
      return this;
    }

    /**
     * list the bean objects having the same domain. Setting a specific domain can reduce the
     * communication to jmx server
     *
     * @param domainName domain
     * @return this builder
     */
    public Builder domainName(String domainName) {
      this.domainName = CommonUtils.requireNonEmpty(domainName);
      return this;
    }

    /**
     * list the bean objects having the same property. Setting a specific property can reduce the
     * communication to jmx server
     *
     * @param key property key
     * @param value property value
     * @return this builder
     */
    public Builder property(String key, String value) {
      return properties(Map.of(key, value));
    }

    /**
     * list the bean objects having the same properties. Setting a specific properties can reduce
     * the communication to jmx server
     *
     * @param properties properties
     * @return this builder
     */
    public Builder properties(Map<String, String> properties) {
      CommonUtils.requireNonEmpty(properties)
          .forEach(
              (k, v) -> {
                CommonUtils.requireNonEmpty(k);
                CommonUtils.requireNonEmpty(v);
              });
      this.properties = CommonUtils.requireNonEmpty(properties);
      return this;
    }

    private BeanObject to(
        ObjectName objectName,
        MBeanInfo beanInfo,
        Function<String, Object> valueGetter,
        long queryTime) {
      // used to filter the "illegal" attribute
      Object unknown = new Object();
      Map<String, Object> attributes =
          Stream.of(beanInfo.getAttributes())
              .collect(
                  Collectors.toUnmodifiableMap(
                      MBeanAttributeInfo::getName,
                      attribute -> {
                        try {
                          return valueGetter.apply(attribute.getName());
                        } catch (Throwable e) {
                          // It is not allow to access the value.
                          return unknown;
                        }
                      }))
              .entrySet().stream()
              .filter(e -> e.getValue() != unknown)
              .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
      return BeanObject.builder()
          .domainName(objectName.getDomain())
          .properties(objectName.getKeyPropertyList())
          .attributes(attributes)
          // For all metrics, we will have a time of querying object
          .queryTime(queryTime)
          .build();
    }

    private ObjectName objectName() {
      if (domainName == null || properties.isEmpty()) return null;
      else {
        try {
          return ObjectName.getInstance(domainName, new Hashtable<>(properties));
        } catch (MalformedObjectNameException e) {
          throw new IllegalArgumentException(e);
        }
      }
    }

    private List<BeanObject> doBuild() {
      if (local) {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        // for each query, we should have same "queryTime" for each metric
        final long queryTime = CommonUtils.current();
        return server.queryMBeans(objectName(), null).stream()
            .map(
                objectInstance -> {
                  try {
                    return Optional.of(
                        to(
                            objectInstance.getObjectName(),
                            server.getMBeanInfo(objectInstance.getObjectName()),
                            (attribute) -> {
                              try {
                                return server.getAttribute(
                                    objectInstance.getObjectName(), attribute);
                              } catch (MBeanException
                                  | AttributeNotFoundException
                                  | InstanceNotFoundException
                                  | ReflectionException e) {
                                throw new IllegalArgumentException(e);
                              }
                            },
                            queryTime));
                  } catch (Throwable e) {
                    return Optional.empty();
                  }
                })
            .filter(Optional::isPresent)
            .map(o -> (BeanObject) o.get())
            .collect(Collectors.toUnmodifiableList());
      } else {
        try (JMXConnector connector =
            JMXConnectorFactory.connect(
                new JMXServiceURL(
                    "service:jmx:rmi:///jndi/rmi://"
                        + CommonUtils.requireNonEmpty(hostname)
                        + ":"
                        + CommonUtils.requireConnectionPort(port)
                        + "/jmxrmi"),
                null)) {
          MBeanServerConnection connection = connector.getMBeanServerConnection();
          // for each query, we should have same "queryTime" for each metric
          final long queryTime = CommonUtils.current();
          return connection.queryMBeans(objectName(), null).stream()
              .map(
                  objectInstance -> {
                    try {
                      return Optional.of(
                          to(
                              objectInstance.getObjectName(),
                              connection.getMBeanInfo(objectInstance.getObjectName()),
                              (attribute) -> {
                                try {
                                  return connection.getAttribute(
                                      objectInstance.getObjectName(), attribute);
                                } catch (MBeanException
                                    | AttributeNotFoundException
                                    | InstanceNotFoundException
                                    | ReflectionException
                                    | IOException e) {
                                  throw new IllegalArgumentException(e);
                                }
                              },
                              queryTime));
                    } catch (Throwable e) {
                      return Optional.empty();
                    }
                  })
              .filter(Optional::isPresent)
              .map(o -> (BeanObject) o.get())
              .collect(Collectors.toUnmodifiableList());
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      }
    }

    @Override
    public BeanChannel build() {
      List<BeanObject> objs =
          doBuild().stream()
              .filter(o -> CommonUtils.isEmpty(domainName) || o.domainName().equals(domainName))
              .filter(
                  o ->
                      CommonUtils.isEmpty(properties)
                          || new HashMap<>(o.properties()).equals(new HashMap<>(properties)))
              .collect(Collectors.toUnmodifiableList());
      return () -> objs;
    }
  }

  class Register<T> {
    private String domain = null;
    private Map<String, String> properties = Map.of();
    private T beanObject = null;

    private Register() {}

    public Register<T> domain(String domain) {
      this.domain = CommonUtils.requireNonEmpty(domain);
      return this;
    }

    public Register<T> properties(Map<String, String> properties) {
      this.properties = CommonUtils.requireNonEmpty(properties);
      return this;
    }

    public Register<T> beanObject(T beanObject) {
      this.beanObject = Objects.requireNonNull(beanObject);
      return this;
    }

    private void check() {
      CommonUtils.requireNonEmpty(domain);
      CommonUtils.requireNonEmpty(properties);
      Objects.requireNonNull(beanObject);
    }

    public T run() {
      check();
      try {
        ObjectName name = ObjectName.getInstance(domain, new Hashtable<>(properties));
        ManagementFactory.getPlatformMBeanServer().registerMBean(beanObject, name);
        return beanObject;
      } catch (MalformedObjectNameException
          | InstanceAlreadyExistsException
          | MBeanRegistrationException
          | NotCompliantMBeanException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
